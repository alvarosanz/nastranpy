import jwt
import getpass
import os
import sys
import time
import shutil
from pathlib import Path
import json
import binascii
import logging
import traceback
import socketserver
from contextlib import contextmanager
import threading
from multiprocessing import Process, cpu_count, Event, Manager, Lock
from nastranpy.results.database import Database
from nastranpy.results.results import process_query
from nastranpy.results.sessions import Sessions
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.results.connection import Connection, get_master_key, get_private_key, get_ip, find_free_port
from nastranpy.setup_logging import LoggerWriter
from nastranpy.bdf.misc import humansize


SERVER_PORT = 8080


class CentralQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        query = connection.recv()
        self.server.check_session(query)

        # WORKER REQUESTS
        if query['request_type'] == 'add_worker':
            self.server.add_worker(tuple(query['worker_address']), query['databases'], query['backup'])
        elif query['request_type'] == 'remove_worker':
            self.server.remove_worker(tuple(query['worker_address']))
        elif query['request_type'] == 'acquire_worker':
            connection.send(msg={'worker_address': self.server.acquire_worker(node=query['node'])})
        elif query['request_type'] == 'release_worker':

            if 'databases' in query:
                self.server.nodes[query['worker_address'][0]].databases = query['databases']

                if query['worker_address'][0] == self.server.server_address[0]:
                    self.server.databases = query['databases']

            self.server.release_worker(tuple(query['worker_address']))
        elif query['request_type'] == 'list_databases':
            connection.send(msg=self.server.databases)

        # CLIENT REQUESTS
        elif query['request_type'] == 'authentication':
            connection.send(msg={'msg': 'Login succesfully!'})
        elif query['request_type'] == 'shutdown':

            if query['node']:
                self.server.shutdown_node(query['node'])
                connection.send(msg={'msg': 'Node shutdown succesfully!'})
            else:
                threading.Thread(target=self.server.shutdown).start()
                connection.send(msg={'msg': 'Cluster shutdown succesfully!'})

        elif query['request_type'] == 'cluster_info':
            connection.send(msg={'msg': self.server.info(print_to_screen=False)})
        elif query['request_type'] == 'add_session':
            self.server.sessions.add_session(query['user'], session_hash=query['session_hash'],
                                             is_admin=query['is_admin'],
                                             create_allowed=query['create_allowed'],
                                             databases=query['databases'])
            connection.send(msg={'msg': "User '{}' added succesfully!".format(query['user'])})
        elif query['request_type'] == 'remove_session':
            self.server.sessions.remove_session(query['user'])
            connection.send(msg={'msg': "User '{}' removed succesfully!".format(query['user'])})
        elif query['request_type'] == 'list_sessions':
            connection.send(msg={'sessions': list(self.server.sessions.sessions.values())})
        elif query['request_type'] == 'sync_databases':
            self.server.sync_databases(query['nodes'], query['databases'], connection)
        else:

            if query['request_type'] != 'create_database' and query['path'] not in self.server.databases:
                raise ValueError("Database '{}' not available!".format(query['path']))

            if  query['request_type'] in ('create_database', 'append_to_database',
                                          'restore_database', 'remove_database'):
                node = self.server.server_address[0]
            else:
                node = None

            connection.send(msg={'redirection_address': self.server.acquire_worker(node=node, database=query['path'])})

class WorkerQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        query = connection.recv()
        self.server.check_session(query)

        if query['request_type'] == 'shutdown':
            self.server._shutdown_request = True
            threading.Thread(target=self.server.shutdown).start()
        elif query['request_type'] == 'list_databases':
            connection.send(msg=self.server.databases._getvalue())
        elif query['request_type'] == 'sync_databases':
            self.server.sync_databases(query['nodes'], query['databases'], connection)
        elif query['request_type'] == 'recv_databases':
            self.server.recv_databases(connection)
        elif query['request_type'] == 'remove_database':

            with self.server.database_lock.acquire(query['path']):
                shutil.rmtree(os.path.join(self.server.root_path, query['path']))

            connection.send(msg={'msg': "Database '{}' removed succesfully!".format(query['path'])})
            del self.server.databases[query['path']]
        else:

            with self.server.database_lock.acquire(query['path'],
                                                   block=(query['request_type'] in ('create_database',
                                                                                    'append_to_database',
                                                                                    'restore_database'))):
                path = os.path.join(self.server.root_path, query['path'])
                msg = ''
                df = None

                if query['request_type'] == 'create_database':

                    if query['path'] in self.server.databases.keys():
                        raise FileExistsError(f"Database already exists at '{query['path']}'!")

                    connection.send(msg=get_tables_specs())
                    db = Database()
                    db.create(query['files'], path, query['name'], query['version'],
                              database_project=query['project'],
                              table_generator=connection.recv_tables())
                    msg = 'Database created succesfully!'
                else:
                    db = Database(path)

                if query['request_type'] == 'check':
                    msg = db.check(print_to_screen=False)
                elif query['request_type'] == 'query':
                    df = db.query(**process_query(query))
                elif query['request_type'] == 'append_to_database':
                    connection.send(msg=db._get_tables_specs())
                    db.append(query['files'], query['batch'], table_generator=connection.recv_tables())
                    msg = 'Database created succesfully!'
                elif query['request_type'] == 'restore_database':
                    db.restore(query['batch'])
                    msg = f"Database restored to '{query['batch']}' state succesfully!"

                header = db.header.__dict__
                db = None

                if self.server.current_session['database_modified']:
                    self.server.databases[query['path']] = get_database_hash(os.path.join(path, '##header.json'))

            connection.send(msg={'msg': msg, 'header': header})

            if not df is None:
                connection.send_dataframe(df)


class DatabaseServer(socketserver.TCPServer):
    allow_reuse_address = True
    request_queue_size = 5

    def __init__(self, server_address, query_handler, root_path, debug=False):
        super().__init__(server_address, query_handler)
        self.root_path = root_path
        self.databases = None
        self.set_keys()
        self.current_session = None
        self._debug = debug
        self._set_log()
        self._done = Event()

    def _set_log(self):
        self.log = logging.getLogger('DatabaseServer')
        self.log.setLevel(logging.DEBUG)

        if not self.log.handlers:
            fh = logging.FileHandler(os.path.join(self.root_path, 'server.log'))

            if self._debug:
                fh.setLevel(logging.DEBUG)
            else:
                fh.setLevel(logging.INFO)

            fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.log.addHandler(fh)

    def set_keys(self):
        self.master_key = None
        self.private_key = get_private_key()

    def wait(self):
        self._done.wait()
        self._done.clear()

    def handle_error(self, request, client_address):
        tb = traceback.format_exc()
        self.log.error(tb)
        Connection(connection_socket=request).send(exception=tb)

    def refresh_databases(self):
        self.databases = get_local_databases(self.root_path)

    def verify_request(self, request, client_address):

        try:
            connection = Connection(connection_socket=request, private_key=self.private_key)
            data = json.loads(connection.recv_secret().decode())

            if 'master_key' in data:

                if self.master_key != data['master_key'].encode():
                    raise PermissionError()

                self.current_session = {'is_admin': True}

            elif 'password' in data:
                self.current_session = self.sessions.get_session(data['user'], data['password'])

                if data['request'] == 'master_key':

                    if not self.current_session['is_admin']:
                        raise PermissionError()

                    connection.send_secret(self.master_key)
                else:
                    authentication = jwt.encode(self.current_session, self.master_key)
                    connection.send_secret(authentication)

            elif 'authentication' in data:
                self.current_session = jwt.decode(data['authentication'].encode(), self.master_key)
            else:
                raise PermissionError()

            connection.send(b'Access granted!')
            return True

        except Exception:
            connection.send(exception='Access denied!')
            return False

    def check_session(self, query):
        self.current_session['request_type'] = query['request_type']

        if not self.current_session['is_admin']:

            if (query['request_type'] in ('shutdown', 'add_worker', 'remove_worker',
                                          'release_worker', 'acquire_worker',
                                          'sync_databases', 'recv_databases',
                                          'add_session', 'remove_session', 'list_sessions') or
                query['request_type'] == 'create_database' and not self.current_session['create_allowed'] or
                query['request_type'] in ('append_to_database', 'restore_database', 'remove_database') and
                (not self.current_session['databases'] or query('path') not in self.current_session['databases'])):
                raise PermissionError('Not enough privileges!')

        if query['request_type'] in ('recv_databases', 'append_to_database', 'restore_database',
                                     'create_database', 'remove_database'):
            self.current_session['database_modified'] = True
        else:
            self.current_session['database_modified'] = False


class CentralServer(DatabaseServer):

    def __init__(self, root_path, debug=False):
        super().__init__((get_ip(), SERVER_PORT), CentralQueryHandler, root_path, debug)
        self.refresh_databases()
        self.sessions = None
        self.master_key = get_master_key()
        self.nodes = dict()

    def start(self, sessions_file=None):
        password = getpass.getpass('password: ')

        if sessions_file:
            self.sessions = Sessions(sessions_file)
        else:
            sessions_file = os.path.join(self.root_path, 'sessions.json')

            if os.path.exists(sessions_file):
                self.sessions = Sessions(sessions_file)
            else:
                self.sessions = Sessions(sessions_file, password)

        try:
            self.sessions.get_session('admin', password)
        except Exception:
            raise PermissionError('Wrong password!')

        manager = Manager()
        start_workers(self.server_address, self.root_path, manager, 'admin', password,
                      n_workers=cpu_count() - 1, debug=self._debug)
        print(f"Address: {self.server_address}")
        print(f"Databases: {len(self.databases)}")
        self.serve_forever()
        print('Cluster shutdown succesfully!')

    def shutdown(self):

        for node in list(self.nodes):
            self.shutdown_node(node)

        self.wait()
        super().shutdown()

    def shutdown_node(self, node):

        for worker in list(self.nodes[node].workers):
            send(worker, {'request_type': 'shutdown'},
                 master_key=self.master_key, private_key=self.private_key)

    def info(self, print_to_screen=True):
        info = list()
        info.append(f"User: {self.current_session['user']}")

        if self.current_session['is_admin']:
            info.append(f"Administrator privileges")
        elif self.current_session['create_allowed']:
            info.append(f"Regular privileges; database creation allowed")
        else:
            info.append(f"Regular privileges")

        info.append(f"Address: {self.server_address}")
        info.append(f"\n{len(self.nodes)} nodes ({sum(len(node.workers) for node in self.nodes.values())} workers):")

        for node_address, node in self.nodes.items():
            info.append(f"  '{node_address}': {len(node.workers)} workers ({node.get_queue()} job/s in progress)")

            if node.backup:
                info[-1] += ' (backup mode)'

        info.append(f"\n{len(self.databases)} databases:")

        for database in self.databases:

            if not self.current_session['is_admin'] and (not self.current_session['databases'] or
                                                         database not in self.current_session['databases']):
                info.append(f"  '{database}' [read-only]")
            else:
                info.append(f"  '{database}'")

        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info

    def add_worker(self, worker, databases, backup):

        if worker[0] not in self.nodes:
            self.nodes[worker[0]] = Node([worker], databases, backup)
        else:
            self.nodes[worker[0]].workers[worker] = 0

    def remove_worker(self, worker):
        del self.nodes[worker[0]].workers[worker]

        if not self.nodes[worker[0]].workers:
            del self.nodes[worker[0]]

        if len(self.nodes) == 0:
            self._done.set()

    def acquire_worker(self, node=None, database=None):

        if not node:

            for node in sorted(self.nodes, key=lambda x: self.nodes[x].get_queue()):

                if (database in self.nodes[node].databases and
                    (not database in self.databases or
                     self.nodes[node].databases[database] == self.databases[database])):
                    break

        worker = self.nodes[node].get_worker()
        self.nodes[worker[0]].workers[worker] += 1
        return worker

    def release_worker(self, worker):
        self.nodes[worker[0]].workers[worker] -= 1

    def sync_databases(self, nodes, databases, connection):

        if not nodes:
            nodes = list(self.nodes)

        try:
            nodes.remove(self.server_address[0])
        except ValueError:
            pass

        nodes = {node: self.nodes[node].backup for node in nodes}

        if not nodes:
            raise ValueError('At least 2 nodes are required in order to sync them!')

        connection.send(msg={'request_type': 'sync_databases',
                             'nodes': nodes, 'databases': databases,
                             'redirection_address': self.acquire_worker(node=self.server_address[0])})

    def shutdown_request(self, request):
        super().shutdown_request(request)
        self.current_session = None


class Node(object):

    def __init__(self, workers, databases, backup):
        self.workers = {worker: 0 for worker in workers}
        self.databases = databases
        self.backup = backup

    def get_worker(self):
        return sorted(self.workers, key= lambda x: self.workers[x])[0]

    def get_queue(self):
        return sum(queue for queue in self.workers.values())


class DatabaseLock(object):

    def __init__(self, main_lock, locks, locked_databases):
        self.main_lock = main_lock
        self.locks = locks
        self.locked_databases = locked_databases

    @contextmanager
    def acquire(self, database, block=True):

        with self.main_lock:
            locked_databases = self.locked_databases._getvalue()

            try:
                lock_index, queue, n_jobs = locked_databases[database]
            except KeyError:
                used_locks = {i for i, _ in locked_databases.values()}
                lock_index = [i for i in range(len(self.locks)) if i not in used_locks].pop()
                queue = 0
                n_jobs = 0

            self.locked_databases[database] = (lock_index, queue + 1, n_jobs)
            lock = self.locks[lock_index]

        lock.acquire()

        with self.main_lock:
            lock_index, queue, n_jobs = self.locked_databases[database]
            self.locked_databases[database] = (lock_index, queue, n_jobs + 1)

        if block:

            while True:

                with self.main_lock:

                    if self.locked_databases[database][2] == 1:
                        break

                time.sleep(1)
        else:
            lock.release()

        yield

        # Release database
        with self.main_lock:
            lock_index, queue, n_jobs = self.locked_databases[database]

            if queue > 1:
                self.locked_databases[database] = (lock_index, queue - 1, n_jobs - 1)
            else:
                del self.locked_databases[database]

            if block:
                lock.release()


class WorkerServer(DatabaseServer):

    def __init__(self, server_address, central_address, root_path,
                 databases, main_lock, database_lock, backup=False, debug=False):
        super().__init__(server_address, WorkerQueryHandler, root_path, debug)
        self.central = central_address
        self.databases = databases
        self.main_lock = main_lock
        self.database_lock = database_lock
        self.backup = backup
        self._shutdown_request = False
        sys.stdout = LoggerWriter(self.log.info)
        sys.stderr = LoggerWriter(self.log.error)

    def start(self, user, password):

        try:
            connection = Connection(self.central, private_key=self.private_key)
            connection.send_secret(json.dumps({'user': user,
                                               'password': password,
                                               'request': 'master_key'}).encode())
            self.master_key = connection.recv_secret()
            connection.send(msg={'request_type': 'add_worker',
                                 'worker_address': self.server_address,
                                 'databases': self.databases._getvalue(),
                                 'backup': self.backup})
        finally:
            connection.kill()

        self.serve_forever()

    def shutdown(self):
        send(self.central, {'request_type': 'remove_worker',
                            'worker_address': self.server_address},
             master_key=self.master_key, private_key=self.private_key)
        super().shutdown()

    def shutdown_request(self, request):
        super().shutdown_request(request)

        if not self._shutdown_request:
            data = {'request_type': 'release_worker',
                    'worker_address': self.server_address}

            if self.current_session['database_modified']:
                data['databases'] = self.databases._getvalue()

            send(self.central, data, master_key=self.master_key, private_key=self.private_key)

        self.current_session = None

    def sync_databases(self, nodes, databases, client_connection):
        self.refresh_databases()

        if databases:
            update_only = False
            databases = {database: self.databases[database] for database in databases if
                         database in self.databases}
        else:
            update_only = True
            databases = self.databases

        for node, backup in nodes.items():
            worker = tuple(request(self.central, {'request_type': 'acquire_worker', 'node': node},
                                   master_key=self.master_key, private_key=self.private_key)[1]['worker_address'])
            client_connection.send(msg={'msg': f"Syncing node '{node}' ..."})

            try:
                connection = Connection(worker, private_key=self.private_key)
                connection.send_secret(json.dumps({'master_key': self.master_key.decode()}).encode())
                connection.recv()
                connection.send(msg={'request_type': 'recv_databases'})
                remote_databases = connection.recv()

                for database in databases:

                    if (not update_only and (database not in remote_databases or
                                             databases[database] != remote_databases[database]) or
                        update_only and (not backup and database in remote_databases and databases[database] != remote_databases[database] or
                                         backup and (database not in remote_databases or databases[database] != remote_databases[database]))):

                        with self.database_lock.acquire(database, block=False):
                            database_path = Path(self.root_path) / database
                            files = [file for pattern in ('**/*header.*', '**/*.bin') for file in database_path.glob(pattern)]
                            connection.send(msg={'database': database, 'msg': '',
                                                 'files': [str(file.relative_to(database_path)) for file in files]})
                            client_connection.send(msg={'msg': f"  Syncing database '{database}' ({len(files)} files; {humansize(sum(os.path.getsize(file) for file in files))})..."})

                            for file in files:
                                connection.send_file(file)
                                msg = connection.recv()

                connection.send(msg={'msg': "Done!"})
            finally:
                connection.kill()

        client_connection.send(msg={'msg': f"Done!"})

    def recv_databases(self, connection):
        self.refresh_databases()
        connection.send(msg=self.databases._getvalue())
        data = connection.recv()

        while data['msg'] != 'Done!':

            with self.database_lock.acquire(data['database']):
                path = Path(self.root_path) / data['database']
                path_temp = path.parent / (path.name + '_TEMP')
                path_temp.mkdir()

                try:

                    for file in data['files']:
                        file = path_temp / file
                        file.parent.mkdir(exist_ok=True)
                        connection.recv_file(file)
                        connection.send(b'OK')

                    if os.path.exists(path):
                        shutil.rmtree(path)

                    path_temp.rename(path)
                except Exception as e:
                    shutil.rmtree(path_temp)
                    raise e

            data = connection.recv()

    def refresh_databases(self):

        with self.main_lock:
            self.databases.clear()
            self.databases.update(get_local_databases(self.root_path))
            return self.databases._getvalue()


def start_worker(server_address, central_address, root_path,
                 databases, main_lock, locks, locked_databases, user, password, backup, debug):
    database_lock = DatabaseLock(main_lock, locks, locked_databases)
    worker = WorkerServer(server_address, central_address, root_path,
                          databases, main_lock, database_lock, backup, debug)
    worker.start(user, password)


def start_workers(central_address, root_path, manager, user, password,
                  n_workers=None, backup=False, debug=False):

    if not n_workers:
        n_workers = cpu_count()

    databases = manager.dict(get_local_databases(root_path))
    main_lock = Lock()
    locks = [Lock() for lock in range(n_workers)]
    locked_databases = manager.dict()
    host = get_ip()
    workers = list()

    for i in range(n_workers):
        workers.append(Process(target=start_worker, args=((host, find_free_port()), central_address, root_path,
                                                          databases, main_lock, locks, locked_databases,
                                                          user, password, backup, debug)))
        workers[-1].start()

    return workers


def start_node(central_address, root_path, backup=False, debug=False):
    user = input('user: ')
    password = getpass.getpass('password: ')
    manager = Manager()
    workers = start_workers(central_address, root_path, manager, user, password,
                            backup=backup, debug=debug)

    for worker in workers:
        worker.join()

    print('Node shutdown succesfully!')


def get_local_databases(root_path):
    databases = dict()

    for header_file in Path(root_path).glob('**/##header.json'):
        database = str(header_file.parent.relative_to(root_path))
        databases[database] = get_database_hash(header_file)

    return databases


def get_database_hash(header_file):

    with open(header_file) as f:
        header = json.load(f)

    with open(str(header_file)[:-4] + header['checksum'], 'rb') as f:
        return binascii.hexlify(f.read()).decode()


def send(address, msg, master_key=None, private_key=None, recv=False):

    try:
        connection = Connection(address, private_key=private_key)

        if connection.private_key:
            connection.send_secret(json.dumps({'master_key': master_key.decode()}).encode())
            connection.recv()

        connection.send(msg=msg)

        if recv:
            return connection.recv()

    finally:
        connection.kill()


def request(address, msg, master_key=None, private_key=None):
    return send(address, msg, master_key, private_key, recv=True)

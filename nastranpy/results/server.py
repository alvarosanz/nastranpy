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
        query = connection.recv()[1]
        self.server.check_session(query)

        if query['request_type'] == 'authentication':
            connection.send('Login succesfully!')
        elif query['request_type'] == 'shutdown':

            if query['node']:
                self.server.shutdown_node(query['node'])
                connection.send('Node shutdown succesfully!')
            else:
                threading.Thread(target=self.server.shutdown).start()
                connection.send('Cluster shutdown succesfully!')

        elif query['request_type'] == 'add_worker':
            self.server.add_worker(tuple(query['worker_address']), query['databases'], query['backup'])
        elif query['request_type'] == 'remove_worker':
            self.server.remove_worker(tuple(query['worker_address']))
        elif query['request_type'] == 'unlock_worker':

            if 'databases' in query:
                self.server.nodes[query['worker_address'][0]].databases = query['databases']

                if query['worker_address'][0] == self.server.server_address[0]:
                    self.server.databases = query['databases']

            self.server.unlock_worker(tuple(query['worker_address']))
        elif query['request_type'] == 'cluster_info':
            connection.send(self.server.info(print_to_screen=False))
        elif query['request_type'] == 'add_session':
            self.server.sessions.add_session(query['user'], session_hash=query['session_hash'],
                                             is_admin=query['is_admin'],
                                             create_allowed=query['create_allowed'],
                                             databases=query['databases'])
            connection.send("User '{}' added succesfully!".format(query['user']))
        elif query['request_type'] == 'remove_session':
            self.server.sessions.remove_session(query['user'])
            connection.send("User '{}' removed succesfully!".format(query['user']))
        elif query['request_type'] == 'list_sessions':
            connection.send(self.server.sessions.info(print_to_screen=False))
        elif query['request_type'] == 'list_databases':
            connection.send(data=self.server.databases)
        elif query['request_type'] == 'sync_databases':
            self.server.sync_databases(query['nodes'], query['databases'], connection)
        elif query['request_type'] == 'acquire_worker':
            worker = self.server.nodes[query['node']].get_worker()
            self.server.lock_worker(worker)
            connection.send(data={'worker_address': worker})
        else:

            if query['request_type'] != 'create_database' and query['path'] not in self.server.databases:
                raise ValueError("Database '{}' not available!".format(query['path']))

            worker = self.server.get_worker(query['request_type'], query['path'])
            self.server.lock_worker(worker)
            connection.send(data={'redirection_address': worker})

class WorkerQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        query = connection.recv()[1]
        self.server.check_session(query)

        if query['request_type'] == 'shutdown':
            self.server._shutdown_request = True
            threading.Thread(target=self.server.shutdown).start()
        elif query['request_type'] == 'list_databases':
            connection.send(data=self.server.databases._getvalue())
        elif query['request_type'] == 'sync_databases':
            self.server.sync_databases(query['nodes'], query['databases'], connection)
        elif query['request_type'] == 'recv_databases':
            self.server.recv_databases(connection)
        elif query['request_type'] == 'remove_database':
            self.server.acquire_database(query['path'])
            shutil.rmtree(os.path.join(self.server.root_path, query['path']))
            connection.send("Database '{}' removed succesfully!".format(query['path']))
        else:
            self.server.acquire_database(query['path'])
            msg = ''
            path = os.path.join(self.server.root_path, query['path'])

            if query['request_type'] == 'create_database':

                if query['path'] in self.server.databases.keys():
                    raise FileExistsError(f"Database already exists at '{query['path']}'!")

                connection.send('Creating database ...', data=get_tables_specs())
                db = Database()
                db.create(query['files'], path, query['name'], query['version'],
                          database_project=query['project'],
                          table_generator=connection.recv_tables())
                msg = 'Database created succesfully!'
            else:
                db = Database(path)

            df = None

            if query['request_type'] == 'check':
                msg = db.check(print_to_screen=False)
            elif query['request_type'] == 'query':
                df=db.query(**process_query(query))
            elif query['request_type'] == 'append_to_database':
                connection.send('Appending to database ...', data=db._get_tables_specs())
                db.append(query['files'], query['batch'], table_generator=connection.recv_tables())
                msg = 'Database created succesfully!'
            elif query['request_type'] == 'restore_database':
                db.restore(query['batch'])
                msg = f"Database restored to '{query['batch']}' state succesfully!"

            connection.send(msg, data=db._export_header(), df=df)


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
        Connection(connection_socket=request).send('#' + tb)

    def refresh_databases(self):
        self.databases = get_local_databases(self.root_path)

    def verify_request(self, request, client_address):

        try:
            connection = Connection(connection_socket=request, private_key=self.private_key)
            data = json.loads(connection.recv_secret())

            if 'master_key' in data:

                if self.master_key != data['master_key']:
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
                    connection.send_secret(authentication.decode())

            elif 'authentication' in data:
                self.current_session = jwt.decode(data['authentication'], self.master_key)
            else:
                raise PermissionError()

            connection.send('Access granted!')
            return True

        except Exception:
            connection.send('#Access denied!')
            return False

    def check_session(self, query):
        self.current_session['request_type'] = query['request_type']

        if not self.current_session['is_admin']:

            if (query['request_type'] in ('shutdown', 'add_worker', 'remove_worker',
                                          'unlock_worker', 'acquire_worker',
                                          'sync_databases', 'recv_databases',
                                          'add_session', 'remove_session', 'list_sessions') or
                query['request_type'] == 'create_database' and not self.current_session['create_allowed'] or
                query['request_type'] in ('append_to_database', 'restore_database', 'remove_database') and
                (not self.current_session['databases'] or query('path') not in self.current_session['databases'])):
                raise PermissionError('Not enough privileges!')


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
        info.append(f"Address: {self.server_address}")
        info.append(f"\n{len(self.nodes)} nodes ({sum(len(node.workers) for node in self.nodes.values())} workers):")

        for node_address, node in self.nodes.items():
            info.append(f"  '{node_address}': {len(node.workers)} workers ({node.get_queue()} job/s in progress)")

            if node.backup:
                info[-1] += ' (backup mode)'

        info.append(f"\n{len(self.databases)} databases:")

        for database in self.databases:
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

    def lock_worker(self, worker):
        self.nodes[worker[0]].workers[worker] += 1

    def unlock_worker(self, worker):
        self.nodes[worker[0]].workers[worker] -= 1

    def get_worker(self, request_type, database=None):

        if request_type in ('create_database', 'append_to_database', 'restore_database', 'remove_database'):
            return self.nodes[self.server_address[0]].get_worker()
        else:

            for node in sorted(self.nodes, key=lambda x: self.nodes[x].get_queue()):

                if (database in self.nodes[node].databases and
                    (not database in self.databases or
                     self.nodes[node].databases[database] == self.databases[database])):
                    return self.nodes[node].get_worker()

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

        worker = self.nodes[self.server_address[0]].get_worker()
        self.lock_worker(worker)
        connection.send(data={'request_type': 'sync_databases',
                              'nodes': nodes, 'databases': databases,
                              'redirection_address': worker})

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


class WorkerServer(DatabaseServer):

    def __init__(self, server_address, central_address, root_path,
                 databases, locked_databases, locks, backup=False, debug=False):
        super().__init__(server_address, WorkerQueryHandler, root_path, debug)
        self.central = central_address
        self.databases = databases
        self.locked_databases = locked_databases
        self.main_lock = locks[0]
        self.database_locks = locks[1:]
        self.database_lock = None
        self.backup = backup
        self._shutdown_request = False
        sys.stdout = LoggerWriter(self.log.info)
        sys.stderr = LoggerWriter(self.log.error)

    def start(self, user, password):

        try:
            connection = Connection(self.central, private_key=self.private_key)
            connection.send_secret(json.dumps({'user': user,
                                               'password': password,
                                               'request': 'master_key'}))
            self.master_key = connection.recv_secret()
            connection.send(data={'request_type': 'add_worker',
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

    def acquire_database(self, database, block=True):
        self.current_session['database'] = database

        with self.main_lock:

            try:
                lock_index, queue = self.locked_databases[database]
            except KeyError:

                for lock_index, lock in enumerate(self.database_locks):

                    if lock.acquire(False):
                        lock.release()
                        queue = 0
                        break

            queue += 1
            self.locked_databases[database] = (lock_index, queue)

        self.database_lock = self.database_locks[lock_index]
        self.database_lock.acquire()

        if self.current_session['request_type'] not in ('create_database', 'remove_database',
                                                        'append_to_database', 'restore_database',
                                                        'recv_databases'):
            self.database_lock.release()
        else:

            while queue > 1:
                time.sleep(1)

                with self.main_lock:
                    queue = self.locked_databases[database][1]

    def release_database(self):

        if self.database_lock:

            with self.main_lock:
                database = self.current_session['database']
                lock_index, queue = self.locked_databases[database]

                if queue > 1:
                    self.locked_databases[database] = (lock_index, queue - 1)
                else:
                    del self.locked_databases[database]

                try:
                    self.database_lock.release()
                except ValueError:
                    pass

                self.database_lock = None

    def shutdown_request(self, request):
        super().shutdown_request(request)

        if not self._shutdown_request:
            data = {'request_type': 'unlock_worker',
                    'worker_address': self.server_address}

            if self.current_session['request_type'] in ('recv_databases',
                                                        'append_to_database', 'restore_database',
                                                        'create_database', 'remove_database'):

                data['databases'] = self.refresh_databases()

            send(self.central, data, master_key=self.master_key, private_key=self.private_key)

        self.release_database()
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
            client_connection.send(f"Syncing node '{node}' ...")

            try:
                connection = Connection(worker, private_key=self.private_key)
                connection.send_secret(json.dumps({'master_key': self.master_key}))
                connection.recv()
                connection.send(data={'request_type': 'recv_databases'})
                remote_databases = connection.recv()[1]

                for database in databases:

                    if (not update_only and (database not in remote_databases or
                                             databases[database] != remote_databases[database]) or
                        update_only and (not backup and database in remote_databases and databases[database] != remote_databases[database] or
                                         backup and (database not in remote_databases or databases[database] != remote_databases[database]))):
                        self.acquire_database(database)
                        database_path = Path(self.root_path) / database
                        files = [file for pattern in ('**/*header.*', '**/*.bin') for file in database_path.glob(pattern)]
                        connection.send(data={'database': database,
                                              'files': [str(file.relative_to(database_path)) for file in files]})
                        client_connection.send(f"  Syncing database '{database}' ({len(files)} files; {humansize(sum(os.path.getsize(file) for file in files))})...")

                        for file in files:
                            connection.send_file(file)
                            msg = connection.recv()[0]

                        self.release_database()

                connection.send(f"Done!")
            finally:
                connection.kill()

        client_connection.send(f"Done!")

    def recv_databases(self, connection):
        self.refresh_databases()
        connection.send(data=self.databases._getvalue())
        msg = ''

        while msg != 'Done!':
            msg, data, _ = connection.recv()
            self.acquire_database(data['database'])
            path = Path(self.root_path) / data['database']
            path_temp = path.parent / (path.name + '_TEMP')
            path_temp.mkdir()

            try:

                for file in data['files']:
                    file = path_temp / file
                    file.parent.mkdir(exist_ok=True)
                    connection.recv_file(file)
                    connection.send(msg='OK')

                if os.path.exists(path):
                    shutil.rmtree(path)

                path_temp.rename(path)
            except Exception as e:
                shutil.rmtree(path_temp)
                raise e
            finally:
                self.release_database()

    def refresh_databases(self):

        with self.main_lock:
            self.databases.clear()
            self.databases.update(get_local_databases(self.root_path))
            return self.databases._getvalue()


def start_worker(server_address, central_address, root_path,
                 databases, locked_databases, locks, user, password, backup, debug):
    worker = WorkerServer(server_address, central_address, root_path,
                          databases, locked_databases, locks, backup, debug)
    worker.start(user, password)


def start_workers(central_address, root_path, manager, user, password,
                  n_workers=None, backup=False, debug=False):

    if not n_workers:
        n_workers = cpu_count()

    databases = manager.dict(get_local_databases(root_path))
    locked_databases = manager.dict()
    locks = [Lock() for lock in range(n_workers)]
    host = get_ip()
    workers = list()

    for i in range(n_workers):
        workers.append(Process(target=start_worker, args=((host, find_free_port()), central_address, root_path,
                                                          databases, locked_databases, locks,
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

        with open(header_file) as f:
            header = json.load(f)

        with open(str(header_file)[:-4] + header['checksum'], 'rb') as f:
            databases[database] = binascii.hexlify(f.read()).decode()

    return databases


def send(address, data, master_key=None, private_key=None, recv=False):

    try:
        connection = Connection(address, private_key=private_key)

        if connection.private_key:
            connection.send_secret(json.dumps({'master_key': master_key}))
            connection.recv()

        connection.send(data=data)

        if recv:
            return connection.recv()

    finally:
        connection.kill()


def request(address, data, master_key=None, private_key=None):
    return send(address, data, master_key, private_key, recv=True)

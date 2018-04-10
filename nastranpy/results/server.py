import os
import sys
import shutil
from pathlib import Path
import json
import binascii
import logging
import traceback
import socketserver
import threading
from multiprocessing import Process, cpu_count, Event
from nastranpy.results.database import Database
from nastranpy.results.results import process_query
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.results.connection import Connection, send, request, get_ip, find_free_port
from nastranpy.setup_logging import LoggerWriter


SERVER_PORT = 8080


class CentralQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        query = connection.recv()[1]
        self.server.log.debug(f"Processing query ('{query['request_type']}') from {self.client_address} in central server {self.server.server_address}")

        if query['request_type'] == 'shutdown':
            threading.Thread(target=self.server.shutdown).start()
        elif query['request_type'] == 'add_worker':
            self.server.add_worker(tuple(query['worker_address']), query['databases'])
        elif query['request_type'] == 'remove_worker':
            self.server.remove_worker(tuple(query['worker_address']))
        elif query['request_type'] == 'unlock_worker':
            self.server.unlock_worker(tuple(query['worker_address']))
        elif query['request_type'] == 'cluster_info':
            connection.send(self.server.info(print_to_screen=False))
        elif query['request_type'] == 'list_databases':
            self.server.refresh_databases()
            connection.send(data=self.server.databases)
        elif query['request_type'] == 'sync_databases':

            if not query['nodes']:
                query['nodes'] = list(self.server.nodes)

            try:
                query['nodes'].remove(self.server.server_address[0])
            except ValueError:
                pass

            worker = self.server.nodes[self.server.server_address[0]].get_worker()
            self.server.lock_worker(worker)
            connection(data={'request_type': 'sync_databases',
                             'nodes': nodes, 'databases': databases,
                             'redirection_address': worker})
        elif query['request_type'] == 'acquire_worker':
            worker = self.server.nodes[query['node']].get_worker()
            self.server.lock_worker(worker)
            connection.send(data={'worker_address': worker})
        else:
            worker = self.server.get_worker(query['request_type'], query['path'])
            self.server.lock_worker(worker)
            connection.send(data={'redirection_address': worker})

class WorkerQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        query = connection.recv()[1]
        self.server.log.debug(f"Processing query ('{query['request_type']}') from {self.client_address} in worker server {self.server.server_address}")

        if query['request_type'] == 'shutdown':
            self.server._shutdown_request = True
            threading.Thread(target=self.server.shutdown).start()
        elif query['request_type'] == 'list_databases':
            self.server.refresh_databases()
            connection.send(data=self.server.databases)
        elif query['request_type'] == 'sync_databases':
            self.server.sync_databases(query['nodes'], query['databases'], connection)
        elif query['request_type'] == 'recv_databases':
            self.server.recv_databases(connection)
        else:
            msg = ''
            path = os.path.join(self.server.root_path, query['path'])

            if query['request_type'] == 'create_database':

                if query['path'] in self.server.databases:
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
        self.refresh_databases()
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

    def wait(self):
        self._done.wait()
        self._done.clear()

    def handle_error(self, request, client_address):
        super().handle_error(request, client_address)
        tb = traceback.format_exc()
        self.log.error(tb)
        Connection(connection_socket=request).send('#' + tb)

    def refresh_databases(self):
        self.databases = get_local_databases(self.root_path)


class CentralServer(DatabaseServer):

    def __init__(self, root_path, debug=False):
        super().__init__((get_ip(), SERVER_PORT), CentralQueryHandler, root_path, debug)
        self.nodes = dict()

    def start(self):
        print(f"Address: {self.server_address}")
        print(f"Databases: {len(self.databases)}")
        start_workers(self.server_address, self.root_path, cpu_count() - 1, self._debug)
        self.serve_forever()

    def shutdown(self):

        for node in list(self.nodes.values()):

            for worker in list(node.workers):
                send(worker, {'request_type': 'shutdown'})

        self.wait()
        super().shutdown()

    def info(self, print_to_screen=True):
        info = list()
        info.append(f"Address: {self.server_address}")
        info.append(f"\n{len(self.nodes)} nodes ({sum(len(node.workers) for node in self.nodes.values())} workers):")

        for node_address, node in self.nodes.items():
            info.append(f"  '{node_address}': {len(node.workers)} workers ({node.get_queue()} job/s in progress)")

        info.append(f"\n{len(self.databases)} databases:")

        for database in self.databases:
            info.append(f"  '{database}'")

        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info

    def add_worker(self, worker, databases):

        if worker[0] not in self.nodes:
            self.nodes[worker[0]] = Node()

        self.nodes[worker[0]].workers[worker] = 0
        self.nodes[worker[0]].databases = databases

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

        if request_type in ('create_database', 'append_to_database', 'restore_database'):
            return self.nodes[self.server_address[0]].get_worker()
        else:

            for node in sorted(self.nodes, key=lambda x: self.nodes[x].get_queue()):

                if (database in self.nodes[node].databases and
                    (not database in self.databases or
                     self.nodes[node].databases[database] == self.databases[database])):
                    return self.nodes[node].get_worker()


class Node(object):

    def __init__(self):
        self.workers = dict()
        self.databases = dict()

    def get_worker(self):
        return sorted(self.workers, key= lambda x: self.workers[x])[0]

    def get_queue(self):
        return sum(queue for queue in self.workers.values())


class WorkerServer(DatabaseServer):

    def __init__(self, server_address, central_address, root_path, debug=False):
        super().__init__(server_address, WorkerQueryHandler, root_path, debug)
        self.central = central_address
        self._shutdown_request = False
        sys.stdout = LoggerWriter(self.log.info)
        sys.stderr = LoggerWriter(self.log.error)

    def start(self):
        send(self.central, {'request_type': 'add_worker',
                            'worker_address': self.server_address,
                            'databases': self.databases})
        self.serve_forever()

    def shutdown(self):
        send(self.central, {'request_type': 'remove_worker',
                            'worker_address': self.server_address})
        super().shutdown()

    def shutdown_request(self, request):
        super().shutdown_request(request)

        if not self._shutdown_request:
            send(self.central, {'request_type': 'unlock_worker',
                                'worker_address': self.server_address})

    def sync_databases(self, nodes, databases, client_connection):
        self.refresh_databases()

        if databases:
            update_only = False
            databases = {database: self.databases[database] for database in databases if
                         database in self.databases}
        else:
            update_only = True
            databases = self.databases

        for node in nodes:
            worker = request(self.central_address, {'request_type': 'acquire_worker',
                                                    'node': node})[1]['worker_address']

            try:
                connection = Connection(worker)
                connection.send(data={'request_type': 'recv_databases'})
                remote_databases = connection.recv()[1]

                for database in databases:
                    client_connection.send(msg=f"Syncing database '{database}' in node '{node}' ...")

                    if (not update_only or database in remote_databases and
                        databases[database] != remote_databases[database]):
                        database_path = Path(self.root_path) / database
                        files = [file for pattern in ('**/*header.*', '**/*.bin') for file in database_path.glob(pattern)]
                        connection.send(data={'database': database,
                                              'files': [str(file.relative_to(database_path)) for file in files]})

                        for file in files:
                            client_connection.send(msg=f"Sending '{file.relative_to(database_path)}' ...")
                            connection.send_file(file)
                            msg = connection.recv()[0]

            finally:
                connection.kill()

            client_connection.send(msg=f"Done!")

    def recv_databases(self, connection):
        self.refresh_databases()
        connection.send(data=self.databases)
        data = connection.recv()[0]
        path = Path(self.root_path) / data['database']
        path_temp = path.parent /path.name + '_TEMP'
        path_temp.mkdir()

        try:

            for file in data['files']:
                connection.recv_file(path_temp / file)
                connection.send(msg='OK')

            shutil.rmtree(path)
            path_temp.rename(path)

        except Exception:
            shutil.rmtree(path_temp)


def start_worker(server_address, central_address, root_path, debug):
    worker = WorkerServer(server_address, central_address, root_path, debug)
    worker.start()


def start_workers(central_address, root_path, n_workers=None, debug=False):
    workers = list()
    host = get_ip()

    for i in range(n_workers if workers else cpu_count()):
        worker = (host, find_free_port())
        Process(target=start_worker, args=(worker, central_address, root_path, debug)).start()
        workers.append(worker)

    return workers


def get_local_databases(root_path):
    databases = dict()

    for header_file in Path(root_path).glob('**/##header.json'):
        database = str(header_file.parent.relative_to(root_path))

        with open(header_file) as f:
            header = json.load(f)

        with open(str(header_file)[:-4] + header['checksum'], 'rb') as f:
            databases[database] = binascii.hexlify(f.read()).decode()

    return databases


class ClusterManager(DatabaseServer):
    pass

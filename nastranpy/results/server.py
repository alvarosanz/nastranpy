import os
import sys
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
WORKERS_PER_NODE = cpu_count() - 1


class CentralQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        query = connection.recv()[1]
        self.server.log.debug(f"Processing query ('{query['request_type']}') from {self.client_address} in {self.server.type} server {self.server.server_address}")

        if query['request_type'] == 'shutdown':
            threading.Thread(target=self.server.shutdown).start()
        elif query['request_type'] == 'add_worker':
            self.server._add_worker(tuple(query['node_address']),
                                    tuple(query['worker_address']), query['databases'])
        elif query['request_type'] == 'remove_worker':
            self.server._remove_worker(tuple(query['node_address']),
                                       tuple(query['worker_address']))
        elif query['request_type'] == 'unlock_worker':
            self.server._unlock_worker(tuple(query['node_address']),
                                       tuple(query['worker_address']))
        elif query['request_type'] == 'cluster_info':
            connection.send(self.server.info(print_to_screen=False))
        elif query['request_type'] == 'list_databases':
            self.server.refresh_databases()
            connection.send(data=self.server.databases)
        else:
            node, worker = self.server._get_worker(query['request_type'], query['path'])
            self.server._lock_worker(node, worker)
            connection.send(data={'redirection_address': worker})

class WorkerQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        query = connection.recv()[1]
        self.server.log.debug(f"Processing query ('{query['request_type']}') from {self.client_address} in {self.server.type} server {self.server.server_address}")

        if query['request_type'] == 'shutdown':
            threading.Thread(target=self.server.shutdown).start()
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
        self._done = threading.Event()
        self._is_shut_down = False
        self._debug = debug
        self._set_log()

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

    def handle_error(self, request, client_address):
        super().handle_error(request, client_address)
        tb = traceback.format_exc()
        self.log.error(tb)
        Connection(connection_socket=request).send('#' + tb)

    def _wait(self):
        self._done.wait()
        self._done.clear()

    def refresh_databases(self):
        self.databases = get_local_databases(self.root_path)

    def _send_database(self, address, database):
        database_path = Path(self.root_path) / database
        files = [file for pattern in ('**/*header.*', '**/*.bin') for file in database_path.glob(pattern)]

        try:
            connection = Connection(address)
            connection.send(data={'request_type': 'send_database',
                                  'database': database,
                                  'files': [str(file.relative_to(database_path)) for file in files]})
            msg = connection.recv()[0]

            for file in files:
                connection.send_file(file)

            msg = connection.recv()[0]
        finally:
            connection.kill()

    def _request_database(self, address, database):

        try:
            connection = Connection(address)
            connection.send(data={'request_type': 'request_database',
                                  'database': database})
            msg, data, _ = connection.recv()

            for file in data['files']:
                connection.recv_file(file)

            msg = connection.recv()[0]
        finally:
            connection.kill()


class CentralServer(DatabaseServer):

    def __init__(self, root_path, debug=False):
        super().__init__((get_ip(), SERVER_PORT), CentralQueryHandler, root_path, debug)
        self.nodes = dict()
        self.type = 'central'

    def start(self):
        threading.Thread(target=self.serve_forever).start()
        start_workers(self.server_address, self.server_address, self.root_path, self._debug)
        self._wait()
        print(f"Address: {self.server_address}")
        print(f"Nodes: {len(self.nodes)} ({self.n_workers} workers)")
        print(f"Databases: {len(self.databases)}")

    def shutdown(self):

        for node in list(self.nodes.values()):

            for worker in list(node.workers):
                send(worker, {'request_type': 'shutdown'})

        self._wait()
        super().shutdown()
        self._is_shut_down = True
        print("Cluster shut down succesfully!")

    @property
    def n_workers(self):
        return sum(len(node.workers) for node in self.nodes.values())

    def info(self, print_to_screen=True):

        if self._is_shut_down:
            print(f"Cluster is off!")
        else:
            info = list()
            info.append(f"Address: {self.server_address}")
            info.append(f"\n{len(self.nodes)} nodes ({self.n_workers} workers):")

            for node_address, node in self.nodes.items():
                info.append(f"  {node_address}: {len(node.workers)} workers ({node.queue} job/s in progress)")

            info.append(f"\n{len(self.databases)} databases:")

            for database in self.databases:
                info.append(f"  '{database}'")

            info = '\n'.join(info)

            if print_to_screen:
                print(info)
            else:
                return info

    def _add_worker(self, node, worker, databases):

        if node not in self.nodes:
            self.nodes[node] = Node(node)
            self.nodes[node].databases = databases

        self.nodes[node].workers[worker] = 0

        if node != self.server_address:
            send(node, data={'request_type': 'add_worker',
                             'node_address': node,
                             'worker_address': worker,
                             'databases': None})

        if len(self.nodes[self.server_address].workers) == WORKERS_PER_NODE:
            self._done.set()

    def _remove_worker(self, node, worker):
        del self.nodes[node].workers[worker]

        if node != self.server_address:
            send(node, data={'request_type': 'remove_worker',
                             'node_address': node,
                             'worker_address': worker})

        if not self.nodes[node].workers:
            del self.nodes[node]

            if node != self.server_address:
                send(node, data={'request_type': 'shutdown'})

        if len(self.nodes) == 0:
            self._done.set()

    def _get_worker(self, request_type, database=None):

        if request_type in ('create_database', 'append_to_database', 'restore_database'):
            return self.server_address, self.nodes[self.server_address].get_worker()
        else:

            for node in sorted(self.nodes, key=lambda x: self.nodes[x].queue):

                if (database in self.nodes[node].databases and
                    (not database in self.databases or
                     self.nodes[node].databases[database] == self.databases[database])):
                    return node, self.nodes[node].get_worker()

    def _lock_worker(self, node, worker):
        self.nodes[node].workers[worker] += 1

    def _unlock_worker(self, node, worker):

        try:
            self.nodes[node].workers[worker] -= 1
        except KeyError:
            pass

    def sync(self):
        self.refresh_databases()

        for node in self.nodes:

            if node != self.server_address:
                databases2sync = list()
                node_databases = request(node, data={'request_type': 'list_databases'})[1]

                for local_database in self.databases:

                    if (local_database in node_databases and
                        self.databases[local_database] != node_databases[local_database]):
                        databases2sync.append(local_database)

                for database in databases2sync:
                    self._send_database(node, database)


class Node(object):

    def __init__(self, node_address):
        self.node_address = node_address
        self.workers = dict()
        self.databases = dict()

    def get_worker(self):
        return sorted(self.workers, key= lambda x: self.workers[x])[0]

    def refresh_databases(self):
        self.databases = request(self.node_address, request_type='list_databases')[1]

    @property
    def queue(self):
        return sum(queue for queue in self.workers.values())


class NodeServer(DatabaseServer):

    def __init__(self, central_address, root_path, debug=False):
        super().__init__((get_ip(), find_free_port()), CentralQueryHandler, root_path, debug)
        self.central_address = central_address
        self.workers = dict()
        self.type = 'node'

    def start(self):
        threading.Thread(target=self.serve_forever).start()
        start_workers(self.central_address, self.server_address, self.root_path, self._debug)
        self._wait()
        print(f"Central address: {self.central_address}")
        print(f"Workers: {len(self.workers)}")
        print(f"Databases: {len(self.databases)}")

    def shutdown(self):

        for worker in list(self.workers):
            send(worker, {'request_type': 'shutdown'})

        self._wait()
        super().shutdown()
        self._is_shut_down = True
        print(f"Node {self.server_address} shut down succesfully!")

    def _add_worker(self, node, worker, databases):
        self.workers[worker] = 0

        if len(self.workers) == WORKERS_PER_NODE:
            self._done.set()

    def _remove_worker(self, node, worker):
        del self.workers[worker]

        if len(self.workers) == 0:
            self._done.set()

    def cluster_info(self):

        if self._is_shut_down:
            print(f"Node is off!")
        else:
            print(request(self.central_address, request_type='cluster_info')[0])

    @property
    def queue(self):
        return sum(queue for queue in self.workers.values())

    def sync(self, database=None):
        self.refresh_databases()
        databases2sync = list()

        if database:
            databases2sync.append(database)
        else:
            central_databases = request(self.central_address, data={'request_type': 'list_databases'})[1]

            for local_database in self.databases:

                if (local_database in central_databases and
                    self.databases[local_database] != central_databases[local_database]):
                    databases2sync.append(local_database)

        for database in databases2sync:
            self._request_database(self.central_address, database)


class WorkerServer(DatabaseServer):

    def __init__(self, server_address, central_address, node_address, root_path, debug=False):
        super().__init__(server_address, WorkerQueryHandler, root_path, debug)
        self.central = central_address
        self.node = node_address
        self.type = 'worker'
        sys.stdout = LoggerWriter(self.log.info)
        sys.stderr = LoggerWriter(self.log.error)

    def start(self):
        send(self.central, {'request_type': 'add_worker',
                            'node_address': self.node,
                            'worker_address': self.server_address,
                            'databases': self.databases})
        self.serve_forever()

    def shutdown(self):
        send(self.central, {'request_type': 'remove_worker',
                            'node_address': self.node,
                            'worker_address': self.server_address})
        super().shutdown()
        self._is_shut_down = True
        print(f"Worker {self.server_address} shut down succesfully!")

    def shutdown_request(self, request):
        super().shutdown_request(request)
        send(self.central, {'request_type': 'unlock_worker',
                            'node_address': self.node,
                            'worker_address': self.server_address})


def start_worker(server_address, central_address, node_address, root_path, debug):
    worker = WorkerServer(server_address, central_address, node_address, root_path, debug)
    worker.start()


def start_workers(central_address, node_address, root_path, debug):
    workers = list()
    host = get_ip()

    for i in range(WORKERS_PER_NODE):
        worker = (host, find_free_port())
        Process(target=start_worker, args=(worker, central_address, node_address, root_path, debug)).start()
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

import os
import sys
from pathlib import Path
import logging
import traceback
import socketserver
import threading
from multiprocessing import Process, cpu_count
from nastranpy.results.database import Database
from nastranpy.results.results import process_query
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.results.connection import Connection, get_ip
from nastranpy.setup_logging import LoggerWriter


SERVER_PORT = 8080


class CentralQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        _, query, _ = connection.recv()
        self.server.log.debug(f"Processing query ('{query['request_type']}') from {self.client_address} in central server {self.server.server_address}")

        if query['request_type'] == 'shutdown':
            threading.Thread(target=self.server.shutdown).start()
        elif query['request_type'] == 'add_worker':
            self.server._add_worker(tuple(query['worker_address']), query['databases'])
        elif query['request_type'] == 'remove_worker':
            self.server._remove_worker(tuple(query['worker_address']))
        elif query['request_type'] == 'unlock_worker':
            self.server._unlock_worker(tuple(query['worker_address']))
        elif query['request_type'] == 'cluster_info':
            connection.send(self.server.info(print_to_screen=False))
        else:
            worker = self.server._get_worker(query['request_type'], query['path'])
            self.server._lock_worker(worker)
            connection.send(data={'redirection_address': worker})

class WorkerQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        _, query, _ = connection.recv()
        self.server.log.debug(f"Processing query ('{query['request_type']}') from {self.client_address} in worker server {self.server.server_address}")

        if query['request_type'] == 'shutdown':
            threading.Thread(target=self.server.shutdown).start()
        else:
            msg = ''
            path = self.server.root_path / query['path']

            if query['request_type'] == 'create_database':
                path.mkdir(parents=True, exist_ok=True)
                connection.send('Creating database ...', data=get_tables_specs())
                db = Database()
                db.create(query['files'], str(path), query['name'], query['version'],
                          database_project=query['project'], overwrite=True,
                          table_generator=connection.recv_tables())
                msg = 'Database created succesfully!'
            else:
                db = Database(str(path))

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

    def __init__(self, server_address, query_handler, root_path):
        super().__init__(server_address, query_handler)
        self.root_path = Path(root_path)
        self.set_log()

    def set_log(self):
        self.log = logging.getLogger('DatabaseServer')
        self.log.setLevel(logging.DEBUG)

        if not self.log.handlers:
            fh = logging.FileHandler(self.root_path / 'server.log')
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.log.addHandler(fh)

    def handle_error(self, request, client_address):
        super().handle_error(request, client_address)
        tb = traceback.format_exc()
        self.log.error(tb)
        Connection(connection_socket=request).send('#' + tb)


class CentralServer(DatabaseServer):

    def __init__(self, root_path):
        super().__init__((get_ip(), SERVER_PORT), CentralQueryHandler, root_path)
        self.nodes = dict()
        self.type = 'Central'
        self._done = threading.Event()

    def start(self):
        threading.Thread(target=self.serve_forever).start()
        self.nodes[self.server_address[0]] = Node(self.server_address, self.root_path, cpu_count() - 1)
        self.wait()
        print(f"Address: {self.server_address}")
        print(f"Nodes: {len(self.nodes)} ({self.n_workers} workers)")
        print(f"Databases: {len(self.databases)}")

    def wait(self):
        self._done.wait()
        self._done.clear()

    def shutdown(self):

        for node in list(self.nodes.values()):
            node.shutdown()

        self.wait()
        super().shutdown()
        print("Cluster shut down succesfully!")

    @property
    def databases(self):
        return {db for node in self.nodes.values() for db in node.databases}

    @property
    def n_workers(self):
        return sum(len(node) for node in self.nodes.values())

    def info(self, print_to_screen=True):
        info = list()
        info.append(f"Address: {self.server_address}")
        info.append(f"\n{len(self.nodes)} nodes ({self.n_workers} workers):")

        for node_address, node in self.nodes.items():
            info.append(f"  '{node_address}': {len(node)} workers ({node.queue} job/s in progress)")

        info.append(f"\n{len(self.databases)} databases:")

        for database in self.databases:
            info.append(f"  '{database}'")

        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info

    def _add_worker(self, worker_address, databases):

        if worker_address[0] not in self.nodes:
            self.nodes[worker_address[0]] = Node(self.server_address, None)

        self.nodes[worker_address[0]].workers[worker_address] = 0
        self.nodes[worker_address[0]].databases = set(databases)

        if not any(worker is None for worker in self.nodes[self.server_address[0]].workers):
            self._done.set()

    def _remove_worker(self, worker_address):

        del self.nodes[worker_address[0]].workers[worker_address]

        if not self.nodes[worker_address[0]].workers:
            del self.nodes[worker_address[0]]

        if len(self.nodes) == 0:
            self._done.set()

    def _get_worker(self, request_type, database=None):

        if request_type in ('create_database', 'append_to_database', 'restore_database'):
            node = self.nodes[self.server_address[0]]
        else:

            for node in sorted(self.nodes.values(), key=lambda x: x.queue):

                if database in node.databases:
                    break

            else:
                node = None

        for worker in sorted(node.workers, key=lambda x: node.workers[x]):
            return worker

    def _lock_worker(self, worker):
        self.nodes[worker[0]].workers[worker] += 1

    def _unlock_worker(self, worker):

        try:
            self.nodes[worker[0]].workers[worker] -= 1
        except KeyError:
            pass


class WorkerServer(DatabaseServer):

    def __init__(self, server_address, root_path, central_address):
        super().__init__(server_address, WorkerQueryHandler, root_path)
        self.central = central_address
        self.type = 'Worker'
        sys.stdout = LoggerWriter(self.log.info)
        sys.stderr = LoggerWriter(self.log.error)

    def start(self):
        send(self.central, {'request_type': 'add_worker',
                            'worker_address': self.server_address,
                            'databases': get_local_databases(self.root_path)})
        self.serve_forever()

    def shutdown(self):
        send(self.central, {'request_type': 'remove_worker',
                            'worker_address': self.server_address})
        super().shutdown()

    def shutdown_request(self, request):
        super().shutdown_request(request)
        send(self.central, {'request_type': 'unlock_worker',
                            'worker_address': self.server_address})


class Node(object):

    def __init__(self, central_address, root_path, n_workers=None, port=None):
        self.central_address = central_address

        if root_path is None:
            self.workers = dict()
            self.databases = set()

        else:
            self.root_path = Path(root_path)
            self.workers = {(get_ip(), SERVER_PORT + 1 + i if not port else port + i): None for i in
                            range(cpu_count() if n_workers is None else n_workers)}
            self.databases = set(get_local_databases(self.root_path))

            for worker in self.workers:
                Process(target=start_worker, args=(worker, self.root_path, central_address)).start()

    def __len__(self):
        return len(self.workers)

    def shutdown(self):

        for worker in list(self.workers):
            send(worker, {'request_type': 'shutdown'})

    @property
    def queue(self):
        return sum(worker_queue for worker_queue in self.workers.values())


def start_worker(server_address, root_path, central_address):
    worker = WorkerServer(server_address, root_path, central_address)
    worker.start()


def get_local_databases(root_path):
    return [str(header.parent.relative_to(root_path)) for header in
            root_path.glob('**/##header.json')]


def send(address, data):

    try:
        connection = Connection(address)
        connection.send(data=data)
    except Exception as e:
        raise ConnectionError(str(e))
    finally:
        connection.kill()

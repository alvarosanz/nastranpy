import os
from pathlib import Path
import logging
import traceback
import socketserver
import threading
from multiprocessing import Process, cpu_count
from nastranpy.results.database import Database, process_query
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.results.connection import Connection, get_ip


SERVER_PORT = 8080


class QueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = Connection(connection_socket=self.request)
        _, query, _ = connection.recv()
        self.handle_query(connection, query)

    def handle_error(self, request, client_address):
        super().handle_error(request, client_address)
        tb = traceback.format_exc()
        self.server.log.error(tb)
        Connection(connection_socket=self.request).send('#' + tb)


class CentralQueryHandler(QueryHandler):

    def handle_query(self, connection, query):

        if query['request_type'] == 'shutdown':
            self.server.shutdown()
        elif query['request_type'] == 'add_worker':
            self.server.add_worker(tuple(query['worker_address']), query['databases'])
        elif query['request_type'] == 'remove_worker':
            self.server.remove_worker(tuple(query['worker_address']))
        elif query['request_type'] == 'unlock_worker':
            self.server.unlock_worker(tuple(query['worker_address']))
        else:

            if query['request_type'] == 'create_database':
                query['path'] = None

            worker = self.server.get_worker(query['path'])

            if not worker:
                raise FileNotFoundError(f"Database '{query['path']}' not found!")

            self.server.lock_worker(worker)
            connection.send(data={'redirection_address': worker})


class WorkerQueryHandler(QueryHandler):

    def handle_query(self, connection, query):

        if query['request_type'] == 'shutdown':
            self.server.shutdown()
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
        self.log = logging.getLogger('DatabaseServer')
        self.log.setLevel(logging.DEBUG)
        fh = logging.FileHandler(self.root_path / 'server.log')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.log.addHandler(fh)

    def exchange(self, peer_address, data):

        try:
            connection = Connection(peer_address)
            connection.send(data=data)
        except Exception as e:
            raise ConnectionError(str(e))
        finally:
            connection.kill()


class CentralServer(DatabaseServer):

    def __init__(self, server_address, root_path):
        super().__init__(server_address, CentralQueryHandler, root_path)
        self.workers = dict()
        self.type = 'Central'

    def shutdown(self):

        for worker in self.workers:
            self.exchange(worker, {'request_type': 'shutdown'})

        super().shutdown()

    def add_worker(self, worker_address, databases):
        self.workers[worker_address] = {'queue': 0,
                                        'databases': set(databases)}

    def remove_worker(self, worker_address):
        del self.workers[worker_address]

    def get_worker(self, database=None):

        for worker in sorted(self.workers, key=lambda x: self.workers[x]['queue']):

            if not database or database in self.workers[worker]['databases']:
                return worker

    def lock_worker(self, worker):
        self.workers[worker]['queue'] += 1

    def unlock_worker(self, worker):
        self.workers[worker]['queue'] -= 1

    def get_databases(self):
        return {db for worker in self.workers for db in self.workers[worker]['databases']}


class WorkerServer(DatabaseServer):

    def __init__(self, server_address, root_path, central_address):
        super().__init__(server_address, WorkerQueryHandler, root_path)
        self.central = central_address
        self.type = 'Worker'

    def shutdown(self):
        self.exchange(self.central, {'request_type': 'remove_worker',
                                     'worker_address': self.server_address})
        super().shutdown()

    def shutdown_request(self, request):
        super().shutdown_request(request)
        self.exchange(self.central, {'request_type': 'unlock_worker',
                                     'worker_address': self.server_address})

    def get_databases(self):
        databases = list()

        for header in self.root_path.glob('**/##header.json'):
            databases.append(str(header.parent.relative_to(self.root_path)))

        return databases


def start_central_server(root_path):
    server = CentralServer((get_ip(), SERVER_PORT), root_path)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.start()
    processes = start_workers(server.server_address, root_path, cpu_count() - 1)
    print(f"Central server is ready! {server.server_address} ({len(server.workers)} workers)")
    print(f"{len(server.get_databases())} database/s available")
    return server, processes


def start_workers(central_address, root_path, n_workers=None):

    if n_workers is None:
        n_workers = cpu_count()

    processes = list()

    for i in range(n_workers):
        process = Process(target=start_worker, args=((get_ip(), SERVER_PORT + 1 + i),
                                                     root_path, central_address))
        process.start()
        processes.append(process)

    return processes


def start_worker(server_address, root_path, central_address):
    server = WorkerServer(server_address, root_path, central_address)
    server.exchange(central_address, {'request_type': 'add_worker',
                                      'worker_address': server.server_address,
                                      'databases': server.get_databases()})
    server.serve_forever()


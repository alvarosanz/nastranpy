import os
from pathlib import Path
from io import BytesIO
import json
import socket
import socketserver
import pandas as pd
from multiprocessing import Process, cpu_count
from nastranpy.results.database import Database, process_query
from nastranpy.results.read_results import tables_in_pch, ResultsTable
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.bdf.misc import humansize


class QueryHandler(socketserver.BaseRequestHandler):

    def handle(self):

        try:
            connection = Connection(connection_socket=self.request)
            _, query, _ = connection.recv()

            if query['request_type'] == 'shutdown':
                self.server.shutdown()
            elif query['request_type'] == 'add_child':
                self.server.add_child(self.client_address, query['databases'])
            elif query['request_type'] == 'remove_child':
                self.server.remove_child(self.client_address)
            elif query['request_type'] == 'unlock_child':
                self.server.unlock_child(self.client_address)
            elif self.childs:
                child = self.server.get_child(query['path'])
                self.server.lock_child(child)
                connection.send(data={'child_address': child})
            else:
                msg = ''
                path = Path(query['path'])

                if not self.server.root_path in path.parents:
                    path = self.server.root_path / path

                if query['request_type'] == 'create_database':
                    path.mkdir(parents=True, exist_ok=True)
                    connection.send('Creating database ...', data=get_tables_specs())
                    db = Database()
                    db.create(query['files'], query['path'], query['name'], query['version'],
                              database_project=query['project'], overwrite=True,
                              table_generator=connection.recv_tables())
                    msg = 'Database created succesfully!'
                else:
                    db = Database(query['path'])

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

        except Exception as e:
            connection.send('#' + str(e))
            raise Exception(str(e))


class Connection(object):

    def __init__(self, server_address=None, connection_socket=None,
                 header_size=12, buffer_size=4096):

        if server_address:
            self.connect(server_address)
        else:
            self.socket = connection_socket

        self.header_size = header_size
        self.buffer_size = buffer_size
        self.pending_data = b''
        self.last_send = None

    def connect(self, server_address):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(server_address)

    def kill(self):
        self.socket.close()

    def send(self, msg='', data=None, df=None):
        self.last_send = {'msg': msg, 'data': data, 'df': df}
        msg = msg.strip()
        buffer = BytesIO()
        buffer.seek(3 * self.header_size + len(msg))

        if data is None:
            data = b''
        else:
            data = json.dumps(data).encode()
            buffer.write(data)

        if not df is None:
            df.to_msgpack(buffer)

        position = buffer.tell()
        buffer.seek(0)
        buffer.write((str(position).zfill(self.header_size) +
                      str(len(msg)).zfill(self.header_size) +
                      str(len(data)).zfill(self.header_size) +
                      msg).encode())
        self.socket.sendall(buffer.getbuffer())

    def recv(self):
        buffer = BytesIO()
        size = 1
        msg = None

        while buffer.tell() < size:

            if (self.pending_data and
                len(self.pending_data) > self.header_size and
                len(self.pending_data) == int(self.pending_data[:self.header_size].decode())):
                data = b''
            else:
                data = self.socket.recv(self.buffer_size)

            if size == 1:
                data = self.pending_data + data
                self.pending_data = b''
                msg_size = int(data[self.header_size : 2 * self.header_size].decode())
                data_size = int(data[2 * self.header_size : 3 * self.header_size].decode())
                size = int(data[:self.header_size].decode()) - 3 * self.header_size
                data = data[3 * self.header_size:]

            buffer.write(data)

        buffer.seek(size)
        self.pending_data = buffer.read()
        buffer.seek(size)
        buffer.truncate()
        buffer.seek(0)
        msg = buffer.read(msg_size).decode()

        if msg and msg[0] == '#':
            raise ConnectionError(msg[1:])

        if data_size:
            data = json.loads(buffer.read(data_size).decode())

            if 'child_address' in data:
                self.kill()
                self.connect(data['child_address'])
                self.last_send['data']['parent_address'] = self.socket.getpeername()
                self.send(**self.last_send)
                return self.recv()

        else:
            data = None

        if size > 3 * self.header_size + msg_size + data_size:
            df = pd.read_msgpack(buffer)
        else:
            df = None

        return msg, data, df

    def send_tables(self, files, tables_specs):
        ignored_tables = set()

        for file in files:

            for table in tables_in_pch(file, tables_specs):
                name = '{} - {}'.format(table.name, table.element_type)

                if name not in tables_specs:

                    if name not in ignored_tables:
                        print("WARNING: '{}' is not supported!".format(name))
                        ignored_tables.add(name)

                    continue

                df = table.df
                del table.__dict__['df']
                self.send(data=table.__dict__, df=df)

        self.send('END')

    def recv_tables(self):

        while True:
            msg, data, df = self.recv()

            if msg == 'END':
                break

            table = ResultsTable(**data)
            table.df = df
            yield table

    def send_files(self, files):
        nbytes = sum(os.path.getsize(file) for file in files)
        print(f"Transferring {len(files)} file/s ({humansize(nbytes)}) ...")
        self.socket.sendall(str(nbytes).encode())
        answer = self.socket.recv(self.buffer_size)

        for i, file in enumerate(files):
            print(f"Sending {os.path.basename(file)} ({i + 1} of {len(files)}) ...")

            with open(file, 'rb') as f:
                sended = 1

                while sended:
                    sended = self.socket.send(f.read(self.buffer_size))

    def recv_files(self, delimiter='\n'):
        size = int(self.socket.recv(self.buffer_size).decode())
        self.socket.send(b'proceed')
        received = 0
        buffer = ''

        while received < size:
            data = self.socket.recv(self.buffer_size).decode()
            received += len(data)
            buffer += data

            while buffer.find(delimiter) != -1:
                line, buffer = buffer.split(delimiter, 1)
                yield line


class DatabaseServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    request_queue_size = 5

    def __init__(self, server_address, root_path, parent_address=None, subprocesses=None):
        super().__init__(server_address, QueryHandler)
        self.root_path = Path(root_path)
        self.parent = parent_address
        self.childs = dict()
        host, port = self.server_address

        for i in range(cpu_count() - 1 if subprocesses is None else subprocesses):
            process = Process(target=start_server,
                              args=((host, port + i + 1), root_path,
                                    self.server_address if parent_address is None else parent_address, 0))
            process.start()

        if self.parent:
            self.send2parent({'request_type': 'add_child',
                              'databases': self.get_databases()})

    def shutdown(self):

        if self.parent:
            self.send2parent({'request_type': 'remove_child'})
        elif self.childs:

            for child in self.childs:
                self.send2parent({'request_type': 'shutdown'})

        super().shutdown()

    def shutdown_request(self):
        super().shutdown_request()

        if self.parent:
            self.send2parent({'request_type': 'unlock_child'})

    def send2parent(self, data):

        try:
            connection = Connection(self.parent)
            connection.send(data=data)
        finally:
            connection.kill()

    def add_child(self, child_address, databases):
        self.childs[child_address] = {'queue': 0,
                                      'databases': child_databases}

    def remove_child(self, child_address):
        del self.childs[child_address]

    def get_child(self, database):

        for child in sorted(self.childs, lambda x: self.childs[x]['queue']):

            if database in self.childs[child]['databases']:
                return child

    def lock_child(self, child):
        self.childs[child]['queue'] += 1

    def unlock_child(self, child):
        self.childs[child]['queue'] -= 1

    def get_databases(self):
        databases = list()

        for header in self.root_path.glob('**/##header.json')
            databases.append(header.parent())

        return databases


def start_server(server_address, root_path, parent_address=None, n_child=None):
    server = DatabaseServer(server_address, root_path, parent_address, n_child)
    server.serve_forever()

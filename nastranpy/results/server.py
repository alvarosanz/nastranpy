import os
from pathlib import Path
from io import StringIO, BytesIO
import json
import socket
import socketserver
import pandas as pd
from nastranpy.results.database import Database, process_query
from nastranpy.results.read_results import tables_in_pch, ResultsTable
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.bdf.misc import humansize


class QueryHandler(socketserver.BaseRequestHandler):

    def handle(self):

        try:
            connection = Connection(connection_socket=self.request)
            _, query, _ = connection.recv()

            if self.server.childs:

                if query['request_type'] == 'unlock_child':
                    self.server.lock_child(query['child_address'])
                else:
                    child = self.server.get_child()
                    self.server.lock_child(child)
                    connection.send(data={'child_address': child,
                                          'redir2child': True})

            elif query['request_type'] == 'check_child':

                try:
                    parent_connection = Connection(self.server.parent)
                    parent_connection.send(data={'check_child': self.server.parent == query['parent_address'],
                                                 'databases': self.server.get_databases()})

                finally:
                    parent_connection.kill()
            else:
                msg = ''
                path = Path(query['path'])

                if not self.server.root_path in path.parents:
                    raise PermissionError(f"'{path}' is not a valid path!")

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

            if 'child_address' in data and 'redir2child' in data and data['redir2child']:
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

    def __init__(self, server_address, root_path,
                 parent_address=None, child_addresses=None):
        super().__init__(server_address, QueryHandler)
        self.root_path = Path(root_path)

        if parent_address and child_addresses:
            raise ValueError('A server cannot be parent and child at the same time!')

        self.parent = parent_address
        self.childs = dict()

        for child_address in child_addresses:
            self.add_child(child_address)

    def check_child(self, child_address, databases):

        try:
            connection = Connection(child_address)
            connection.send(data={'request_type': 'check_child',
                                  'parent_address': self.server_address})
            _, data, _ = connection.recv()

            is_OK = data['check_child']
            databases += data['databases']

        except:
            is_OK = False
        finally:
            connection.kill()

        return is_OK

    def add_child(self, child_address):
        child_databases = list()

        if check_child(child_address, child_databases):
            self.childs[child_address] = {'is_busy': False,
                                          'databases': child_databases}
        else:
            print(f"WARNING: Child server not available: {child_address}")

    def remove_child(self, child_address):
        del self.childs[child_address]

    def get_child(self, database_path):

        for child in self.childs:

            if not self.childs[child]['is_busy'] and self.check_child(child):
                return child

    def lock_child(self, child):
        self.childs[child]['is_busy'] = True

    def unlock_child(self, child):
        self.childs[child]['is_busy'] = False

    def get_databases(self):
        return list()

    def shutdown_request(self):
        super().shutdown_request()

        if self.parent:

            try:
                connection = Connection(self.parent)
                connection.send(data={'request_type': 'unlock_child',
                                      'child_address': self.server_address})
            finally:
                connection.kill()


def start_server(server_address, root_path,
                 parent_address=None, child_addresses=None):
    server = DatabaseServer(server_address, root_path, parent_address, child_addresses)
    server.serve_forever()


if __name__ == '__main__':
    start_server(('127.0.0.1', 8080))

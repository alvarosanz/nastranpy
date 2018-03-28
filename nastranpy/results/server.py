import os
from pathlib import Path
from io import StringIO, BytesIO
import json
import socket
import socketserver
import pandas as pd
from nastranpy.results.results import create_database
from nastranpy.results.database import Database, process_query
from nastranpy.bdf.misc import humansize


class QueryHandler(socketserver.BaseRequestHandler):

    def handle(self):

        try:
            connection = Connection(connection_socket=self.request)
            _, query, _ = connection.recv()
            msg = ''
            path = Path(query['path'])

            if not self.server.root_path in path.parents:
                raise PermissionError(f"'{path}' is not a valid path!")

            if query['request_type'] == 'create_database':
                path.mkdir(parents=True, exist_ok=True)
                connection.send('Creating database ...')
                db = create_database([connection.recv_files()], query['path'], query['name'], query['version'],
                                     database_project=query['project'], overwrite=True,
                                     filenames=query['files'])
                msg = 'Database created succesfully!'
            else:
                db = Database(query['path'])

            df = None

            if query['request_type'] == 'check':
                msg = db.check(print_to_screen=False)
            elif query['request_type'] == 'query':
                df=db.query(**process_query(query))
            elif query['request_type'] == 'append_to_database':
                connection.send('Appending to database ...')
                db.append([connection.recv_files()], query['batch'], filenames=query['files'])
                msg = 'Database created succesfully!'
            elif query['request_type'] == 'restore_database':
                db.restore(query['batch'])
                msg = f"Database restored to '{query['batch']}' state succesfully!"

            connection.send(msg, data=db._export_header(), dataframe=df)

        except Exception as e:
            connection.send('#' + str(e))
            raise Exception(str(e))


class Connection(object):

    def __init__(self, server_address=None, connection_socket=None,
                 header_size=12, buffer_size=4096):

        if server_address:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect(server_address)
        else:
            self.socket = connection_socket

        self.header_size = header_size
        self.buffer_size = buffer_size

    def kill(self):
        self.socket.close()

    def send(self, msg='', data=None, dataframe=None):
        msg = msg.strip()
        buffer = BytesIO()
        buffer.seek(3 * self.header_size + len(msg))

        if data is None:
            data = b''
        else:
            data = json.dumps(data).encode()
            buffer.write(data)

        if not dataframe is None:
            dataframe.to_msgpack(buffer)

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
            data = self.socket.recv(self.buffer_size)

            if size == 1:
                msg_size = int(data[self.header_size : 2 * self.header_size].decode())
                data_size = int(data[2 * self.header_size : 3 * self.header_size].decode())
                size = int(data[:self.header_size].decode()) - 3 * self.header_size
                data = data[3 * self.header_size:]

            buffer.write(data)

        buffer.seek(0)
        msg = buffer.read(msg_size).decode()

        if msg and msg[0] == '#':
            raise ConnectionError(msg[1:])

        if data_size:
            data = json.loads(buffer.read(data_size).decode())
        else:
            data = None

        if size > 3 * self.header_size + msg_size + data_size:
            dataframe = pd.read_msgpack(buffer)
        else:
            dataframe = None

        return msg, data, dataframe

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

    def __init__(self, server_address, root_path):
        super().__init__(server_address, QueryHandler)
        self.root_path = Path(root_path)


def start_server(server_address, root_path):
    server = DatabaseServer(server_address, root_path)
    server.serve_forever()


if __name__ == '__main__':
    start_server(('127.0.0.1', 8080))

import os
from io import StringIO, BytesIO
import json
import socket
import socketserver
import pandas as pd
from nastranpy.results.results import create_database
from nastranpy.results.database import Database, process_query


class QueryHandler(socketserver.BaseRequestHandler):
    header_size = 12
    answer_size = 80

    def handle(self):
        msg = self.get_request()

        if msg['request_type'] == 'create_database':

            try:
                self.answer('Creating database ...')
                db = create_database([self.readlines()], msg['path'], msg['name'], msg['version'],
                                     database_project=msg['project'], overwrite=True,
                                     filenames=msg['files'])
                log = 'Database created succesfully!'
                self.answer(log, data_type='json', data=db._export_header())
            except FileExistsError as e:
                self.answer(str(e), False)

            return

        else:

            try:
                db = Database(msg['path'])
            except FileNotFoundError as e:
                self.answer(str(e), False)

        if msg['request_type'] == 'header':
            self.answer(data_type='json', data=db._export_header())
        elif msg['request_type'] == 'check':
            self.answer(data_type='text', data=db.check(print_to_screen=False))
        elif msg['request_type'] == 'query':
            self.answer(data_type='pandas', data=db.query(**process_query(msg)))
        elif msg['request_type'] == 'append_to_database':
            self.answer('Appending to database ...')
            db.append([self.readlines()], msg['batch'], filenames=msg['files'])
            log = 'Database created succesfully!'
            self.answer(log, data_type='json', data=db._export_header())
        elif msg['request_type'] == 'restore_database':

            try:
                db.restore(msg['batch'])
                log = f"Database restored to '{msg['batch']}' state succesfully!"
                self.answer(log, data_type='json', data=db._export_header())
            except ValueError as e:
                self.answer(str(e), False)

    def get_request(self, buffer_size=4096):
        buffer = StringIO()
        size = None

        while not size or buffer.tell() < size:
            data = self.request.recv(buffer_size)

            if not size:
                size = int(data[:self.header_size].decode()) - self.header_size
                data = data[self.header_size:]

            buffer.write(data.decode())

        buffer.seek(0)
        return json.load(buffer)

    def answer(self, answer='', is_OK=True, data_type='', data=None):

        if is_OK:
            answer = 'OK' + data_type.ljust(8) + answer
        else:
            answer = 'KO' + data_type.ljust(8) + answer

        buffer = BytesIO()
        buffer.seek(self.header_size + self.answer_size)

        if not data is None:

            if data_type == 'json':
                buffer.write(json.dumps(data).encode())
            elif data_type == 'text':
                buffer.write(data.encode())
            elif data_type == 'pandas':
                data.to_msgpack(buffer)

        size = str(buffer.tell()).zfill(self.header_size).encode()
        buffer.seek(0)
        buffer.write(size + answer.ljust(self.answer_size).encode())
        self.request.sendall(buffer.getvalue())

    def readlines(self, delimiter='\n', buffer_size=4096):
        size = int(self.request.recv(buffer_size).decode())
        self.answer()
        buffer = ''
        received = 0

        while received < size:
            data = self.request.recv(buffer_size)
            buffer += data.decode()
            received += len(data)

            while buffer.find(delimiter) != -1:
                line, buffer = buffer.split('\n', 1)
                yield line


class DatabaseServer(socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, server_address):
        super().__init__(server_address, QueryHandler)


class Connection(object):

    def __init__(self, server_address, header_size=12, answer_size=80, buffer_size=4096):
        self.server_address = server_address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.header_size = header_size
        self.answer_size = answer_size
        self.buffer_size = buffer_size
        self.socket.connect(self.server_address)

    def request(self, msg=None, files=None):

        if files:
            nbytes = sum(os.path.getsize(file) for file in files)
            self.socket.sendall(str(nbytes).zfill(self.header_size).encode())
            self._recv()

            for i, file in enumerate(files):
                print(f"Sending {os.path.basename(file)} ({i + 1} of {len(files)}) ...")

                with open(file, 'rb') as f:
                    sended = 1

                    while sended:
                        sended = self.socket.send(f.read(self.buffer_size))

        else:
            msg = json.dumps(msg).encode()
            header = str(len(msg) + self.header_size).zfill(self.header_size).encode()
            self.socket.sendall(header + msg)

        return self._recv()

    def kill(self):
        self.socket.close()

    def _recv(self):
        buffer = BytesIO()
        size = None
        answer = None

        while size is None or buffer.tell() < size:
            data = self.socket.recv(self.buffer_size)

            if size is None:
                size = int(data[:self.header_size].decode()) - self.header_size - self.answer_size
                answer = data[self.header_size:self.header_size + self.answer_size].decode()
                status = answer[:2]
                data_type = answer[2:10].strip()
                answer = answer[10:].strip()
                data = data[self.header_size + self.answer_size:]

                if status == 'KO':
                    raise ConnectionError(answer)

            buffer.write(data)

        buffer.seek(0)

        if data_type == 'json':
            return answer, json.loads(buffer.read().decode())
        elif data_type == 'text':
            return answer, buffer.read().decode()
        elif data_type == 'pandas':
            return answer, pd.read_msgpack(buffer)
        elif not data_type:
            return answer, None


def start_server(server_address):
    server = DatabaseServer(server_address)
    server.serve_forever()


if __name__ == '__main__':
    start_server(('127.0.0.1', 8080))

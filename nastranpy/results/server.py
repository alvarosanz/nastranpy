import os
from io import StringIO, BytesIO
import json
import socketserver
from nastranpy.results.results import create_database
from nastranpy.results.database import Database, process_query


class QueryHandler(socketserver.BaseRequestHandler):
    header_size = 12
    answer_size = 80

    def handle(self):
        msg = self.recv_msg()

        if msg['request_type'] == 'create_database':

            try:
                self.send('OK#Creating database ...')
                db = create_database([self.readlines()], msg['path'], msg['name'], msg['version'],
                                     database_project=msg['project'], overwrite=True,
                                     filenames=msg['files'])
                log = 'Database created succesfully!'
                self.send(log, data=json.dumps(db._export_header()).encode())
            except FileExistsError as e:
                self.send(str(e))

            return

        else:

            try:
                db = Database(msg['path'])
            except FileNotFoundError as e:
                self.send(str(e))

        if msg['request_type'] == 'header':
            self.send(data=json.dumps(db._export_header()).encode())
        elif msg['request_type'] == 'check':
            self.send(data=db.check(print_to_screen=False).encode())
        elif msg['request_type'] == 'query':
            df = db.query(**process_query(msg))
            self.send(data=df.to_msgpack)
        elif msg['request_type'] == 'append_to_database':
            self.send('OK#Appending to database ...')
            db.append([self.readlines()], msg['batch'], filenames=msg['files'])
            log = 'Database created succesfully!'
            self.send(log, data=json.dumps(db._export_header()).encode())
        elif msg['request_type'] == 'restore_database':

            try:
                db.restore(msg['batch'])
                log = f"Database restored to '{msg['batch']}' state succesfully!"
                self.send(log, data=json.dumps(db._export_header()).encode())
            except ValueError as e:
                self.send(str(e))

    def recv_msg(self, buffer_size=4096):
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

    def send(self, answer='OK#', data=None):
        buffer = BytesIO()
        buffer.seek(self.header_size + self.answer_size)

        if not data is None:

            try:
                data(buffer)
            except TypeError:
                buffer.write(data)

        position = str(buffer.tell()).zfill(self.header_size).encode()
        buffer.seek(0)
        buffer.write(position + answer.ljust(self.answer_size).encode())
        self.request.sendall(buffer.getvalue())

    def readlines(self, delimiter='\n', buffer_size=4096):
        size = int(self.request.recv(buffer_size).decode())
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


def start_server(server_address):
    server = DatabaseServer(server_address)
    server.serve_forever()


if __name__ == '__main__':
    start_server(('127.0.0.1', 8080))

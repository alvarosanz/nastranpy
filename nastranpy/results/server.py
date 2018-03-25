from io import StringIO, BytesIO
import json
import socketserver
from nastranpy.results.results import create_database
from nastranpy.results.database import DataBase, process_query


class QueryHandler(socketserver.BaseRequestHandler):
    header_size = 12

    def handle(self):
        msg = self.recv_json()
        database = DataBase(msg['path'])

        if msg['request_type'] == 'info':
            self.send_data(database.info(print_to_screen=False, detailed=True).encode())
        elif msg['request_type'] == 'check':
            self.send_data(database.check(print_to_screen=False).encode())
        elif msg['request_type'] == 'query':
            df = database.query(**process_query(msg))
            self.send_data(df.to_msgpack)

    def recv_json(self, buffer_size=4096):
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

    def send_data(self, data):
        buffer = BytesIO()
        buffer.seek(self.header_size)

        try:
            data(buffer)
        except TypeError:
            buffer.write(data)

        position = str(buffer.tell()).zfill(self.header_size).encode()
        buffer.seek(0)
        buffer.write(position)
        self.request.sendall(buffer.getvalue())


class DatabaseServer(socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, server_address):
        super().__init__(server_address, QueryHandler)


def start_server(server_address):
    server = DatabaseServer(server_address)
    server.serve_forever()


if __name__ == '__main__':
    start_server(('127.0.0.1', 8080))

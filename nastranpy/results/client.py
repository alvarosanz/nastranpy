import json
from io import BytesIO
import socket
import pandas as pd
from nastranpy.results.database import process_query


class Client(object):

    def __init__(self, server_address, database_path):
        self.server_address = server_address
        self.database_path = database_path

    def request(self, msg):
        request = Request(self.server_address)
        msg['path'] = self.database_path
        buffer = request.send(msg)

        if msg['request_type'] in ('info', 'check'):
            return buffer.getvalue().decode()
        elif msg['request_type'] == 'query':
            df = pd.read_msgpack(buffer)

            if msg['output_path']:
                df.csv(msg['output_path'])

            return df

    def info(self):
        print(self.request({'request_type': 'info'}))

    def check(self):
        print(self.request({'request_type': 'check'}))

    def query(self, file):

        with open(file) as f:
            query = json.load(f)

        query['request_type'] = 'query'
        return self.request(query)


class Request(object):

    def __init__(self, server_address, header_size=12):
        self.server_address = server_address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.header_size = header_size

    def send(self, msg, buffer_size=4096):

        try:
            self.socket.connect(self.server_address)
            self._send_json(msg)
            buffer = self._recv_buffer(buffer_size)
        finally:
            self.socket.close()

        return buffer

    def _send_json(self, msg):
        msg = json.dumps(msg).encode()
        header = str(len(msg) + self.header_size).zfill(self.header_size).encode()
        self.socket.sendall(header + msg)

    def _recv_buffer(self, buffer_size):
        buffer = BytesIO()
        size = None

        while not size or buffer.tell() < size:
            data = self.socket.recv(buffer_size)

            if not size:
                size = int(data[:self.header_size].decode()) - self.header_size
                data = data[self.header_size:]

            buffer.write(data)

        buffer.seek(0)
        return buffer


def query_server(file):

    with open(file) as f:
        query = json.load(f)

    client = Client((query['host'], query['port']), query['path'])
    return client.request(query)

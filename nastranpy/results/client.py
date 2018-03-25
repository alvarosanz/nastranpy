import os
import json
from io import BytesIO
import socket
import pandas as pd
from nastranpy.results.database import ParentDatabase, is_loaded, process_query


class DatabaseClient(ParentDatabase):

    def __init__(self, server_address, path=None):
        self.server_address = server_address
        self.path = path
        self.clear()

        if self.path:
            self._request({'request_type': 'header'})

    def _set_headers(self, headers):
        self._headers = headers['headers']
        self._project = headers['project']
        self._name = headers['name']
        self._version = headers['version']
        self._batches = headers['batches']
        self._nbytes = headers['nbytes']

    def clear(self):
        self._headers = None
        self._project = None
        self._name = None
        self._version = None
        self._batches = None
        self._nbytes = None

    @is_loaded
    def check(self):
        self._request({'request_type': 'check'})

    @is_loaded
    def append(self, files=None, request_file=None):
        return self._complex_request('append_to_database', files, request_file)

    @is_loaded
    def restore(self, batch_name):
        self._request({'request_type': 'restore_database',
                      'batch': batch_name})

    @is_loaded
    def query(self, request_file):
        return self._complex_request('query', request_file=request_file)

    def create(self, database_path, database_name, database_version,
               database_project=None, files=None, request_file=None):

        if self.path:
            raise ValueError('Database already loaded!')

        self.path = database_path
        self._project = database_project
        self._name = database_name
        self._version = database_version
        return self._complex_request('create_database', files, request_file)

    def _request(self, msg):
        request = Request(self.server_address)
        msg['path'] = self.path
        msg['project'] = self._project
        msg['name'] = self._name
        msg['version'] = self._version

        answer, buffer = request.send(json.dumps(msg).encode())

        if answer[:3] == 'OK#':

            if msg['request_type'] == 'query':
                df = pd.read_msgpack(buffer)

                if msg['output_path']:
                    df.csv(msg['output_path'])

                return df

            elif msg['request_type'] in ('create_database', 'append_to_database'):
                print(answer[3:])
                answer, buffer = request.send(files=msg['files'], kill=True)
                print(answer)
                self._set_headers(json.loads(buffer.read().decode()))
            elif msg['request_type'] == 'restore_database':
                print(answer)
                self._set_headers(json.loads(buffer.read().decode()))
            elif msg['request_type'] == 'header':
                self._set_headers(json.loads(buffer.read().decode()))
            else:
                print(buffer.getvalue().decode())
        else:
            request.kill()
            raise ConnectionError(answer)

    def _complex_request(self, request_type, files=None, request_file=None):

        if files:

            if isinstance(files, str):
                files = [files]

            query = {'request_type': request_type,
                     'files': files}

        else:

            with open(request_file) as f:
                query = json.load(f)

            query['request_type'] = request_type

        return self._request(query)


class Request(object):

    def __init__(self, server_address, header_size=12, answer_size=80):
        self.server_address = server_address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.header_size = header_size
        self.answer_size = answer_size
        self._is_connected = False

    def send(self, msg=None, files=None, buffer_size=4096, kill=False):

        try:

            if not self._is_connected:
                self.socket.connect(self.server_address)
                self._is_connected = True

            if files:
                nbytes = sum(os.path.getsize(file) for file in files)
                self.socket.sendall(str(nbytes).zfill(self.header_size).encode())

                for file in files:

                    with open(file, 'rb') as f:
                        sended = 1

                        while sended:
                            sended = self.socket.send(f.read(buffer_size))

            else:
                header = str(len(msg) + self.header_size).zfill(self.header_size).encode()
                self.socket.sendall(header + msg)

            buffer = self._recv_buffer(buffer_size)

        finally:

            if kill:
                self.kill()

        return buffer

    def kill(self):
        self.socket.close()

    def _recv_buffer(self, buffer_size):
        buffer = BytesIO()
        size = None
        answer = None

        while size is None or buffer.tell() < size:
            data = self.socket.recv(buffer_size)

            if size is None:
                size = int(data[:self.header_size].decode()) - self.header_size - self.answer_size
                answer = data[self.header_size:self.header_size + self.answer_size].decode().strip()
                data = data[self.header_size + self.answer_size:]

            buffer.write(data)

        buffer.seek(0)
        return answer, buffer


def query_server(file):

    with open(file) as f:
        query = json.load(f)

    client = DatabaseClient((query['host'], query['port']), query['path'])
    return client.request(query)

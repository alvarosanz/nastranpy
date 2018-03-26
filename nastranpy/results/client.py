import json
from nastranpy.results.database import ParentDatabase, get_query_from_file
from nastranpy.results.server import Connection


class DatabaseClient(ParentDatabase):

    def __init__(self, server_address, path=None):
        self.server_address = server_address
        self.path = path
        self.clear()

        if self.path:
            self._request('header')

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

    def info(self, print_to_screen=True, detailed=False):
        print(f"Host: {self.server_address[0]}")
        print(f"Port: {self.server_address[1]}")

        if self._headers:
            super().info(print_to_screen, detailed)

    def check(self):
        self._request('check')

    def append(self, files, batch_name):
        return self._request('append_to_database', files=files, batch=batch_name)

    def restore(self, batch_name):

        if batch_name not in self.restore_points or batch_name == self.restore_points[-1]:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        self._request('restore_database', batch=batch_name)

    def query(self, request_file):
        query = get_query_from_file(request_file)
        query.pop('request_type', None)
        return self._request('query', **query)

    def create(self, files, database_path, database_name, database_version,
               database_project=None):

        if self.path:
            raise ValueError('Database already loaded!')

        self.path = database_path
        self._project = database_project
        self._name = database_name
        self._version = database_version
        return self._request('create_database', files=files)

    def _request(self, request_type, **kwargs):

        if request_type not in ('header', 'create_database') and self._headers is None:
            print('You must load a database first!')

        msg = dict()

        for key, value in kwargs.items():

            if key == 'files' and isinstance(value, str):
                value = [value]

            msg[key] = value

        msg['path'] = self.path
        msg['project'] = self._project
        msg['name'] = self._name
        msg['version'] = self._version
        msg['request_type'] = request_type

        try:
            connection = Connection(self.server_address)
            answer, data = connection.request(msg)

            if answer:
                print(answer)

            if request_type in ('create_database', 'append_to_database'):
                answer, data = connection.request(files=msg['files'])
                print(answer)

        finally:
            connection.kill()

        if request_type == 'query':

            if msg['output_path']:
                print(f"Writing '{os.path.basename(msg['output_path'])}' ...")
                data.to_csv(msg['output_path'])

            return data

        elif request_type in ('header', 'create_database', 'append_to_database', 'restore_database'):
            self._set_headers(data)


def query_server(file):

    with open(file) as f:
        query = json.load(f)

    client = DatabaseClient((query['host'], query['port']), query['path'])
    return client.request(query)

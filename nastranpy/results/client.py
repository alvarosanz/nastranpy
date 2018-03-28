import json
from nastranpy.results.database import ParentDatabase, get_query_from_file
from nastranpy.results.server import Connection


class DatabaseClient(ParentDatabase):

    def __init__(self, server_address, path=None):
        self.server_address = server_address
        self.path = path
        self.clear()

        if self.path:
            self._request(request_type='header', path=self.path)

    def _set_headers(self, headers):
        self.path = headers['path']
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
        self._request(request_type='check', path=self.path)

    def append(self, files, batch_name):
        return self._request(request_type='append_to_database', path=self.path, files=files, batch=batch_name)

    def restore(self, batch_name):

        if batch_name not in self.restore_points or batch_name == self.restore_points[-1]:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        self._request(request_type='restore_database', path=self.path, batch=batch_name)

    def query(self, request_file):
        query = get_query_from_file(request_file)
        query['request_type'] = 'query'

        if self.path:
            query['path'] = self.path

        query['host'] = self.server_address[1]
        query['port'] = self.server_address[1]
        return self._request(**query)

    def create(self, files, database_path, database_name, database_version,
               database_project=None):

        if self.path:
            raise ValueError('Database already loaded!')

        return self._request(request_type='create_database', path=database_path, files=files,
                             name=database_name, version=database_version, project=database_project)

    def _request(self, **kwargs):

        if 'files' in kwargs and isinstance(kwargs['files'], str):
            kwargs['files'] = [kwargs['files']]

        try:
            connection = Connection(self.server_address)
            connection.send(data=kwargs)
            msg, data, dataframe = connection.recv()

            if msg:
                print(msg)

            if kwargs['request_type'] in ('create_database', 'append_to_database'):
                connection.send_files(kwargs['files'])
                msg, data, _ = connection.recv()
                print(msg)

        finally:
            connection.kill()

        self._set_headers(data)

        if kwargs['request_type'] == 'query':

            if kwargs['output_path']:
                print(f"Writing '{os.path.basename(kwargs['output_path'])}' ...")
                dataframe.to_csv(kwargs['output_path'])

            return dataframe


def query_server(file):

    with open(file) as f:
        query = json.load(f)

    client = DatabaseClient((query['host'], query['port']), query['path'])
    return client.request(query)

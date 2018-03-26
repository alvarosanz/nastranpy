import json
from nastranpy.results.database import ParentDatabase, is_loaded, get_query_from_file
from nastranpy.results.server import Connection


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
    def append(self, files, batch_name):
        return self._complex_request('append_to_database', files, batch=batch_name)

    @is_loaded
    def restore(self, batch_name):

        if batch_name not in self.restore_points or batch_name == self.restore_points[-1]:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        self._request({'request_type': 'restore_database',
                      'batch': batch_name})

    @is_loaded
    def query(self, request_file):
        return self._complex_request('query', request_file=request_file)

    def create(self, files, database_path, database_name, database_version,
               database_project=None):

        if self.path:
            raise ValueError('Database already loaded!')

        self.path = database_path
        self._project = database_project
        self._name = database_name
        self._version = database_version
        return self._complex_request('create_database', files)

    def _request(self, msg):

        try:
            connection = Connection(self.server_address)
            msg['path'] = self.path
            msg['project'] = self._project
            msg['name'] = self._name
            msg['version'] = self._version
            answer, data = connection.request(msg)

            if msg['request_type'] == 'header':
                self._set_headers(data)
            elif msg['request_type'] == 'query':

                if msg['output_path']:
                    print(f"Writing '{os.path.basename(msg['output_path'])}' ...")
                    data.to_csv(msg['output_path'])

                return data

            print(answer)

            if msg['request_type'] in ('create_database', 'append_to_database'):
                answer, data = connection.request(files=msg['files'])
                print(answer)
                self._set_headers(data)
            elif msg['request_type'] == 'restore_database':
                self._set_headers(data)

        finally:
            connection.kill()

    def _complex_request(self, request_type, files=None, request_file=None, **kwargs):

        if files:

            if isinstance(files, str):
                files = [files]

            query = {'request_type': request_type,
                     'files': files}

        else:
            query = get_query_from_file(request_file)
            query['request_type'] = request_type

        for key, value in kwargs.items():
            query[key] = value

        return self._request(query)


def query_server(file):

    with open(file) as f:
        query = json.load(f)

    client = DatabaseClient((query['host'], query['port']), query['path'])
    return client.request(query)

import json
from nastranpy.results.database import ParentDatabase, get_query_from_file
from nastranpy.results.server import Connection


class DatabaseClient(ParentDatabase):

    def __init__(self, server_address, path=None):
        self.server_address = server_address
        self.path = path
        self._is_local = False
        self.reload()

    def info(self, print_to_screen=True, detailed=False):
        print(f"Server: {self.server_address[0]} ({self.server_address[1]})")

        if self._headers:
            super().info(print_to_screen, detailed)

    def check(self):
        self._request(request_type='check', path=self.path)

    def create(self, files, database_path, database_name, database_version,
               database_project=None):

        if self.path:
            raise ValueError('Database already loaded!')

        return self._request(request_type='create_database', path=database_path, files=files,
                             name=database_name, version=database_version, project=database_project)

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

    def _request(self, **kwargs):

        if 'files' in kwargs and isinstance(kwargs['files'], str):
            kwargs['files'] = [kwargs['files']]

        try:
            connection = Connection(self.server_address)
            connection.send(data=kwargs)
            msg, data, df = connection.recv()

            if kwargs['request_type'] == 'header':
                return data

            if msg:
                print(msg)

            if kwargs['request_type'] in ('create_database', 'append_to_database'):
                connection.send_tables(kwargs['files'], data)
                msg, data, _ = connection.recv()
                print(msg)

        finally:
            connection.kill()

        self.reload(data)

        if kwargs['request_type'] == 'query':

            if kwargs['output_path']:
                print(f"Writing '{os.path.basename(kwargs['output_path'])}' ...")
                df.to_csv(kwargs['output_path'])

            return df

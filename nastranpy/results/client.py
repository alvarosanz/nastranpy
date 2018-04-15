import getpass
import json
from nastranpy.results.database import ParentDatabase
from nastranpy.results.connection import Connection, get_private_key
from nastranpy.bdf.misc import get_hash


class DatabaseClient(ParentDatabase):

    def __init__(self, server_address, path, private_key, authentication, headers=None):
        self.server_address = server_address
        self.path = path
        self._private_key = private_key
        self._authentication = authentication
        self._is_local = False
        self.reload(headers)

    def info(self, print_to_screen=True, detailed=False):
        print(f"Address: {self.server_address[0]} ({self.server_address[1]})")

        if self._headers:
            super().info(print_to_screen, detailed)

    def check(self):
        self._request(request_type='check')

    def append(self, files, batch_name):
        self._request(request_type='append_to_database', files=files, batch=batch_name)

    def restore(self, batch_name):

        if batch_name not in self.restore_points or batch_name == self.restore_points[-1]:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        self._request(request_type='restore_database', batch=batch_name)

    def query(self, table=None, outputs=None, LIDs=None, EIDs=None,
              geometry=None, weights=None, output_file=None, **kwargs):
        query = {'table': table, 'outputs': outputs, 'LIDs': LIDs, 'EIDs': EIDs,
                 'geometry': geometry, 'weights': weights, 'output_file': output_file}
        return self._request(request_type='query', **query)

    def _request(self, **kwargs):

        try:
            kwargs['path'] = self.path
        except AttributeError:
            pass

        if 'files' in kwargs and isinstance(kwargs['files'], str):
            kwargs['files'] = [kwargs['files']]

        try:
            connection = Connection(self.server_address, private_key=self._private_key)

            if self._authentication:
                connection.send_secret(json.dumps({'authentication': self._authentication}))
            else:
                connection.send_secret(json.dumps({'user': input('user: '),
                                                   'password': getpass.getpass('password: '),
                                                   'request': 'authentication'}))
                self._authentication = connection.recv_secret()

            connection.recv()
            connection.send(data=kwargs)
            msg, data, df = connection.recv()

            if data and 'redirection_address' in data:

                for key in data:

                    if key != 'redirection_address':
                        kwargs[key] = data[key]

                connection.kill()
                connection.connect(tuple(data['redirection_address']))
                connection.send_secret(json.dumps({'authentication': self._authentication}))
                connection.recv()
                connection.send(data=kwargs)
                msg, data, df = connection.recv()

            if msg:
                print(msg)

            if kwargs['request_type'] in ('authentication', 'cluster_info',
                                          'add_session', 'remove_session', 'list_sessions',
                                          'remove_database', 'shutdown'):
                return
            elif kwargs['request_type'] == 'header':
                return data
            elif kwargs['request_type'] == 'sync_databases':

                while msg != 'Done!':
                    msg, data, df = connection.recv()
                    print(msg)

                return
            elif kwargs['request_type'] in ('create_database', 'append_to_database'):
                connection.send_tables(kwargs['files'], data)
                msg, data, _ = connection.recv()
                print(msg)

                if kwargs['request_type'] == 'create_database':
                    return data

        finally:
            connection.kill()

        self.reload(data)

        if kwargs['request_type'] == 'query':

            if kwargs['output_file']:
                print(f"Writing '{kwargs['output_file']}' ...")
                df.to_csv(kwargs['output_file'])

            return df


class Client(object):
    _request = DatabaseClient._request

    def __init__(self, server_address):
        self.server_address = server_address
        self.database = None
        self._private_key = get_private_key()
        self._authentication = None
        self._request(request_type='authentication')

    def info(self):
        self._request(request_type='cluster_info')

    def load(self, database):
        self.database = DatabaseClient(self.server_address, database,
                                       self._private_key, self._authentication)

    def create(self, files, database, database_name, database_version,
               database_project=None):

        headers = self._request(request_type='create_database', files=files, path=database,
                                name=database_name, version=database_version, project=database_project)
        self.database = DatabaseClient(self.server_address, database,
                                       self._private_key, self._authentication, headers)

    def add_session(self, user, password, is_admin=False, create_allowed=False, databases=None):
        self._request(request_type='add_session', session_hash=get_hash(f'{user}:{password}'),
                      user=user, is_admin=is_admin, create_allowed=create_allowed, databases=databases)

    def remove_session(self, user):
        self._request(request_type='remove_session', user=user)

    def list_sessions(self):
        self._request(request_type='list_sessions')

    def remove_database(self, database):
        self._request(request_type='remove_database', path=database)

    def sync_databases(self, nodes=None, databases=None):
        self._request(request_type='sync_databases', nodes=nodes, databases=databases)

    def shutdown(self, node=None):
        self._request(request_type='shutdown', node=node)

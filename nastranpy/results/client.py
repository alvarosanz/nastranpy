import getpass
import json
import jwt
from nastranpy.results.database import DatabaseHeader
from nastranpy.results.connection import Connection, get_private_key
from nastranpy.results.results import get_query_from_file
from nastranpy.bdf.misc import get_hash


class BaseClient(object):

    def authenticate(self, connection):

        if self._authentication:
            connection.send_secret(json.dumps({'authentication': self._authentication.decode()}).encode())
        else:
            connection.send_secret(json.dumps({'user': input('user: '),
                                               'password': getpass.getpass('password: '),
                                               'request': 'authentication'}).encode())
            self._authentication = connection.recv_secret()

        connection.recv()

    def _request(self, **kwargs):
        connection = Connection(self.server_address, private_key=self._private_key)

        try:
            # Authentication
            self.authenticate(connection)

            # Sending request
            connection.send(msg=kwargs)
            data = connection.recv()

            # Redirecting request (if necessary)
            if 'redirection_address' in data:

                for key in data:

                    if key != 'redirection_address':
                        kwargs[key] = data[key]

                connection.kill()
                connection.connect(tuple(data['redirection_address']))
                self.authenticate(connection)
                connection.send(msg=kwargs)
                data = connection.recv()

            # Processing request
            if kwargs['request_type'] == 'sync_databases':

                while data['msg'] != 'Done!':
                    data = connection.recv()
                    print(data['msg'])

            elif kwargs['request_type'] in ('create_database', 'append_to_database'):
                connection.send_tables(kwargs['files'], data)
                data = connection.recv()
            elif kwargs['request_type'] == 'query':
                data['df'] = connection.recv_dataframe()

        finally:
            connection.kill()

        return data


class DatabaseClient(BaseClient):

    def __init__(self, server_address, path, private_key, authentication, header=None):
        self.server_address = server_address
        self.path = path
        self._private_key = private_key
        self._authentication = authentication

        if header:
            self.header = DatabaseHeader(header=header)
        else:
            self._request(request_type='header')

    def info(self, print_to_screen=True, detailed=False):
        print(f"Address: {self.server_address[0]} ({self.server_address[1]})")

        if self._headers:
            super().info(print_to_screen, detailed)

    def check(self):
        print(self._request(request_type='check')['msg'])

    def append(self, files, batch_name):

        if isinstance(files, str):
            files = [files]

        print(self._request(request_type='append_to_database', files=files, batch=batch_name)['msg'])

    def restore(self, batch_name):
        restore_points = [batch[0] for batch in self.header.batches]

        if batch_name not in restore_points or batch_name == restore_points[-1]:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        print(self._request(request_type='restore_database', batch=batch_name)['msg'])

    def query(self, table=None, outputs=None, LIDs=None, EIDs=None,
              geometry=None, weights=None, output_file=None, **kwargs):
        df = self._request(request_type='query', table=table, outputs=outputs,
                           LIDs=LIDs, EIDs=EIDs, geometry=geometry, weights=weights)['df']

        if output_file:
            print(f"Writing '{output_file}' ...")
            df.to_csv(output_file)

        return df

    def query_from_file(self, file):
        return self.query(**get_query_from_file(file))

    def _request(self, **kwargs):
        kwargs['path'] = self.path
        data = super()._request(**kwargs)
        self.header = DatabaseHeader(header=data['header'])
        return data


class Client(BaseClient):

    def __init__(self, server_address):
        self.server_address = server_address
        self.database = None
        self._private_key = get_private_key()
        self._authentication = None
        self._request(request_type='authentication')

    @property
    def session(self):
        return jwt.decode(self._authentication, verify=False)

    def info(self):
        print(self._request(request_type='cluster_info')['msg'])

    def load(self, database):
        self.database = DatabaseClient(self.server_address, database,
                                       self._private_key, self._authentication)

    def create_database(self, files, database, database_name, database_version,
                        database_project=None):

        if isinstance(files, str):
            files = [files]

        data = self._request(request_type='create_database', files=files, path=database,
                             name=database_name, version=database_version, project=database_project)
        print(data['msg'])
        self.database = DatabaseClient(self.server_address, database,
                                       self._private_key, self._authentication, data['header'])

    def remove_database(self, database):
        print(self._request(request_type='remove_database', path=database)['msg'])

    @property
    def databases(self):
        return list(self._request(request_type='list_databases'))

    @property
    def sessions(self):
        return self._request(request_type='list_sessions')['sessions']

    def add_session(self, user, password, is_admin=False, create_allowed=False, databases=None):
        print(self._request(request_type='add_session', session_hash=get_hash(f'{user}:{password}'),
                            user=user, is_admin=is_admin, create_allowed=create_allowed, databases=databases)['msg'])

    def remove_session(self, user):
        print(self._request(request_type='remove_session', user=user)['msg'])

    def sync_databases(self, nodes=None, databases=None):
        self._request(request_type='sync_databases', nodes=nodes, databases=databases)

    def shutdown(self, node=None):
        print(self._request(request_type='shutdown', node=node)['msg'])

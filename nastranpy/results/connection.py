import os
import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.fernet import Fernet
import socket
import json
import pandas as pd
from io import BytesIO
from nastranpy.bdf.misc import humansize
from nastranpy.results.read_results import tables_in_pch, ResultsTable


class Connection(object):

    def __init__(self, server_address=None, connection_socket=None,
                 private_key=None, header_size=12, buffer_size=4096):

        if server_address:
            self.connect(server_address)
        else:
            self.socket = connection_socket

        self.encryptor = None
        self.private_key = private_key
        self.header_size = header_size
        self.buffer_size = buffer_size
        self.pending_data = b''

    def connect(self, server_address):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(server_address)

    def kill(self):
        self.socket.close()
        self.encryptor = None

    def send(self, msg='', data=None, df=None):
        buffer = BytesIO()
        buffer.seek(3 * self.header_size + len(msg))

        if data is None:
            data = b''
        else:
            data = json.dumps(data).encode()
            buffer.write(data)

        if not df is None:
            df.to_msgpack(buffer)

        position = buffer.tell()
        buffer.seek(0)
        buffer.write((str(position).zfill(self.header_size) +
                      str(len(msg)).zfill(self.header_size) +
                      str(len(data)).zfill(self.header_size) +
                      msg).encode())
        self.socket.sendall(buffer.getbuffer())

    def recv(self):
        buffer = BytesIO()
        size = 1
        msg = None

        while buffer.tell() < size:

            if (self.pending_data and
                len(self.pending_data) > self.header_size and
                len(self.pending_data) == int(self.pending_data[:self.header_size].decode())):
                data = b''
            else:
                data = self.socket.recv(self.buffer_size)

            if size == 1:
                data = self.pending_data + data
                self.pending_data = b''
                msg_size = int(data[self.header_size : 2 * self.header_size].decode())
                data_size = int(data[2 * self.header_size : 3 * self.header_size].decode())
                size = int(data[:self.header_size].decode()) - 3 * self.header_size
                data = data[3 * self.header_size:]

            buffer.write(data)

        buffer.seek(size)
        self.pending_data = buffer.read()
        buffer.seek(size)
        buffer.truncate()
        buffer.seek(0)
        msg = buffer.read(msg_size).decode()

        if msg and msg[0] == '#':
            raise ConnectionError(msg[1:])

        if data_size:
            data = json.loads(buffer.read(data_size).decode())
        else:
            data = None

        if size > 3 * self.header_size + msg_size + data_size:
            df = pd.read_msgpack(buffer)
        else:
            df = None

        return msg, data, df

    def send_tables(self, files, tables_specs):
        ignored_tables = set()

        for file in files:

            for table in tables_in_pch(file, tables_specs):

                if table.name not in tables_specs:

                    if table.name not in ignored_tables:
                        print("WARNING: '{}' is not supported!".format(table.name))
                        ignored_tables.add(table.name)

                    continue

                df = table.df
                del table.__dict__['df']
                self.send(data=table.__dict__, df=df)

        self.send('END')

    def recv_tables(self):

        while True:
            msg, data, df = self.recv()

            if msg == 'END':
                break

            table = ResultsTable(**data)
            table.df = df
            yield table

    def send_file(self, file):
        self.socket.send(str(os.path.getsize(file)).zfill(self.header_size).encode())

        with open(file, 'rb') as f:
            sended = 1

            while sended:
                sended = self.socket.send(f.read(self.buffer_size))

    def recv_file(self, file):
        data = self.socket.recv(self.buffer_size)
        size = int(data[:self.header_size].decode())

        with open(file, 'wb') as f:
            f.write(data[self.header_size:])

            while f.tell() < size:
                f.write(self.socket.recv(self.buffer_size))

    def send_secret(self, secret):

        if not self.encryptor:
            self.send(self.private_key.public_key().public_bytes(encoding=serialization.Encoding.PEM,
                                                                 format=serialization.PublicFormat.SubjectPublicKeyInfo).decode())
            public_key_other = serialization.load_pem_public_key(self.recv()[0].encode(),
                                                                 backend=default_backend())
            self.encryptor = Fernet(self._get_key(public_key_other))

        self.send(self.encryptor.encrypt(secret.encode()).decode())

    def recv_secret(self):

        if not self.encryptor:
            public_key_other = serialization.load_pem_public_key(self.recv()[0].encode(),
                                                                 backend=default_backend())
            self.send(self.private_key.public_key().public_bytes(encoding=serialization.Encoding.PEM,
                                                                 format=serialization.PublicFormat.SubjectPublicKeyInfo).decode())
            self.encryptor = Fernet(self._get_key(public_key_other))

        return self.encryptor.decrypt(self.recv()[0].encode()).decode()

    def _get_key(self, public_key_other):
        shared_key = self.private_key.exchange(ec.ECDH(), public_key_other)
        return base64.urlsafe_b64encode(HKDF(algorithm=hashes.SHA256(),
                                             length=32,
                                             salt=None,
                                             info=b'handshake data',
                                             backend=default_backend()).derive(shared_key))


def get_private_key():
    return ec.generate_private_key(ec.SECP384R1(), default_backend())


def get_master_key():
    return Fernet.generate_key().decode()


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    port = s.getsockname()[1]
    s.close()
    return port

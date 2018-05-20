import os
import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.fernet import Fernet
import socket
import json
import pyarrow as pa
import numpy as np
from io import BytesIO
from nastranpy.bdf.misc import humansize
from nastranpy.results.read_results import tables_in_pch, ResultsTable


class Connection(object):

    def __init__(self, server_address=None, connection_socket=None,
                 private_key=None, header_size=15, buffer_size=4096):

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

    def send(self, bytes=None, msg=None, exception=None):

        if bytes:
            data_type = '0'
        elif msg:
            data_type = '1'
            bytes = json.dumps(msg).encode()
        elif exception:
            data_type = '#'
            bytes = exception.encode()

        self.socket.send((str(len(bytes)).zfill(self.header_size - 1) + data_type).encode())
        self.socket.sendall(bytes)

    def recv(self):
        buffer = BytesIO()
        size = 1
        msg = None

        while buffer.tell() < size:

            if (self.pending_data and
                len(self.pending_data) > self.header_size and
                (len(self.pending_data) - self.header_size) == int(self.pending_data[:self.header_size - 1].decode())):
                data = b''
            else:
                data = self.socket.recv(self.buffer_size)

            if size == 1:
                data = self.pending_data + data
                self.pending_data = b''
                size = int(data[:self.header_size - 1].decode())
                data_type = data[self.header_size - 1:self.header_size].decode()
                data = data[self.header_size:]

            buffer.write(data)

        buffer.seek(size)
        self.pending_data = buffer.read()
        buffer.seek(size)
        buffer.truncate()
        buffer.seek(0)

        if data_type == '0':
            return buffer
        elif data_type == '1':
            return json.loads(buffer.read().decode())
        elif data_type == '#':
            raise ConnectionError(buffer.read().decode())

    def send_batch(self, batch):
        writer = pa.RecordBatchStreamWriter(self.socket.makefile('wb'), batch.schema)
        writer.write_batch(batch)

    def recv_batch(self):
        reader = pa.RecordBatchStreamReader(self.socket.makefile('rb'))
        return reader.read_next_batch()

    def send_tables(self, files, tables_specs):
        ignored_tables = set()

        for file in files:

            for table in tables_in_pch(file, tables_specs):

                if table.name not in tables_specs:

                    if table.name not in ignored_tables:
                        print("WARNING: '{}' is not supported!".format(table.name))
                        ignored_tables.add(table.name)

                    continue

                f = BytesIO()
                data = np.save(f, table.data)
                table.data = None
                self.send(msg=table.__dict__)
                self.send(bytes=f.getbuffer())

        self.send(msg='END')

    def recv_tables(self):

        while True:
            data = self.recv()

            if data == 'END':
                break

            table = ResultsTable(**data)
            table.data = np.load(self.recv())
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
                                                                 format=serialization.PublicFormat.SubjectPublicKeyInfo))
            public_key_other = serialization.load_pem_public_key(self.recv().read(), backend=default_backend())
            self.encryptor = Fernet(self._get_key(public_key_other))

        self.send(self.encryptor.encrypt(secret))

    def recv_secret(self):

        if not self.encryptor:
            public_key_other = serialization.load_pem_public_key(self.recv().read(),
                                                                 backend=default_backend())
            self.send(self.private_key.public_key().public_bytes(encoding=serialization.Encoding.PEM,
                                                                 format=serialization.PublicFormat.SubjectPublicKeyInfo))
            self.encryptor = Fernet(self._get_key(public_key_other))

        return self.encryptor.decrypt(self.recv().read())

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
    return Fernet.generate_key()


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

import socket
import json
import pandas as pd
from io import BytesIO
from nastranpy.bdf.misc import humansize
from nastranpy.results.read_results import tables_in_pch, ResultsTable


class Connection(object):

    def __init__(self, server_address=None, connection_socket=None,
                 header_size=12, buffer_size=4096):

        if server_address:
            self.connect(server_address)
        else:
            self.socket = connection_socket

        self.header_size = header_size
        self.buffer_size = buffer_size
        self.pending_data = b''
        self.last_send = None

    def connect(self, server_address):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(server_address)

    def kill(self):
        self.socket.close()

    def send(self, msg='', data=None, df=None):
        self.last_send = {'msg': msg, 'data': data, 'df': df}
        msg = msg.strip()
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

            if 'redirection_address' in data:
                self.kill()
                self.connect(tuple(data['redirection_address']))
                self.send(**self.last_send)
                return self.recv()

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
                name = '{} - {}'.format(table.name, table.element_type)

                if name not in tables_specs:

                    if name not in ignored_tables:
                        print("WARNING: '{}' is not supported!".format(name))
                        ignored_tables.add(name)

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

    def send_files(self, files):
        nbytes = sum(os.path.getsize(file) for file in files)
        print(f"Transferring {len(files)} file/s ({humansize(nbytes)}) ...")
        self.socket.sendall(str(nbytes).encode())
        answer = self.socket.recv(self.buffer_size)

        for i, file in enumerate(files):
            print(f"Sending {os.path.basename(file)} ({i + 1} of {len(files)}) ...")

            with open(file, 'rb') as f:
                sended = 1

                while sended:
                    sended = self.socket.send(f.read(self.buffer_size))

    def recv_files(self, delimiter='\n'):
        size = int(self.socket.recv(self.buffer_size).decode())
        self.socket.send(b'proceed')
        received = 0
        buffer = ''

        while received < size:
            data = self.socket.recv(self.buffer_size).decode()
            received += len(data)
            buffer += data

            while buffer.find(delimiter) != -1:
                line, buffer = buffer.split(delimiter, 1)
                yield line


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

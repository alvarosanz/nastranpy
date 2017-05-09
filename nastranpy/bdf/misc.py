import os
import time
from functools import wraps


def timeit(func):

    @wraps(func)
    def wrapped(*args, **kw):
        ts = time.time()
        result = func(*args, **kw)
        te = time.time()
        print('{0:2.1f} sec'.format(te - ts))
        return result

    return wrapped


def assure_path_exists(path):
    dir = os.path.dirname(path)

    if dir and not os.path.exists(dir):
        os.makedirs(dir)


def get_singular(name):

    if name[-3:] == 'ies':
        return name[:-3] + 'y'
    elif name[-1] == 's':
        return name[:-1]
    else:
        return name


def get_plural(name):

    if name[-1] == 'y':
        return name[:-1] + 'ies'
    elif name[-1] == 's':
        return name + 'es'
    else:
        return name + 's'


def indent(lines, amount=4, ch=' '):
    padding = amount * ch
    return padding + ('\n' + padding).join(lines.split('\n'))


def get_id_info(ids, detailed=False):

    if ids:
        info = [len(ids), min(ids), max(ids)]
    else:
        info = [0, '', '']

    if detailed:
        free_slots = list()
        id_max = 99999999
        free_id0 = 1

        for used_id in sorted(ids):
            free_id1 = used_id - 1

            if free_id1 >= free_id0:
                free_slots.append((free_id0, free_id1))

            free_id0 = used_id + 1

        if free_id0 <= id_max:
            free_slots.append((free_id0, id_max))

        info.append(free_slots)

    return info


suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

def humansize(nbytes):
    if nbytes == 0:
        return '0 B'

    i = 0

    while nbytes >= 1024 and i < len(suffixes) - 1:
        nbytes /= 1024.
        i += 1

    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])


class CallCounted(object):

    def __init__(self, method):
        self.method = method
        self.counter = 0

    def __call__(self, *args, **kwargs):
        self.counter += 1
        return self.method(*args, **kwargs)

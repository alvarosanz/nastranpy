import time


def timeit(method):

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('{0:2.1f} sec'.format(te - ts))
        return result

    return timed

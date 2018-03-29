import numpy as np


class TableData(object):

    def __init__(self, fields, LIDs, EIDs, LID_name='LID', EID_name='EID'):
        self._LIDs = np.array(LIDs)
        self._EIDs = np.array(EIDs)
        self._LID_name = LID_name
        self._EID_name = EID_name

        try:
            self._fields = {field.name: field for field in fields}

            if not all(np.array_equal(field.LIDs, self._LIDs) and
                       np.array_equal(field.EIDs, self._EIDs) and
                       field._LID_name == self._LID_name and
                       field._EID_name == self._EID_name for
                       field in self._fields.values()):
                raise ValueError('Inconsistent fields!')

        except AttributeError:
            self._fields = list(fields)

    @property
    def names(self):
        return [name for name in self._fields]

    @property
    def index_labels(self):
        return (self._LID_name, self._EID_name)

    @property
    def LIDs(self):
        return np.array(self._LIDs)

    @property
    def EIDs(self):
        return np.array(self._EIDs)

    def __getitem__(self, key):
        return self._fields[key]

    def __contains__(self, value):
        return value in self._fields

    def close(self):

        for field in self._fields.values():
            field.close()

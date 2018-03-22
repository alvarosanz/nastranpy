import numpy as np
import pandas as pd
from nastranpy.results.queries import query_functions


class TableData(object):

    def __init__(self, fields, LIDs, EIDs, LID_name='LID', EID_name='EID'):
        self._names = [field.name for field in fields]
        self._LIDs = np.array(LIDs)
        self._EIDs = np.array(EIDs)
        self._LID_name = LID_name
        self._EID_name = EID_name
        self._fields = {field.name: field for field in fields}

        if not all(np.array_equal(field.LIDs, self._LIDs) and
                   np.array_equal(field.EIDs, self._EIDs) and
                   field._LID_name == self._LID_name and
                   field._EID_name == self._EID_name for
                   field in self._fields.values()):
            raise ValueError('Inconsistent fields!')

    @property
    def names(self):
        return [name for name in self._names]

    @property
    def index_labels(self):
        return tuple(self._names[:2])

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
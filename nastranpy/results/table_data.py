import numpy as np
import pandas as pd


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

    def __getitem__(self, key):
        return self._fields[key]

    def get_dataframe(self, LIDs=None, EIDs=None, columns=None):

        if LIDs is None:
            LIDs = self._LIDs

        if EIDs is None:
            EIDs = self._EIDs

        if columns is None:
            columns = self._names

        return pd.DataFrame({name: self._fields[name].get_array(LIDs, EIDs).ravel() for name in columns},
                            columns=columns, index=pd.MultiIndex.from_product([LIDs, EIDs],
                                                                              names=[self._LID_name,
                                                                                     self._EID_name,]))

    @property
    def names(self):
        return [name for name in self._names]

    @property
    def LIDs(self):
        return np.array(self._LIDs)

    @property
    def EIDs(self):
        return np.array(self._EIDs)

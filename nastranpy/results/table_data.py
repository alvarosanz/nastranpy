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

    def __getitem__(self, key):
        return self._fields[key]

    def get_dataframe(self, LIDs=None, EIDs=None, columns=None, fields_derived=None):

        if LIDs is None:
            LIDs = self._LIDs

        if EIDs is None:
            EIDs = self._EIDs

        if columns is None:
            columns = self._names

        data = {name: self._fields[name].get_array(LIDs, EIDs).ravel() for name in columns}

        if fields_derived:

            for field_name, field_args, field_func in fields_derived:

                if not callable(field_func):

                    if field_func.upper() in query_functions:
                        field_func = query_functions[field_func.upper()]
                    else:
                        raise ValueError('Unsupported field function: {}'.format(field_func))

                field_args = [field.upper() for field in field_args]
                field_args = [(field[4:-1], True) if field[:4] == 'ABS(' and field[-1] == ')' else
                              (field, False) for field in field_args]
                arrays = [self._fields[name].get_array(LIDs, EIDs, absolute_value=use_abs) for
                          name, use_abs in field_args]
                data[field_name] = field_func(*arrays).ravel()
                columns.append(field_name)

        return pd.DataFrame(data, columns=columns,
                            index=pd.MultiIndex.from_product([LIDs, EIDs], names=[self._LID_name,
                                                                                  self._EID_name,]))

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

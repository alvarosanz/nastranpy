import numpy as np
from nastranpy.results.field_data import FieldData


class TableData(object):

    def __init__(self, fields, LIDs, IDs):
        """
        Initialize a TableData instance.

        Parameters
        ----------
        fields : list of (str, np.dtype, str)
            List of tuples for each field. Each tuple contains the following info:
                (field name, field type, field file)

        LIDs : list of int
            List of LIDs.
        IDs : list of int
            List of IDs.
        """
        self._LIDs = LIDs
        self._IDs = IDs
        iLIDs = {LID: i for i, LID in enumerate(LIDs)}
        iIDs = {ID: i for i, ID in enumerate(IDs)}
        self._fields = {name: FieldData(name, dtype, file, LIDs, IDs, iLIDs, iIDs) for
                        name, dtype, file in fields}

    @property
    def LIDs(self):
        return np.array(self._LIDs)

    @property
    def IDs(self):
        return np.array(self._IDs)

    def __getitem__(self, key):
        return self._fields[key]

    def __contains__(self, value):
        return value in self._fields

    def close(self):
        """
        Close fields.
        """

        for field in self._fields.values():
            field.close()

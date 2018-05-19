import numpy as np


class TableData(object):

    def __init__(self, fields, LIDs, IDs):
        """
        Initialize a TableData instance.

        Parameters
        ----------
        fields: list of FieldData
            List of fields.
        LIDs: list of int
            List of LIDs.
        IDs: list of int
            List of IDs.
        """
        self._LIDs = LIDs
        self._IDs = IDs
        self._fields = {field.name: field for field in fields}

    @property
    def names(self):
        return [name for name in self._fields]

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

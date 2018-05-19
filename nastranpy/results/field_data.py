import os
import numpy as np
from numba import guvectorize


class FieldData(object):

    def __init__(self, name, dtype, file, LIDs, IDs):
        """
        Initialize a FieldData instance.

        Parameters
        ----------
        name: str
            Field name.
        dtype: numpy.dtype
            Field type.
        file: str
            File path.
        LIDs: list of int
            List of LIDs.
        IDs: list of int
            List of IDs.
        """
        self._name = name
        self._dtype = dtype
        self._file = file
        self._LIDs = LIDs
        self._IDs = IDs
        self._data_by_LID = None
        self._data_by_ID = None

    @property
    def name(self):
        return self._name

    @property
    def LIDs(self):
        return np.array(self._LIDs)

    @property
    def IDs(self):
        return np.array(self._IDs)

    @property
    def dtype(self):
        return self._dtype

    def close(self):
        """
        Close mapped files.
        """
        self._data_by_LID = None
        self._data_by_ID = None

    def read(self, LIDs=None, IDs=None, out=None):
        """
        Returns requested field values.

        Parameters
        ----------
        LIDs: list of int or dict, optional
            List of LIDs. If not provided or None, all LIDs are returned.
        IDs: list of int, optional
            List of IDs. If not provided or None, all IDs are returned.
        out: numpy.ndarray, optional
            A location into which the result is stored. If not provided or None, a freshly-allocated array is returned.

        Returns
        -------
        numpy.ndarray
            Field values requested.
        """

        if self._data_by_LID is None and self._data_by_ID  is None:
            self._n_LIDs = len(self._LIDs)
            self._n_IDs = len(self._IDs)
            self._iLIDs = {LID: i for i, LID in enumerate(self._LIDs)}
            self._iIDs = {ID: i for i, ID in enumerate(self._IDs)}
            self._offset = self._n_LIDs * self._n_IDs * np.dtype(self._dtype).itemsize

            if 2 * self._offset != os.path.getsize(self._file):
                raise ValueError("Inconsistency found! ('{}')".format(self._file))

        LIDs_queried = self._LIDs if LIDs is None else LIDs
        IDs_queried = self._IDs if IDs is None else IDs

        if out is None:
            out = np.empty((len(LIDs_queried), len(IDs_queried)), dtype=np.float64)

        array = out

        is_combination = isinstance(LIDs, dict)

        if is_combination:
            LIDs_queried = [LID for LID, seq in LIDs.items() if not seq]
            LIDs_requested = set(LIDs_queried)
            LIDs_queried += list({LID for seq in LIDs.values() for _, LID in seq if
                                  LID not in LIDs_requested and LID in self._iLIDs})
            LIDs_combined_used = list({LID for seq in LIDs.values() for _, LID in seq if
                                       LID not in self._iLIDs})
            LIDs_queried_index = {LID: i for i, LID in enumerate(LIDs_queried + LIDs_combined_used)}
            LID_combinations = [(LIDs_queried_index[LID] if LID in LIDs_queried_index else None,
                                 np.array([LIDs_queried_index[LID] for _, LID in seq], dtype=np.int64),
                                 np.array([coeff for coeff, _ in seq], dtype=np.float64)) for LID, seq in LIDs.items()]
            array = np.empty((len(LIDs_queried) + len(LIDs_combined_used), len(IDs_queried)), dtype=np.float64)

        if len(LIDs_queried) < len(IDs_queried):
            iIDs = slice(None) if IDs is None else np.array([self._iIDs[ID] for ID in IDs_queried])

            if self._data_by_LID is None:
                self._data_by_LID = np.memmap(self._file, dtype=self._dtype, shape=(self._n_LIDs, self._n_IDs), mode='r')

            for i, LID in enumerate(LIDs_queried):
                array[i, :] = self._data_by_LID[self._iLIDs[LID], :][iIDs]

        else:
            iLIDs = slice(None) if LIDs is None else np.array([self._iLIDs[LID] for LID in LIDs_queried])

            if self._data_by_ID is None:
                self._data_by_ID = np.memmap(self._file, dtype=self._dtype, shape=(self._n_IDs, self._n_LIDs), mode='r', offset=self._offset)

            for i, ID in enumerate(IDs_queried):
                array[:len(LIDs_queried), i] = self._data_by_ID[self._iIDs[ID], :][iLIDs].T

        if is_combination:

            for i, (index, indexes, coeffs) in enumerate(LID_combinations):

                if len(coeffs):
                    combine(array, indexes, coeffs, out[i, :])

                    if index:
                        array[index, :] = out[i, :]

                else:
                    out[i, :] = array[index, :]

        return out

@guvectorize(['(double[:, :], int64[:], double[:], double[:])'],
             '(n, m), (l), (l) -> (m)',
             target='cpu', nopython=True)
def combine(array, indexes, coeffs, out):
    """
    Combine load cases.

    Parameters
    ----------
    array: numpy.ndarray
        Field values (not combined).
    indexes: numpy.ndarray
        Indexes of LIDs to combine.
    coeffs: numpy.ndarray
        Multiplication coefficients.
    out: numpy.ndarray
        Output argument. Combined field values.
    """

    for i in range(array.shape[1]):
        aux = 0.0

        for j in range(indexes.shape[0]):
            aux += array[indexes[j], i] * coeffs[j]

        out[i] = aux

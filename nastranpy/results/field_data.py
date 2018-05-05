import os
import numpy as np
from numba import guvectorize


class FieldData(object):

    def __init__(self, name, file, dtype, LIDs, EIDs, LID_name='LID', EID_name='EID'):
        self._name = name
        self._file = file
        self._dtype = dtype
        self._LIDs = LIDs
        self._EIDs = EIDs
        self._LID_name = LID_name
        self._EID_name = EID_name
        self._data_by_LID = None
        self._data_by_EID = None

    @property
    def name(self):
        return self._name

    @property
    def index_labels(self):
        return (self._LID_name, self._EID_name)

    @property
    def LIDs(self):
        return np.array(self._LIDs)

    @property
    def EIDs(self):
        return np.array(self._EIDs)

    @property
    def dtype(self):
        return self._dtype

    def close(self):
        self._data_by_LID = None
        self._data_by_EID = None

    def read(self, LIDs=None, EIDs=None, out=None):

        if self._data_by_LID is None and self._data_by_EID  is None:
            self._n_LIDs = len(self._LIDs)
            self._n_EIDs = len(self._EIDs)
            self._iLIDs = {LID: i for i, LID in enumerate(self._LIDs)}
            self._iEIDs = {EID: i for i, EID in enumerate(self._EIDs)}
            self._offset = self._n_LIDs * self._n_EIDs * np.dtype(self._dtype).itemsize

            if 2 * self._offset != os.path.getsize(self._file):
                raise ValueError("Inconsistency found! ('{}')".format(self._file))

        LIDs_queried = self._LIDs if LIDs is None else LIDs
        EIDs_queried = self._EIDs if EIDs is None else EIDs

        if out is None:
            out = np.empty((len(LIDs_queried), len(EIDs_queried)), dtype=np.float64)

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
            array = np.empty((len(LIDs_queried) + len(LIDs_combined_used), len(EIDs_queried)), dtype=np.float64)

        if len(LIDs_queried) < len(EIDs_queried):
            iEIDs = slice(None) if EIDs is None else np.array([self._iEIDs[EID] for EID in EIDs_queried])

            if self._data_by_LID is None:
                self._data_by_LID = np.memmap(self._file, dtype=self._dtype, shape=(self._n_LIDs, self._n_EIDs), mode='r')

            for i, LID in enumerate(LIDs_queried):
                array[i, :] = self._data_by_LID[self._iLIDs[LID], :][iEIDs]

        else:
            iLIDs = slice(None) if LIDs is None else np.array([self._iLIDs[LID] for LID in LIDs_queried])

            if self._data_by_EID is None:
                self._data_by_EID = np.memmap(self._file, dtype=self._dtype, shape=(self._n_EIDs, self._n_LIDs), mode='r', offset=self._offset)

            for i, EID in enumerate(EIDs_queried):
                array[:len(LIDs_queried), i] = self._data_by_EID[self._iEIDs[EID], :][iLIDs].T

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

    for i in range(array.shape[1]):
        aux = 0.0

        for j in range(indexes.shape[0]):
            aux += array[indexes[j], i] * coeffs[j]

        out[i] = aux

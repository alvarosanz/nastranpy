import numpy as np
import pandas as pd


class FieldData(object):

    def __init__(self, name, data_by_LID, data_by_EID, LIDs, EIDs,
                 LID_name='LID', EID_name='EID'):
        self._name = name
        self._data_by_LID = data_by_LID
        self._data_by_EID = data_by_EID
        self._LIDs = np.array(LIDs)
        self._EIDs = np.array(EIDs)
        self._LID_name = LID_name
        self._EID_name = EID_name
        self._iLIDs = {LID: i for i, LID in enumerate(self._LIDs)}
        self._iEIDs = {EID: i for i, EID in enumerate(self._EIDs)}
        self._n_LIDs = len(self._LIDs)
        self._n_EIDs = len(self._EIDs)

    def get_array(self, LIDs=None, EIDs=None, LID_combinations=None):

        if LID_combinations:

            if LIDs is None:
                LIDs_requested = set()
                LIDs = np.array(sorted({LID for seq in LID_combinations for _, LID in seq}))
            else:
                LIDs_requested = set(LIDs)
                LIDs = np.array(list(LIDs) + sorted({LID for seq in LID_combinations for
                                                     _, LID in seq if LID not in LIDs_requested}))

            iLIDs = {LID: i for i, LID in enumerate(LIDs)}
            LID_combinations = [(np.array([iLIDs[LID] for _, LID in seq]),
                                 np.array([coeff for coeff, _ in seq])) for seq in
                                LID_combinations]

        if LIDs is None and EIDs is None:
            array = np.array(self._data_by_LID)
        elif EIDs is None:
            iLIDs = np.array(sorted(self._iLIDs[LID] for LID in LIDs))

            if len(LIDs) > self._n_EIDs:
                array = self._data_by_LID[iLIDs, :]
            else:
                array = self._data_by_EID[:, iLIDs].T

        elif LIDs is None:
            iEIDs = np.array(sorted(self._iEIDs[EID] for EID in EIDs))

            if self._n_LIDs > len(EIDs):
                array = self._data_by_LID[:, iEIDs]
            else:
                array = self._data_by_EID[iEIDs, :].T

        else:
            iLIDs = np.array(sorted(self._iLIDs[LID] for LID in LIDs))
            iEIDs = np.array(sorted(self._iEIDs[EID] for EID in EIDs))

            if len(LIDs) > len(EIDs):
                array = self._data_by_LID[iLIDs, :][:, iEIDs]
            else:
                array = self._data_by_EID[iEIDs, :][:, iLIDs].T

        if LID_combinations:
            array1 = np.empty((len(LIDs_requested) + len(LID_combinations), array.shape[1]),
                             dtype=array.dtype)

            if LIDs_requested:
                array1[:len(LIDs_requested), :] = array[:len(LIDs_requested), :]

            array = array.T

            for i, (index, coeffs) in enumerate(LID_combinations):
                array1[len(LIDs_requested) + i, :] = np.dot(array[:, index], coeffs)

            return array1
        else:
            return array

    def get_series(self, LIDs=None, EIDs=None):
        array = self.get_array(LIDs, EIDs)

        if LIDs is None:
            LIDs = self._LIDs

        if EIDs is None:
            EIDs = self._EIDs

        return pd.Series(array.ravel(), index=pd.MultiIndex.from_product([LIDs, EIDs],
                                                                         names=[self._LID_name,
                                                                                self._EID_name,]),
                         name = self._name)

    @property
    def name(self):
        return self._name

    @property
    def LIDs(self):
        return self._LIDs

    @property
    def EIDs(self):
        return self._EIDs

    @property
    def dtype(self):
        return self._data_by_LID.dtype

    @property
    def shape(self):
        return self._data_by_LID.shape

    @property
    def size(self):
        return self._data_by_LID.size

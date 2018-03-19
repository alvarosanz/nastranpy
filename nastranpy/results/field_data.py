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
        self._dtype = self._data_by_LID.dtype
        self._item_size = np.dtype(self._dtype).itemsize

    def get_array(self, LIDs=None, EIDs=None, LID_combinations=None,
                  return_indexes=False, absolute_value=False, max_size=2e9):
        LIDs_combined_used = list()

        if LID_combinations:

            if LIDs is None:
                LIDs = list()

            LIDs_requested = set(LIDs)
            LIDs = list(LIDs) + list({LID for seq in LID_combinations.values() for _, LID in seq if
                                      LID not in LIDs_requested and LID in self._iLIDs})
            LIDs_combined_used = list({LID for seq in LID_combinations.values() for _, LID in seq if
                                       LID in LID_combinations})
            iLIDs_combined = {LID: i for i, LID in enumerate(LIDs + LIDs_combined_used)}

        n_LIDs = self._n_LIDs if LIDs is None else len(LIDs)
        n_EIDs = self._n_EIDs if EIDs is None else len(EIDs)
        memory_used_by_LID = n_LIDs * self._n_EIDs * self._item_size
        memory_used_by_EID = n_EIDs * self._n_LIDs * self._item_size
        by_LID = ((n_LIDs <= n_EIDs and (memory_used_by_LID < max_size or
                                         memory_used_by_LID < memory_used_by_EID)) or
                  (n_LIDs > n_EIDs and (memory_used_by_LID < max_size and
                                        memory_used_by_EID > max_size)))

        array = np.empty((n_LIDs + len(LIDs_combined_used), n_EIDs), dtype=self._dtype)

        if LIDs is None and EIDs is None:
            array[:n_LIDs, :] = self._data_by_LID
        elif EIDs is None:
            iLIDs = np.array(sorted(self._iLIDs[LID] for LID in LIDs))

            if by_LID:
                array[:n_LIDs, :] = self._data_by_LID[iLIDs, :]
            else:
                array[:n_LIDs, :] = np.array(self._data_by_EID)[:, iLIDs].T

        elif LIDs is None:
            iEIDs = np.array(sorted(self._iEIDs[EID] for EID in EIDs))

            if by_LID:
                array[:n_LIDs, :] = np.array(self._data_by_LID)[:, iEIDs]
            else:
                array[:n_LIDs, :] = self._data_by_EID[iEIDs, :].T

        else:
            iLIDs = np.array(sorted(self._iLIDs[LID] for LID in LIDs))
            iEIDs = np.array(sorted(self._iEIDs[EID] for EID in EIDs))

            if by_LID:
                array[:n_LIDs, :] = self._data_by_LID[iLIDs, :][:, iEIDs]
            else:
                array[:n_LIDs, :] = self._data_by_EID[iEIDs, :][:, iLIDs].T

        if LID_combinations:
            array_combined = np.empty((len(LIDs_requested) + len(LID_combinations), n_EIDs),
                                      dtype=self._dtype)

            if LIDs_requested:
                array_combined[:len(LIDs_requested), :] = array[:len(LIDs_requested), :]

            for i, (LID_combined, seq) in enumerate(LID_combinations.items()):
                index = np.array([iLIDs_combined[LID] for _, LID in seq])
                coeffs = np.array([coeff for coeff, _ in seq])
                LID_array = np.dot(array[index, :].T, coeffs)

                if LID_combined in iLIDs_combined:
                    array[iLIDs_combined[LID_combined], :] = LID_array

                array_combined[len(LIDs_requested) + i, :] = LID_array

            array = array_combined

        if absolute_value:
            array = np.abs(array)

        if return_indexes:

            if LIDs is None:
                LIDs = np.array(self._LIDs)
            else:
                LIDs = np.array(LIDs)

            if EIDs is None:
                EIDs = np.array(self._EIDs)
            else:
                EIDs = np.array(EIDs)

            return array, LIDs, EIDs
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
        return np.array(self._LIDs)

    @property
    def EIDs(self):
        return np.array(self._EIDs)

    @property
    def dtype(self):
        return self._dtype

    @property
    def shape(self):
        return self._data_by_LID.shape

    @property
    def size(self):
        return self._data_by_LID.size

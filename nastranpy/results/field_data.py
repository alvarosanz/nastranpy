import numpy as np


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

    @property
    def shape(self):
        return self._data_by_LID.shape

    @property
    def size(self):
        return self._data_by_LID.size

    def close(self):
        self._data_by_LID = None
        self._data_by_EID = None

    def get_array(self, LIDs=None, EIDs=None, max_size=2e9):
        is_combination = isinstance(LIDs, dict)

        if is_combination:
            LIDs_queried = [LID for LID, seq in LIDs.items() if not seq]
            LIDs_requested = set(LIDs_queried)
            LIDs_queried += list({LID for seq in LIDs.values() for _, LID in seq if
                                  LID not in LIDs_requested and LID in self._iLIDs})
            LIDs_combined_used = list({LID for seq in LIDs.values() for _, LID in seq if
                                       LID not in self._iLIDs})
            LIDs_index = {LID: i for i, LID in enumerate(LIDs)}
            LIDs_queried_index = {LID: i for i, LID in enumerate(LIDs_queried + LIDs_combined_used)}

        else:
            LIDs_queried = self._LIDs if LIDs is None else LIDs
            LIDs_combined_used = list()

        n_LIDs = len(LIDs_queried)
        n_EIDs = self._n_EIDs if EIDs is None else len(EIDs)
        memory_used_by_LID = n_LIDs * self._n_EIDs * self._item_size
        memory_used_by_EID = n_EIDs * self._n_LIDs * self._item_size
        by_LID = (n_LIDs <= n_EIDs and memory_used_by_LID < max_size or
                  n_LIDs > n_EIDs and memory_used_by_EID > max_size)

        if by_LID and memory_used_by_LID > max_size or not by_LID and memory_used_by_EID > max_size:
            raise MemoryError('The requested data is too large to fit into memory!')

        array = np.empty((n_LIDs + len(LIDs_combined_used), n_EIDs), dtype=self._dtype)

        if LIDs is None and EIDs is None:
            LIDs = np.array(self._LIDs)
            EIDs = np.array(self._EIDs)
            array[:n_LIDs, :] = self._data_by_LID
        elif EIDs is None:
            EIDs = np.array(self._EIDs)
            iLIDs = np.array(sorted(self._iLIDs[LID] for LID in LIDs_queried))

            if by_LID:
                array[:n_LIDs, :] = self._data_by_LID[iLIDs, :]
            else:
                array[:n_LIDs, :] = np.array(self._data_by_EID)[:, iLIDs].T

        elif LIDs is None:
            LIDs = np.array(self._LIDs)
            iEIDs = np.array(sorted(self._iEIDs[EID] for EID in EIDs))

            if by_LID:
                array[:n_LIDs, :] = np.array(self._data_by_LID)[:, iEIDs]
            else:
                array[:n_LIDs, :] = self._data_by_EID[iEIDs, :].T

        else:
            iLIDs = np.array(sorted(self._iLIDs[LID] for LID in LIDs_queried))
            iEIDs = np.array(sorted(self._iEIDs[EID] for EID in EIDs))

            if by_LID:
                array[:n_LIDs, :] = self._data_by_LID[iLIDs, :][:, iEIDs]
            else:
                array[:n_LIDs, :] = self._data_by_EID[iEIDs, :][:, iLIDs].T

        if is_combination:
            array0 = array
            array = np.empty((len(LIDs), n_EIDs), dtype=self._dtype)

            for LID, seq in LIDs.items():

                if seq:
                    row = np.dot(array0[np.array([LIDs_queried_index[LID] for _, LID in seq]), :].T,
                                 np.array([coeff for coeff, _ in seq]))

                    if LID in LIDs_queried_index:
                        array0[LIDs_queried_index[LID], :] = row

                    array[LIDs_index[LID], :] = row
                else:
                    array[LIDs_index[LID], :] = array0[LIDs_queried_index[LID], :]

        return array, LIDs, EIDs

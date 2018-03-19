import os
import json
import numpy as np
from nastranpy.results.field_data import FieldData
from nastranpy.results.table_data import TableData
from nastranpy.results.queries import query_functions
from nastranpy.bdf.misc import get_plural, humansize, indent


class DataBase(object):

    def __init__(self, path, model=None):
        self.path = path
        self.model = model
        self.clear()
        self.load()

    def clear(self):
        self.tables = None
        self._nbytes = 0
        self._header = None
        self._project = None
        self._name = None
        self._version = None
        self._date = None

    @property
    def project(self):
        return self._project

    @property
    def name(self):
        return self._name

    @property
    def version(self):
        return self._version

    @property
    def date(self):
        return self._date

    def _walk_header(self):

        if self._header is None:
            self._header = list()

            with open(os.path.join(self.path, '#header.json')) as f:
                database_header = json.load(f)

            for table_name in database_header['tables']:
                table_path = os.path.join(self.path, table_name)

                with open(os.path.join(table_path, '#header.json')) as f:
                    table_header = json.load(f)

                self._header.append((table_name, table_path, table_header))

        yield from self._header

    def load(self):

        if self.tables is None:

            with open(os.path.join(self.path, '#header.json')) as f:
                database_header = json.load(f)
                self._project = database_header['project']
                self._name = database_header['name']
                self._version = database_header['version']
                self._date = database_header['date']

            self.tables = dict()

            for table_name, table_path, table_header in self._walk_header():
                LID_name, LID_dtype = table_header['columns'][0]
                EID_name, EID_dtype = table_header['columns'][1]
                n_LIDs = table_header[get_plural(LID_name)]
                n_EIDs = table_header[get_plural(EID_name)]
                LIDs_file = os.path.join(table_path, LID_name + '.bin')
                EIDs_file = os.path.join(table_path, EID_name + '.bin')
                self._nbytes += os.path.getsize(LIDs_file)
                self._nbytes += os.path.getsize(EIDs_file)

                if (n_LIDs * np.dtype(LID_dtype).itemsize != os.path.getsize(LIDs_file) or
                    n_EIDs * np.dtype(EID_dtype).itemsize != os.path.getsize(EIDs_file)):
                    raise ValueError("Inconsistency found! ('{}')".format(table_name))

                LIDs = np.fromfile(LIDs_file, dtype=LID_dtype)
                EIDs = np.fromfile(EIDs_file, dtype=EID_dtype)
                fields = list()

                for field_name, dtype in table_header['columns'][2:]:
                    file_by_LID = os.path.join(table_path, field_name + '.bin')
                    file_by_EID = os.path.join(table_path, field_name + '#T.bin')
                    self._nbytes += os.path.getsize(file_by_LID)
                    self._nbytes += os.path.getsize(file_by_EID)

                    if ((n_LIDs * n_EIDs) * np.dtype(dtype).itemsize != os.path.getsize(file_by_LID) or
                        (n_LIDs * n_EIDs) * np.dtype(dtype).itemsize != os.path.getsize(file_by_EID)):
                        raise ValueError("Inconsistency found! ('{}')".format(table_name))

                    array_by_LID = np.memmap(file_by_LID, dtype=dtype, shape=(n_LIDs, n_EIDs), mode='r')
                    array_by_EID = np.memmap(file_by_EID, dtype=dtype, shape=(n_EIDs, n_LIDs), mode='r')
                    fields.append(FieldData(field_name, array_by_LID, array_by_EID, LIDs, EIDs,
                                            LID_name=table_header['columns'][0][0],
                                            EID_name=table_header['columns'][1][0]))

                self.tables[table_name] = TableData(fields, LIDs, EIDs,
                                                     LID_name=table_header['columns'][0][0],
                                                     EID_name=table_header['columns'][1][0])

        else:
            print('Database already loaded!')

    def query(self, table_name, fields, aggregation_options=None, return_stats=False,
              LIDs=None, EIDs=None, LID_combinations=None,
              geometry=None, weights=None):
        is_singular_instance = False

        if isinstance(fields, str):
            fields = [fields]
            is_singular_instance = True

        fields = [field.upper() for field in fields]
        fields = [(field[4:-1], True) if field[:4] == 'ABS(' and field[-1] == ')' else
                  (field, False) for field in fields]

        arrays = list()

        for field, use_abs in fields:
            array, LIDs_returned, EIDs_returned = self.tables[table_name][field].get_array(LIDs, EIDs, LID_combinations,
                                                                                           return_indexes=True,
                                                                                           absolute_value=use_abs)
            arrays.append(array)

        if aggregation_options is None:

            if is_singular_instance:
                return arrays[0]
            else:
                return arrays

        else:
            aggregations = list()

            is_singular_instance = False

            if not isinstance(aggregation_options, list):
                aggregation_options = [aggregation_options]
                is_singular_instance = True

            for aggregation_option in aggregation_options:
                stats = None
                aggregation_sequence = None

                if not isinstance(aggregation_option, str):
                    func, aggregation_sequence = aggregation_option
                else:
                    func = aggregation_option

                if not callable(func):

                    if func.upper() in query_functions:
                        func = query_functions[func.upper()]
                    else:
                        raise ValueError('Unsupported aggregation function: {}'.format(func))

                args = [array for array in arrays]

                if not geometry is None:
                    args.append(geometry)

                array = func(*args)

                if aggregation_sequence:

                    if not isinstance(aggregation_sequence, list):
                        aggregation_sequence = [aggregation_sequence]

                    for method, axis in aggregation_sequence:
                        last_axis = axis[:3].upper()

                        if len(array.shape) == 2:

                            if axis.upper() not in ('LIDS', 'EIDS', 'GIDS', 'LID', 'EID', 'GID'):
                                raise ValueError(f"Illegal axis ('{axis}')! Use 'LIDs', 'EIDs' or 'GIDs' instead.")

                            axis = 0 if last_axis == 'LID' else 1

                        else:
                            axis = 0

                        if method.upper() == 'AVG':

                            if return_stats:
                                stats = None

                            array = np.average(array, axis, weights)
                        elif method.upper() == 'MAX':

                            if return_stats:
                                stats = array.argmax(axis)

                            array = array.max(axis)
                        elif method.upper() == 'MIN':

                            if return_stats:
                                stats = array.argmin(axis)

                            array = array.min(axis)
                        else:
                            raise ValueError('Unsupported aggregation method: {}'.format(method))

                if return_stats:

                    if not stats is None:

                        if last_axis == 'LID':
                            IDs = LIDs_returned
                        else:
                            IDs = EIDs_returned

                        stats = IDs[stats]

                    aggregations.append((array, stats))
                else:
                    aggregations.append(array)

            if is_singular_instance:
                return aggregations[0]
            else:
                return aggregations

    def get_dataframe(self, table_name, LIDs=None, EIDs=None, columns=None):
        return self.tables[table_name].get_dataframe(LIDs, EIDs, columns)

    def info(self, print_to_screen=True):
        info = list()
        info.append(f'Project: {self.project}')
        info.append(f'Name: {self.name}')
        info.append(f'Version: {self.version}')
        info.append(f'Date: {self.date}')
        info.append(f'Total size: {humansize(self._nbytes)}'.format())
        info.append(f'Number of tables: {len(self.tables)}'.format())
        info.append('')

        for table_name, table_path, table_header in self._walk_header():
            LIDs_name = get_plural(table_header['columns'][0][0])
            EIDs_name = get_plural(table_header['columns'][1][0])
            info.append(f"Table name: '{table_name}' ({LIDs_name}: {table_header[LIDs_name]}, {EIDs_name}: {table_header[EIDs_name]})")
            info.append('   ' + ' '.join(['_' * 6 for _, _ in table_header['columns']]))
            info.append('  |' + '|'.join([' ' * 6 for _, _ in table_header['columns']]) + '|')
            info.append('  |' + '|'.join([name.center(6) for name, _ in table_header['columns']]) + '|')
            info.append('  |' + '|'.join(['_' * 6 for _, _ in table_header['columns']]) + '|')
            info.append('  |' + '|'.join([' ' * 6 for _, _ in table_header['columns']]) + '|')
            info.append('  |' + '|'.join([dtype[1:].center(6) for _, dtype in table_header['columns']]) + '|')
            info.append('  |' + '|'.join(['_' * 6 for _, _ in table_header['columns']]) + '|')
            info.append('')

        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info

import os
import json
import re
import csv
import numpy as np
import pandas as pd
from nastranpy.results.field_data import FieldData
from nastranpy.results.table_data import TableData
from nastranpy.results.queries import query_functions
from nastranpy.bdf.misc import get_plural, humansize, indent, get_hasher, hash_bytestr


class DataBase(object):

    def __init__(self, path, model=None):
        self.path = path
        self.model = model
        self.clear()
        self.load()

    def clear(self):
        self.tables = None
        self._nbytes = 0
        self._table_headers = None
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

        if self._table_headers is None:
            self._table_headers = list()

            with open(os.path.join(self.path, '#header.json')) as f:
                database_header = json.load(f)

            for table_name in database_header['tables']:
                table_path = os.path.join(self.path, table_name)

                with open(os.path.join(table_path, '#header.json')) as f:
                    table_header = json.load(f)

                self._table_headers.append((table_name, table_path, table_header))

        yield from self._table_headers

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

    def check(self):
        print('Checking data integrity ...')
        files_corrupted = list()

        for table_name, table_path, table_header in self._walk_header():

            files = [field for field, _ in table_header['columns']]
            files += [field + '#T' for field, _ in table_header['columns'][2:]]
            files = [os.path.join(table_path, field + '.bin') for field in files]
            files.append(os.path.join(table_path, '#header.json'))

            for file in files:

                with open(file, 'rb') as f, open(os.path.splitext(file)[0] + '.' + table_header['checksum'], 'rb') as f_checksum:

                    if f_checksum.read() != hash_bytestr(f, get_hasher(table_header['checksum'])):
                        files_corrupted.append(file)

        if files_corrupted:

            for file in files_corrupted:
                print(f"'{file}' is corrupted!")
        else:
            print('Everything is OK!')

    def query(self, table=None, outputs=None, LIDs=None, EIDs=None,
              geometry=None, weights=None, file=None, custom_functions=None, **kwargs):

        if file:
            return self.query(**get_query_from_file(file))
        elif not table and not fields:
            raise ValueError('You must specify a query!')

        EID_groups = None

        if isinstance(EIDs, dict):
            EID_groups = EIDs
            EIDs = sorted({EID for EIDs in EIDs.values() for EID in EIDs})
            iEIDs = {EID: i for i, EID in enumerate(EIDs)}

        if geometry:
            geometry = {parameter: np.array([geometry[parameter][EID] for EID in EIDs]) for
                        parameter in geometry}

        query = dict()
        fields = dict()
        all_aggregations = list()

        if not outputs:
            outputs = self.tables[table].names

        for output in outputs:

            if isinstance(output, str):
                output_field, is_absolute = self._is_abs(output.upper())
                aggregations = None
            else:
                output_field, is_absolute = self._is_abs(output[0].upper())
                aggregations = output[1].upper()
                all_aggregations.append(aggregations)

            if output_field in self.tables[table]:
                field_array, LIDs, EIDs = self.tables[table][output_field].get_array(LIDs, EIDs)
                fields[output_field] = field_array
                output_array = np.array(fields[output_field])
            else:
                func_name, func_args = self._get_args(output_field)

                if custom_functions and func_name in custom_functions:
                    func = custom_functions[func_name]
                elif func_name in query_functions[table]:
                    func, func_args = query_functions[table][func_name]
                else:
                    raise ValueError(f"Unsupported output: '{output_field}'")

                output_field = func_name
                args = list()

                for arg in func_args:

                    if arg not in fields:

                        if geometry and arg in geometry:
                            fields[arg] = geometry[arg]
                        elif arg in self.tables[table]:
                            field_array, LIDs, EIDs = self.tables[table][arg].get_array(LIDs, EIDs)
                            fields[arg] = field_array
                        else:
                            continue

                    args.append(fields[arg])

                output_array = func(*args)

            if is_absolute:
                output_array = np.abs(output_array)
                output_field = f'ABS({output_field})'

            if EID_groups:

                if not aggregations:
                    raise ValueError('A grouped query must be aggregated!')

                index0 = list(EID_groups)

                if len(aggregations.split('-')) == 2:
                    n = 1
                    index1 = None
                else:
                    n = len(LIDs)
                    index1 = np.array(LIDs)

                array_agg = np.empty((len(EID_groups), n), dtype=output_array.dtype)
                LIDs_agg = np.empty((len(EID_groups), n), dtype=self.tables[table].LIDs.dtype)

                for i, EID_group in enumerate(EID_groups):
                    EIDs = np.array(EID_groups[EID_group])
                    weights = np.array([weights[EID] for EID in EIDs]) if weights else None
                    array_agg[i, :], LIDs_agg[i, :] = self._aggregate(output_array[:, np.array([iEIDs[EID] for EID in EIDs])],
                                                                      aggregations, LIDs, weights)

                query[f'{output_field} ({aggregations})'] = array_agg

                if index1 is None:
                    query['{} (LID {})'.format(output_field, aggregations.split('-')[1])] = LIDs_agg
            else:

                if aggregations:
                    raise ValueError('A pick query must not be aggregated!')

                query[output_field] = output_array
                index0 = np.array(LIDs)
                index1 = np.array(EIDs)

        if len({0 if aggregations is None else len(aggregations.split('-')) for
                aggregations in all_aggregations}) > 1:
            raise ValueError("All aggregations must be one-level (i.e. 'AVG') or two-level (i. e. 'AVG-MAX')")

        data = {field: array.ravel() for field, array in query.items()}
        columns = list(data)

        if EID_groups:
            index_names = ['Group', self.tables[table]._LID_name]
        else:
            index_names = [self.tables[table]._LID_name, self.tables[table]._EID_name]

        if index1 is None:
            index = pd.Index(index0, name='Group')
        else:
            index = pd.MultiIndex.from_product([index0, index1], names=index_names)

        return pd.DataFrame(data, columns=columns, index=index)

    @staticmethod
    def _is_abs(field_str):

        if field_str[:4] == 'ABS(' and field_str[-1] == ')':
            return field_str[4:-1], True
        else:
            return field_str, False

    @staticmethod
    def _get_args(func_str):
        re_match = re.search('(.+)\((.+?)\)', func_str)

        if re_match:
            return re_match[1], [arg.strip() for arg in re_match[2].split(',')]
        else:
            return func_str, None

    @classmethod
    def _aggregate(cls, output_array, aggregations, LIDs, weights):

        for i, aggregation in enumerate(aggregations.strip().split('-')):
            axis = 1 - i
            aggregation, is_absolute = cls._is_abs(aggregation)

            if aggregation == 'AVG':

                if axis == 0:
                    raise ValueError("'AVG' aggregation cannot be applied to LIDs!")

                output_array = np.average(output_array, axis, weights)
            elif aggregation == 'MAX':

                if axis == 0:
                    LIDs = np.array([LIDs[output_array.argmax(axis)]])
                    output_array = np.array([np.max(output_array, axis)])
                else:
                    output_array = np.max(output_array, axis)
            elif aggregation == 'MIN':

                if axis == 0:
                    LIDs = np.array([LIDs[output_array.argmin(axis)]])
                    output_array = np.array([np.min(output_array, axis)])
                else:
                    output_array = np.min(output_array, axis)
            else:
                raise ValueError(f"Unsupported aggregation method: '{aggregation}'")

            if is_absolute:
                output_array = np.abs(output_array)

        return output_array, LIDs


def get_query_from_file(file):

    try:

        with open(file) as f:
            query = json.load(f)

    except TypeError:
        query = json.load(file)

    query = {key: value if value else None for key, value in query.items()}

    for field in ('LIDs', 'geometry', 'weights'):

        try:

            if query[field]:
                query[field] = {int(key): value for key, value in query[field].items()}

        except AttributeError:
            pass

    return query

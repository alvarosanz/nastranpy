import os
import json
import re
import csv
import shutil
import numpy as np
import pandas as pd
from nastranpy.results.field_data import FieldData
from nastranpy.results.table_data import TableData
from nastranpy.results.queries import query_functions
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.results.database_creation import create_tables, finalize_database, open_table
from nastranpy.bdf.misc import humansize, indent, get_hasher, hash_bytestr


def is_loaded(func):

    def wrapped(self, *args, **kwargs):

        if self._headers is None:
            print('You must load a database first!')
        else:
            return func(self, *args, **kwargs)

    return wrapped


class ParentDatabase(object):

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
    def restore_points(self):
        return [batch_name for batch_name, _, _ in self._batches]

    @is_loaded
    def info(self, print_to_screen=True, detailed=False):
        info = list()
        info.append(f'Path: {self.path}')
        info.append('')

        if self.project:
            info.append(f'Project: {self.project}')

        info.append(f'Name: {self.name}')
        info.append(f'Version: {self.version}')
        info.append(f'Size: {humansize(self._nbytes)}'.format())
        info.append('')

        for header in self._headers.values():
            ncols = len(header['columns'])
            info.append(f"Table name: '{header['name']}' ({header['columns'][0][0]}: {len(header['LIDs'])}, {header['columns'][1][0]}: {len(header['EIDs'])})")
            info.append('   ' + ' '.join(['_' * 6 for i in range(ncols)]))
            info.append('  |' + '|'.join([' ' * 6 for i in range(ncols)]) + '|')
            info.append('  |' + '|'.join([field.center(6) for field, _ in header['columns']]) + '|')
            info.append('  |' + '|'.join(['_' * 6 for i in range(ncols)]) + '|')
            info.append('  |' + '|'.join([' ' * 6 for i in range(ncols)]) + '|')
            info.append('  |' + '|'.join([dtype[1:].center(6) for _, dtype in header['columns']]) + '|')
            info.append('  |' + '|'.join(['_' * 6 for i in range(ncols)]) + '|')
            info.append('')

        info.append('Restore points:')

        for i, (batch_name, batch_date, batch_files) in enumerate(self._batches):
            info.append(f"  {i} - '{batch_name}': {batch_date}")

            if detailed:

                for file in batch_files:
                    info.append(f'        {file}')

                info.append('')

        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info


class Database(ParentDatabase):

    def __init__(self, path, model=None, max_chunk_size=1e8):
        self.path = path
        self.model = model
        self._max_chunk_size = max_chunk_size
        self.reload()

    def reload(self):
        self.clear()
        self.load()

    def clear(self):
        self.tables = None
        self._headers = None
        self._project = None
        self._name = None
        self._version = None
        self._batches = None
        self._nbytes = 0

    def _set_headers(self):

        with open(os.path.join(self.path, '#header.json')) as f:
            database_header = json.load(f)

        self._project = database_header['project']
        self._name = database_header['name']
        self._version = database_header['version']
        self._batches = database_header['batches']
        self._headers = dict()

        for name in database_header['tables']:
            table_path = os.path.join(self.path, name)

            with open(os.path.join(table_path, '#header.json')) as f:
                header = json.load(f)

            header['path'] = table_path
            header['files'] = dict()
            self._headers[name] = header

    def _export_header(self):
        header = {'project': self._project,
                  'name': self._name,
                  'version': self._version,
                  'batches': self._batches,
                  'nbytes': self._nbytes,
                  'headers': {name: self._headers[name] for name in self._headers}}

        for table in header['headers']:
            header['headers'][table]['LIDs'] = [int(x) for x in header['headers'][table]['LIDs']]
            header['headers'][table]['EIDs'] = [int(x) for x in header['headers'][table]['EIDs']]

        return header

    def load(self):

        if self.tables is None:
            self._set_headers()
            self.tables = dict()

            for name, header in self._headers.items():
                LID_name, LID_dtype = header['columns'][0]
                EID_name, EID_dtype = header['columns'][1]
                n_LIDs = header['LIDs']
                n_EIDs = header['EIDs']
                LIDs_file = os.path.join(header['path'], LID_name + '.bin')
                EIDs_file = os.path.join(header['path'], EID_name + '.bin')
                self._nbytes += os.path.getsize(LIDs_file)
                self._nbytes += os.path.getsize(EIDs_file)

                if (n_LIDs * np.dtype(LID_dtype).itemsize != os.path.getsize(LIDs_file) or
                    n_EIDs * np.dtype(EID_dtype).itemsize != os.path.getsize(EIDs_file)):
                    raise ValueError("Inconsistency found! ('{}')".format(name))

                header['LIDs'] = list(np.fromfile(LIDs_file, dtype=LID_dtype))
                header['EIDs'] = np.fromfile(EIDs_file, dtype=EID_dtype)
                fields = list()

                for field_name, dtype in header['columns'][2:]:
                    file = os.path.join(header['path'], field_name + '.bin')
                    self._nbytes += os.path.getsize(file)
                    offset = n_LIDs * n_EIDs * np.dtype(dtype).itemsize

                    if 2 * offset != os.path.getsize(file):
                        raise ValueError("Inconsistency found! ('{}')".format(file))

                    fields.append(FieldData(field_name,
                                            np.memmap(file, dtype=dtype, shape=(n_LIDs, n_EIDs), mode='r'),
                                            np.memmap(file, dtype=dtype, shape=(n_EIDs, n_LIDs), mode='r', offset=offset),
                                            header['LIDs'], header['EIDs'],
                                            LID_name, EID_name))

                self.tables[name] = TableData(fields,
                                              header['LIDs'], header['EIDs'],
                                              LID_name, EID_name)
        else:
            print('Database already loaded!')

    def check(self):
        print('Checking data integrity ...')
        files_corrupted = list()

        for header in self._headers.values():

            for file, checksums in header['checksums'].items():

                with open(os.path.join(header['path'], file), 'rb') as f:

                    if checksums[-1][2] != hash_bytestr(f, get_hasher(header['checksum']), ashexstr=True):
                        files_corrupted.append(file)

            header_file = os.path.join(header['path'], '#header.json')

            with open(header_file, 'rb') as f, open(os.path.splitext(header_file)[0] + '.' + header['checksum'], 'rb') as f_checksum:

                if f_checksum.read() != hash_bytestr(f, get_hasher(header['checksum'])):
                    files_corrupted.append(header_file)

        if files_corrupted:

            for file in files_corrupted:
                print(f"'{file}' is corrupted!")
        else:
            print('Everything is OK!')

    def append(self, files, batch_name, **kwargs):

        if batch_name in {batch_name for batch_name, _, _ in self._batches}:
            raise ValueError(f"'{batch_name}' already exists!")

        print('Appending to database ...')

        if isinstance(files, str):
            files = [files]

        tables_specs = get_tables_specs()
        self._close()

        for name, header in self._headers.items():
            tables_specs[name]['columns'] = [field for field, _ in header['columns']]
            tables_specs[name]['dtypes'] = {field: dtype for field, dtype in header['columns']}
            tables_specs[name]['pch_format'] = [[(field, tables_specs[name]['dtypes'][field] if
                                                  field in tables_specs[name]['dtypes'] else
                                                  dtype) for field, dtype in row] for row in
                                                tables_specs[name]['pch_format']]
            open_table(header, new_table=False)

        _, load_cases_info = create_tables(self.path, files, tables_specs, self._headers,
                                           load_cases_info={name: dict() for name in self.tables})

        if not 'filenames' in kwargs:
            filenames = [os.path.basename(file) for file in files]
        else:
            filenames = kwargs['filenames']

        self._batches.append([batch_name, None, filenames])
        finalize_database(self.path, self.name, self.version, self.project,
                          self._headers, load_cases_info, self._batches, self._max_chunk_size)
        self.reload()

        print('Database updated succesfully!')

    def restore(self, batch_name=None):

        if not batch_name:
            batch_name = 'Initial batch'
        elif batch_name not in self.restore_points or batch_name == self.restore_points[-1]:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        print(f"Restoring database to '{batch_name}' state ...")
        self._close()

        for name, header in self._headers.items():

            if batch_name in {batch_name for batch_name, _, _ in header['checksums']['LID.bin']}:
                index = [batch_name for batch_name, _, _ in header['checksums']['LID.bin']].index(batch_name)
                position = header['checksums']['LID.bin'][index][1]
                header['LIDs'] = header['LIDs'][:position]

                for LID in header['LIDs'][position:]:
                    header['LOAD CASES INFO'].pop(LID, None)

                for field, dtype in header['columns']:

                    if field != self.tables[name].index_labels[1]:
                        header['checksums'][field + '.bin'] = header['checksums'][field + '.bin'][:index + 1]
                        offset = position * np.dtype(dtype).itemsize

                        if field not in self.tables[name].index_labels:
                            offset *= len(header['EIDs'])

                        with open(os.path.join(header['path'], field + '.bin'), 'rb+') as f:
                            f.seek(offset)
                            f.truncate()

            else:
                del self.tables[name]
                shutil.rmtree(header['path'])

        batch_index = [batch_name for batch_name, _, _ in self._batches].index(batch_name)
        finalize_database(self.path, self.name, self.version, self.project,
                          {name: self._headers[name] for name in self.tables},
                          {name: dict() for name in self.tables},
                          self._batches[:batch_index + 1], self._max_chunk_size)
        self.reload()
        print(f"Database restored to '{batch_name}' state succesfully!")

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
                    index1 = np.array(list(LIDs))

                array_agg = np.empty((len(EID_groups), n), dtype=output_array.dtype)
                LIDs_agg = np.empty((len(EID_groups), n), dtype=self.tables[table].LIDs.dtype)

                for i, EID_group in enumerate(EID_groups):
                    group_EIDs = np.array(EID_groups[EID_group])
                    weights = np.array([weights[EID] for EID in group_EIDs]) if weights else None
                    array_agg[i, :], LIDs_agg[i, :] = self._aggregate(output_array[:, np.array([iEIDs[EID] for EID in group_EIDs])],
                                                                      aggregations, LIDs, weights)

                query[f'{output_field} ({aggregations})'] = array_agg

                if index1 is None:
                    query['{} (LID {})'.format(output_field, aggregations.split('-')[1])] = LIDs_agg
            else:

                if aggregations:
                    raise ValueError('A pick query must not be aggregated!')

                query[output_field] = output_array
                index0 = np.array(list(LIDs))
                index1 = EIDs

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

    def _close(self):

        for table in self.tables.values():
            table.close()

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
        LIDs = np.array(list(LIDs))

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

    return process_query(query)


def process_query(query):
    query = {key: value if value else None for key, value in query.items()}

    for field in ('LIDs', 'geometry', 'weights'):

        try:

            if query[field]:
                query[field] = {int(key): value for key, value in query[field].items()}

        except AttributeError:
            pass

    return query

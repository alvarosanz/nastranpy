import os
import json
import shutil
import numpy as np
import pandas as pd
from nastranpy.results.field_data import FieldData
from nastranpy.results.table_data import TableData
from nastranpy.results.queries import query_functions
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.results.database_creation import create_tables, finalize_database, open_table
from nastranpy.bdf.misc import humansize, get_hasher, hash_bytestr


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

    def reload(self, headers=None):
        self._clear()
        self._load(headers)

    def _clear(self):
        self.tables = None
        self._headers = None
        self._project = None
        self._name = None
        self._version = None
        self._batches = None
        self._nbytes = 0

    def _load(self, headers=None):

        if self.path:
            self._set_headers(headers)
            self.tables = dict()
            fields = None

            for name, header in self._headers.items():
                LID_name, LID_dtype = header['columns'][0]
                EID_name, EID_dtype = header['columns'][1]

                if self._is_local:
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
                else:
                    fields = [field_name for field_name in header['columns'][2:]]

                self.tables[name] = TableData(fields,
                                              header['LIDs'], header['EIDs'],
                                              LID_name, EID_name)

    def _set_headers(self, headers=None):

        if self._is_local:

            with open(os.path.join(self.path, '#header.json')) as f:
                headers = json.load(f)

        else:

            if not headers:
                headers = self._request(request_type='header', path=self.path)

            self.path = headers['path']
            self._headers = headers['headers']
            self._nbytes = headers['nbytes']

        self._project = headers['project']
        self._name = headers['name']
        self._version = headers['version']
        self._batches = headers['batches']

        if self._is_local:
            self._headers = dict()

            for name in headers['tables']:
                table_path = os.path.join(self.path, name)

                if self._is_local:

                    with open(os.path.join(table_path, '#header.json')) as f:
                        header = json.load(f)

                header['path'] = table_path
                header['files'] = dict()
                self._headers[name] = header

    def _export_header(self):
        header = {'path': self.path,
                  'project': self._project,
                  'name': self._name,
                  'version': self._version,
                  'batches': self._batches,
                  'nbytes': self._nbytes,
                  'headers': {name: self._headers[name] for name in self._headers}}

        for table in header['headers']:
            header['headers'][table]['LIDs'] = [int(x) for x in header['headers'][table]['LIDs']]
            header['headers'][table]['EIDs'] = [int(x) for x in header['headers'][table]['EIDs']]

        return header

    def _get_tables_specs(self):
        tables_specs = get_tables_specs()

        for name, header in self._headers.items():
            tables_specs[name]['columns'] = [field for field, _ in header['columns']]
            tables_specs[name]['dtypes'] = {field: dtype for field, dtype in header['columns']}
            tables_specs[name]['pch_format'] = [[(field, tables_specs[name]['dtypes'][field] if
                                                  field in tables_specs[name]['dtypes'] else
                                                  dtype) for field, dtype in row] for row in
                                                tables_specs[name]['pch_format']]

        return tables_specs

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

    def __init__(self, path=None, max_chunk_size=1e8):
        self.path = path
        self._max_chunk_size = max_chunk_size
        self._is_local = True
        self.reload()

    def check(self, print_to_screen=True):
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

        info = list()

        if files_corrupted:

            for file in files_corrupted:
                info.append(f"'{file}' is corrupted!")
        else:
            info.append('Everything is OK!')

        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info

    def create(self, files, database_path, database_name, database_version,
               database_project=None, tables_specs=None, overwrite=False,
               checksum='sha256', table_generator=None):
        print('Creating database ...')

        if not os.path.exists(database_path):
            os.mkdir(database_path)
        elif not overwrite:
            raise FileExistsError(f"Database already exists at '{database_path}'!")

        self.path = database_path

        if isinstance(files, str):
            files = [files]

        batches = [['Initial batch', None, [os.path.basename(file) for file in files]]]
        headers, load_cases_info = create_tables(self.path, files, tables_specs,
                                                 checksum=checksum, table_generator=table_generator)
        finalize_database(self.path, database_name, database_version, database_project,
                          headers, load_cases_info, batches, self._max_chunk_size)

        self.reload()
        print('Database created succesfully!')

    def append(self, files, batch_name, table_generator=None):

        if batch_name in {batch_name for batch_name, _, _ in self._batches}:
            raise ValueError(f"'{batch_name}' already exists!")

        print('Appending to database ...')

        if isinstance(files, str):
            files = [files]

        self._close()

        for header in self._headers.values():
            open_table(header, new_table=False)

        _, load_cases_info = create_tables(self.path, files, self._get_tables_specs(), self._headers,
                                           load_cases_info={name: dict() for name in self.tables},
                                           table_generator=table_generator)

        self._batches.append([batch_name, None, [os.path.basename(file) for file in files]])
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
              geometry=None, weights=None, **kwargs):
        EID_groups = None

        if isinstance(EIDs, dict):
            EID_groups = EIDs
            EIDs = sorted({EID for EIDs in EIDs.values() for EID in EIDs})
            iEIDs = {EID: i for i, EID in enumerate(EIDs)}

        if geometry:
            geometry = {parameter: np.array([geometry[parameter][EID] for EID in EIDs]) for
                        parameter in geometry}

        if not outputs:
            outputs = self.tables[table].names

        LIDs_queried = self.tables[table].LIDs if LIDs is None else np.array(list(LIDs), dtype=np.int64)
        EIDs_queried = self.tables[table].EIDs if EIDs is None else np.array(list(EIDs), dtype=np.int64)
        data = np.empty((len(outputs), len(LIDs_queried), len(EIDs_queried)), dtype=np.float64)
        data_agg = None
        aggs = dict()
        columns = list()
        fields = dict()
        n_aggregations = None

        for i, output in enumerate(outputs):
            output_array = data[i, :, :]

            if isinstance(output, str):
                output_field, is_absolute = self._is_abs(output.upper())
                aggregations = list()
            else:
                output_field, is_absolute = self._is_abs(output[0].upper())
                aggregations = output[1].strip().upper().split('-')

            if aggregations and not EID_groups:
                raise ValueError('A pick query must not be aggregated!')

            if not aggregations and EID_groups:
                raise ValueError('A grouped query must be aggregated!')

            if n_aggregations is None:
                n_aggregations = len(aggregations)
            elif len(aggregations) != n_aggregations:
                raise ValueError("All aggregations must be one-level (i.e. 'AVG') or two-level (i. e. 'AVG-MAX')")

            if output_field in self.tables[table]:

                if output_field not in fields:
                    fields[output_field] = self.tables[table][output_field].read(LIDs, EIDs, out=output_array)
                else:
                    output_array[:, :] = fields[output_field]

            else:

                if output_field in query_functions[table]:
                    func, func_args = query_functions[table][output_field]
                    args = list()

                    for arg in func_args:

                        if arg in self.tables[table]:

                            if arg not in fields:
                                fields[arg] = self.tables[table][arg].read(LIDs, EIDs)

                            arg = fields[arg]
                        elif geometry and arg in geometry:
                            arg = geometry[arg]
                        else:
                            continue

                        args.append(arg)

                    args.append(output_array)
                    func(*args)
                else:
                    raise ValueError(f"Unsupported output: '{output_field}'")

            if is_absolute:
                fields[output_field] =np.array(output_array)
                np.abs(output_array, out=output_array)
                output_field = f'ABS({output_field})'

            if EID_groups:
                index0 = LIDs_queried
                index1 = list(EID_groups)
                columns.append(f"{output_field} ({'-'.join(aggregations)})")

                if len(aggregations) == 2:
                    index0 = None
                    array_agg = np.empty((1, len(EID_groups)), dtype=np.float64)
                    LIDs_agg = np.empty((1, len(EID_groups)), dtype=np.int64)
                    aggs[columns[-1]] = array_agg
                    aggs['{} (LID {})'.format(output_field, aggregations[1])] = LIDs_agg
                else:

                    if data_agg is None:
                        data_agg = np.empty((len(outputs), len(LIDs_queried), len(EID_groups)), dtype=np.float64)

                    array_agg = data_agg[i, :, :]
                    LIDs_agg = np.empty((1, len(EID_groups)), dtype=np.int64)

                for j, EID_group in enumerate(EID_groups.values()):
                    self._aggregate(output_array[:, np.array([iEIDs[EID] for EID in EID_group])],
                                    array_agg[:, j], aggregations, LIDs_queried, LIDs_agg[:, j],
                                    np.array([weights[EID] for EID in EID_group]) if weights else None)

            else:
                index0 = LIDs_queried
                index1 = EIDs_queried
                columns.append(output_field)

        index_names = list(self.tables[table].index_labels)

        if EID_groups:
            data = data_agg
            index_names[1] = 'Group'

        if len(aggregations) < 2:
            data = data.reshape((len(outputs), len(index0) * len(index1))).T
            index = pd.MultiIndex.from_product([index0, index1], names=index_names)
        else:
            data = {field: aggs[field].ravel() for field in aggs}
            columns = list(aggs)
            index = pd.Index(index1, name=index_names[1])

        return pd.DataFrame(data, columns=columns, index=index, copy=False)

    def _close(self):

        for table in self.tables.values():
            table.close()

    @staticmethod
    def _is_abs(field_str):

        if field_str[:4] == 'ABS(' and field_str[-1] == ')':
            return field_str[4:-1], True
        else:
            return field_str, False

    @classmethod
    def _aggregate(cls, array, array_agg, aggregations, LIDs, LIDs_agg, weights):

        for i, aggregation in enumerate(aggregations):
            axis = 1 - i
            aggregation, is_absolute = cls._is_abs(aggregation)

            if axis == 0:
                LIDs = np.array(list(LIDs), dtype=np.int64)

            if aggregation == 'AVG':

                if axis == 0:
                    raise ValueError("'AVG' aggregation cannot be applied to LIDs!")

                array = np.average(array, axis, weights)
            elif aggregation == 'MAX':

                if axis == 0:
                    LIDs_agg[0] = LIDs[array.argmax(axis)]
                    array = np.array([np.max(array, axis)])
                else:
                    array = np.max(array, axis)
            elif aggregation == 'MIN':

                if axis == 0:
                    LIDs_agg[0] = LIDs[array.argmin(axis)]
                    array = np.array([np.min(array, axis)])
                else:
                    array = np.min(array, axis)
            else:
                raise ValueError(f"Unsupported aggregation method: '{aggregation}'")

            if is_absolute:
                np.abs(array, out=array)

        array_agg[:] = array


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

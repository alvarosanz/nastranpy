import os
from pathlib import Path
import json
import shutil
import numpy as np
from nastranpy.results.field_data import FieldData
from nastranpy.results.table_data import TableData
from nastranpy.results.queries import query_functions
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.results.database_creation import create_tables, finalize_database, open_table, truncate_file
from nastranpy.results.results import get_query_from_file
from nastranpy.bdf.misc import humansize, get_hasher, hash_bytestr


class DatabaseHeader(object):

    def __init__(self, path=None, header=None):

        if header:
            self.__dict__ = header
        else:

            with open(os.path.join(path, '##header.json')) as f:
                self.__dict__ = json.load(f)

            self.nbytes = 0
            self.tables = dict()

            for name in self.checksums:

                with open(os.path.join(os.path.join(path, name), '#header.json')) as f:
                    self.tables[name] = json.load(f)

                for i, (field_name, dtype) in enumerate(self.tables[name]['columns']):
                    file = os.path.join(path, name, field_name + '.bin')
                    self.nbytes += os.path.getsize(file)

                    if i == 0:
                        self.tables[name]['LIDs'] = np.fromfile(file, dtype=dtype).tolist()
                    elif i == 1:
                        self.tables[name]['IDs'] = np.fromfile(file, dtype=dtype).tolist()

    def info(self, print_to_screen=True, detailed=False):
        info = list()

        if self.project:
            info.append(f'Project: {self.project}')

        info.append(f'Name: {self.name}')
        info.append(f'Version: {self.version}')
        info.append(f'Size: {humansize(self.nbytes)}'.format())
        info.append('')

        for table in self.tables.values():
            ncols = len(table['columns'])
            info.append(f"Table: '{table['name']}' ({table['columns'][0][0]}: {len(table['LIDs'])}, {table['columns'][1][0]}: {len(table['IDs'])})")
            info.append('   ' + ' '.join(['_' * 6 for i in range(ncols)]))
            info.append('  |' + '|'.join([' ' * 6 for i in range(ncols)]) + '|')
            info.append('  |' + '|'.join([field.center(6) for field, _ in table['columns']]) + '|')
            info.append('  |' + '|'.join(['_' * 6 for i in range(ncols)]) + '|')
            info.append('  |' + '|'.join([' ' * 6 for i in range(ncols)]) + '|')
            info.append('  |' + '|'.join([dtype[1:].center(6) for _, dtype in table['columns']]) + '|')
            info.append('  |' + '|'.join(['_' * 6 for i in range(ncols)]) + '|')
            info.append('')

        info.append('Restore points:')

        for i, (batch_name, batch_date, batch_files) in enumerate(self.batches):
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


class Database(object):

    def __init__(self, path=None):
        self.path = path
        self.load()

    def load(self):

        if self.path:
            self.header = DatabaseHeader(self.path)
            self.tables = dict()

            for name, header in self.header.tables.items():
                fields = [FieldData(field_name, dtype,
                                    os.path.join(self.path, name, field_name + '.bin'),
                                    header['LIDs'], header['IDs']) for field_name, dtype in
                          header['columns'][2:]]
                self.tables[name] = TableData(fields, header['LIDs'], header['IDs'])

    def check(self, print_to_screen=True):
        files_corrupted = list()

        for name, header in self.header.tables.items():

            for file, checksum in header['batches'][-1][2].items():

                with open(os.path.join(self.path, name, file), 'rb') as f:

                    if checksum != hash_bytestr(f, get_hasher(self.header.checksum)):
                        files_corrupted.append(file)

            header_file = os.path.join(self.path, name, '#header.json')

            with open(header_file, 'rb') as f:

                if self.header.checksums[header['name']] != hash_bytestr(f, get_hasher(self.header.checksum)):
                    files_corrupted.append(header_file)

        database_header_file = os.path.join(self.path, '##header.json')

        with open(database_header_file, 'rb') as f, open(os.path.splitext(database_header_file)[0] + '.' + self.header.checksum, 'rb') as f_checksum:

            if f_checksum.read() != hash_bytestr(f, get_hasher(self.header.checksum), ashexstr=False):
                files_corrupted.append(database_header_file)

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
               table_generator=None, max_chunk_size=1e8):
        Path(database_path).mkdir(parents=True, exist_ok=overwrite)
        print('Creating database ...')
        self.path = database_path

        if isinstance(files, str):
            files = [files]

        batches = [['Initial batch', None, [os.path.basename(file) for file in files]]]
        headers, load_cases_info = create_tables(self.path, files, tables_specs,
                                                 table_generator=table_generator)
        finalize_database(self.path, database_name, database_version, database_project,
                          headers, load_cases_info, batches, max_chunk_size)

        self.load()
        print('Database created succesfully!')

    def append(self, files, batch_name, table_generator=None, max_chunk_size=1e8):

        if batch_name in {batch_name for batch_name, _, _ in self.header.batches}:
            raise ValueError(f"'{batch_name}' already exists!")

        print('Appending to database ...')

        if isinstance(files, str):
            files = [files]

        self._close()

        for header in self.header.tables.values():
            header['path'] = os.path.join(self.path, header['name'])
            open_table(header, new_table=False)

        _, load_cases_info = create_tables(self.path, files, self._get_tables_specs(), self.header.tables,
                                           load_cases_info={name: dict() for name in self.tables},
                                           table_generator=table_generator)

        self.header.batches.append([batch_name, None, [os.path.basename(file) for file in files]])
        finalize_database(self.path, self.header.name, self.header.version, self.header.project,
                          self.header.tables, load_cases_info, self.header.batches, max_chunk_size, self.header.checksum)
        self.load()
        print('Database updated succesfully!')

    def restore(self, batch_name, max_chunk_size=1e8):
        restore_points = [batch[0] for batch in self.header.batches]

        if batch_name not in restore_points or batch_name == restore_points[-1]:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        print(f"Restoring database to '{batch_name}' state ...")
        self._close()
        batch_index = 0

        for name, header in self.header.tables.items():

            try:
                index = [batch_name for batch_name, _, _ in header['batches']].index(batch_name)
                batch_index = max(batch_index, index)
                header['batches'] = header['batches'][:index + 1]
                position = header['batches'][index][1]

                for LID in header['LIDs'][position:]:
                    header['LOAD CASES INFO'].pop(LID, None)

                header['LIDs'] = header['LIDs'][:position]

                truncate_file(os.path.join(self.path, name, 'LID.bin'),
                              position * np.dtype(header['columns'][0][1]).itemsize)

                for field, dtype in header['columns'][2:]:
                    truncate_file(os.path.join(self.path, name, field + '.bin'),
                                  position * np.dtype(dtype).itemsize * len(header['IDs']))

                header['path'] = os.path.join(self.path, header['name'])

            except ValueError:
                del self.tables[name]
                shutil.rmtree(self.path, name)

        finalize_database(self.path, self.header.name, self.header.version, self.header.project,
                          {name: self.header.tables[name] for name in self.tables},
                          {name: dict() for name in self.tables},
                          self.header.batches[:batch_index + 1], max_chunk_size, self.header.checksum)
        self.load()
        print(f"Database restored to '{batch_name}' state succesfully!")

    def query(self, table=None, outputs=None, LIDs=None, IDs=None,
              geometry=None, weights=None, **kwargs):
        import pandas as pd
        ID_groups = None

        if isinstance(IDs, dict):
            ID_groups = IDs
            IDs = sorted({ID for IDs in IDs.values() for ID in IDs})
            iIDs = {ID: i for i, ID in enumerate(IDs)}

        if geometry:
            geometry = {parameter: np.array([geometry[parameter][ID] for ID in IDs]) for
                        parameter in geometry}

        if not outputs:
            outputs = self.tables[table].names

        LIDs_queried = self.tables[table].LIDs if LIDs is None else np.array(list(LIDs), dtype=np.int64)
        IDs_queried = self.tables[table].IDs if IDs is None else np.array(list(IDs), dtype=np.int64)
        data = np.empty((len(outputs), len(LIDs_queried), len(IDs_queried)), dtype=np.float64)
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

            if aggregations and not ID_groups:
                raise ValueError('A pick query must not be aggregated!')

            if not aggregations and ID_groups:
                raise ValueError('A grouped query must be aggregated!')

            if n_aggregations is None:
                n_aggregations = len(aggregations)
            elif len(aggregations) != n_aggregations:
                raise ValueError("All aggregations must be one-level (i.e. 'AVG') or two-level (i. e. 'AVG-MAX')")

            if output_field in self.tables[table]:

                if output_field not in fields:
                    fields[output_field] = self.tables[table][output_field].read(LIDs, IDs, out=output_array)
                else:
                    output_array[:, :] = fields[output_field]

            else:

                if output_field in query_functions[table]:
                    func, func_args = query_functions[table][output_field]
                    args = list()

                    for arg in func_args:

                        if arg in self.tables[table]:

                            if arg not in fields:
                                fields[arg] = self.tables[table][arg].read(LIDs, IDs)

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

            if ID_groups:
                index0 = LIDs_queried
                index1 = list(ID_groups)
                columns.append(f"{output_field} ({'-'.join(aggregations)})")

                if len(aggregations) == 2:
                    index0 = None
                    array_agg = np.empty((1, len(ID_groups)), dtype=np.float64)
                    LIDs_agg = np.empty((1, len(ID_groups)), dtype=np.int64)
                    aggs[columns[-1]] = array_agg
                    aggs['{} (LID {})'.format(output_field, aggregations[1])] = LIDs_agg
                else:

                    if data_agg is None:
                        data_agg = np.empty((len(outputs), len(LIDs_queried), len(ID_groups)), dtype=np.float64)

                    array_agg = data_agg[i, :, :]
                    LIDs_agg = np.empty((1, len(ID_groups)), dtype=np.int64)

                for j, ID_group in enumerate(ID_groups.values()):
                    self._aggregate(output_array[:, np.array([iIDs[ID] for ID in ID_group])],
                                    array_agg[:, j], aggregations, LIDs_queried, LIDs_agg[:, j],
                                    np.array([weights[ID] for ID in ID_group]) if weights else None)

            else:
                index0 = LIDs_queried
                index1 = IDs_queried
                columns.append(output_field)

        index_names = [self.header.tables[table]['columns'][0][0],
                       self.header.tables[table]['columns'][1][0]]

        if ID_groups:
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

    def query_from_file(self, file):
        return self.query(**get_query_from_file(file))

    def _close(self):

        for table in self.tables.values():
            table.close()

    def _get_tables_specs(self):
        tables_specs = get_tables_specs()

        for name, header in self.header.tables.items():
            tables_specs[name]['columns'] = [field for field, _ in header['columns']]
            tables_specs[name]['dtypes'] = {field: dtype for field, dtype in header['columns']}
            tables_specs[name]['pch_format'] = [[(field, tables_specs[name]['dtypes'][field] if
                                                  field in tables_specs[name]['dtypes'] else
                                                  dtype) for field, dtype in row] for row in
                                                tables_specs[name]['pch_format']]

        return tables_specs

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

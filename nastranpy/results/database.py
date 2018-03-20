import os
import json
import csv
import numpy as np
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

        files_corrupted = list()

        for table_name, table_path, table_header in self._walk_header():

            files = [field for field, _ in table_header['columns']]
            files += [field + '#T' for field, _ in table_header['columns'][2:]]
            files = [os.path.join(table_path, field + '.bin') for field in files]

            for file in files:

                with open(file, 'rb') as f, open(file[:-3] + table_header['checksum'], 'rb') as f_checksum:

                    if f_checksum.read() != hash_bytestr(f, get_hasher(table_header['checksum'])):
                        files_corrupted.append(file)

        if files_corrupted:

            for file in files_corrupted:
                print(f"'{file}' is corrupted!")
        else:
            print('Everything is OK!')

    def query(self, table=None, fields=None, outputs=None,
              LIDs=None, EIDs=None, LID_combinations=None,
              geometry=None, weights=None, return_stats=True, file=None, **kwargs):

        if file:
            return self.query(**get_query_from_file(file))
        elif not table and not fields:
            raise ValueError('You must specify a query!')

        EID_groups = None

        if isinstance(EIDs, dict):
            EID_groups = dict(EIDs)
            EIDs = sorted({EID for EID_group in EIDs.values() for EID in EID_group})

        if isinstance(fields, str):
            fields = [fields]

        fields = [field.upper() for field in fields]
        fields = [(field[4:-1], field, True) if field[:4] == 'ABS(' and field[-1] == ')' else
                  (field, field, False) for field in fields]

        field_arrays = dict()

        for field, field_mod, use_abs in fields:
            field_array, LIDs_returned, EIDs_returned = self.tables[table][field].get_array(LIDs, EIDs, LID_combinations,
                                                                                            return_indexes=True,
                                                                                            absolute_value=use_abs)
            field_arrays[field_mod] = field_array

        iEIDs = {EID: i for i, EID in enumerate(EIDs_returned)}

        if geometry:
            geometry = np.array([geometry[EID] for EID in EIDs_returned])

        if not outputs:
            query = {field: (array, LIDs_returned, EIDs_returned, None) for field, array in field_arrays.items()}
        else:
            query = dict()

            if not isinstance(outputs, list):
                outputs = [outputs]

            for query_output in outputs:

                if isinstance(query_output, str):
                    output_field = query_output
                    aggregations = None
                else:
                    output_field, aggregations = query_output

                if not callable(output_field):

                    if output_field in field_arrays:
                        output_array = np.array(field_arrays[output_field])
                    elif output_field.upper() in query_functions:
                        output_array = query_functions[output_field.upper()](*field_arrays.values(), geometry)
                    else:
                        raise ValueError('Unsupported output field: {}'.format(output_field))
                else:
                    output_array = output_field(*field_arrays.values(), geometry)
                    output_field = output_field.__name__

                if EID_groups:

                    if not aggregations:
                        raise ValueError('A grouped query must be aggregated!')

                    query[output_field] = dict()

                    for EID_group in EID_groups:
                        EIDs = np.array(EID_groups[EID_group])
                        weights = np.array([weights[EID] for EID in EIDs]) if weights else None
                        query[output_field][EID_group] = self._get_output(output_array[:, np.array([iEIDs[EID] for EID in EIDs])],
                                                                          aggregations,
                                                                          LIDs_returned, EIDs,
                                                                          weights)

                else:
                    EIDs = EIDs_returned
                    weights = np.array([weights[EID] for EID in EIDs]) if weights else None
                    query[output_field] = self._get_output(output_array,
                                                           aggregations,
                                                           LIDs_returned, EIDs,
                                                           weights)

        return query

    def write_query(self, query, path):
        pass

        # def iter_query(query):

        #     try:
        #         for name, (array, LIDs, EIDs, aggregations) in query.items():
        #             yield (name, aggregations, array, EIDs, LIDs)

        #     except ValueError:

        #         for name in query:
        #             EID_groups = list()
        #             arrays = list()

        #             for EID_group, (array, LIDs, EIDs, aggregations) in query[name].items():
        #                 EID_groups.append(EID_group)
        #                 arrays.append(array)

        #             yield (name, aggregations, array, EID_groups, LIDs)


        # for name, aggregation, rows, indexes, header in iter_query(query):
        #     filename = f'{name}_{aggregations}.csv' if aggregations else f'{name}.csv'
        #     name = f'{name} ({aggregations})' if aggregations else name

        #     with open(os.path.join(path, filename), 'w') as f:
        #         csv_writer = csv.writer(f, lineterminator='\n')
        #         csv_writer.

        #         for row in rows:
        #             csv_writer.writerow(row)


    def get_dataframe(self, table_name, LIDs=None, EIDs=None, columns=None,
                      fields_derived=None):
        return self.tables[table_name].get_dataframe(LIDs, EIDs, columns, fields_derived)

    @staticmethod
    def _get_output(output_array, aggregations, LIDs, EIDs, weights):

        if aggregations:
            EIDs = None

            for i, aggregation in enumerate(aggregations.strip().upper().split('-')):
                axis = 1 - i

                if aggregation == 'AVG':

                    if axis == 0:
                        LIDs = None

                    output_array = np.average(output_array, axis, weights)
                elif aggregation == 'MAX':

                    if axis == 0:
                        LIDs = LIDs[output_array.argmax(axis)]

                    output_array = np.max(output_array, axis)
                elif aggregation == 'MIN':

                    if axis == 0:
                        LIDs = LIDs[output_array.argmin(axis)]

                    output_array = np.min(output_array, axis)
                else:
                    raise ValueError('Unsupported aggregation method: {}'.format(aggregation))

                if axis == 0:
                    output_array = np.array([output_array])

                    if not LIDs is None:
                        LIDs = np.array([LIDs])

        return (output_array, LIDs, EIDs)


def get_query_from_file(file):

    try:

        with open(file) as f:
            query = json.load(f)

    except TypeError:
        query = json.load(file)

    query = {key: value if value else None for key, value in query.items()}

    for field in ('LID_combinations', 'geometry', 'weights'):

        if query[field]:
            query[field] = {int(key): value for key, value in query[field].items()}

    return query

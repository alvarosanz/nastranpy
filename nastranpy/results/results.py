import os
import json
import datetime
import numpy as np
import pandas as pd
from nastranpy.results.read_files import tables_in_pch
from nastranpy.results.database import DataBase, get_query_from_file
from nastranpy.results.tables_specs import tables_specs
from nastranpy.bdf.misc import get_plural, get_hasher, hash_bytestr


def get_tables(files):

    if isinstance(files, str):
        files = [files]

    tables = dict()

    for file in files:

        for table in tables_in_pch(file, tables_specs=tables_specs):
            table_name = '{} - {}'.format(table.name, table.element_type)

            if table_name not in tables:
                tables[table_name] = list()

            tables[table_name].append(table.df)

    return {table_name: pd.concat(tables[table_name]) for table_name in tables}


def query(file):
    query = get_query_from_file(file)
    database = DataBase(query['path'])

    if query['check']:
        database.check()

    df = database.query(**query)

    if query['output_path']:
        df.to_csv(query['output_path'])

    return df


def create_database(files, database_path, database_name, database_version,
                    database_project=None, max_chunk_size=1e8, checksum='sha256'):
    print('Creating database ...')

    if not os.path.exists(database_path):
        os.mkdir(database_path)

    if isinstance(files, str):
        files = [files]

    tables_info, load_cases_info = _create_tables(database_path, files)

    for table_name, table_info in tables_info.items():
        _create_table_header(table_info, load_cases_info[table_name], checksum)
        _create_transpose(table_info, max_chunk_size)
        _create_checksums(table_info, checksum)

    _create_database_header(database_path, database_name, database_version,
                            database_project, tables_info)

    print('Database created succesfully!')
    return DataBase(database_path)


def _create_tables(database_path, files):
    tables_info = dict()
    load_cases_info = dict()
    ignored_tables = set()

    try:

        for file in files:

            for table in tables_in_pch(file, tables_specs=tables_specs):
                table_name = '{} - {}'.format(table.name, table.element_type)

                if table_name not in tables_specs:

                    if table_name not in ignored_tables:
                        print("WARNING: '{}' is not supported!".format(table_name))
                        ignored_tables.add(table_name)

                    continue

                if len(table.df.index.get_level_values(0).unique()) != 1:
                    raise ValueError("Inconsistent LIDs! ('{}')".format(table_name))

                if table_name not in tables_info:
                    table_path = os.path.join(database_path, table_name)
                    tables_info[table_name] = _open_table(table_name, table_path, table)
                    load_cases_info[table_name] = dict()
                    is_success = True
                else:
                    is_success = _append_to_table(table, tables_info[table_name])

                if is_success:
                    load_cases_info[table_name][table.subcase] = {'TITLE': table.title,
                                                                  'SUBTITLE': table.subtitle,
                                                                  'LABEL': table.label}
    finally:

        for table_info in tables_info.values():
            _close_table(table_info)

    return tables_info, load_cases_info


def _open_table(table_name, table_path, table):
    table_info = dict()

    if not os.path.exists(table_path):
        os.mkdir(table_path)

    table_info['name'] = table_name
    table_info['path'] = table_path
    table_info['specs'] = tables_specs[table_name]
    table_info['LIDs'] = [table.df.index.get_level_values(0).values[0]]
    table_info['EIDs'] = table.df.index.get_level_values(1).values
    table_info['files'] = dict()

    for field_name in table_info['specs']['columns'][2:]:
        table_info['files'][field_name] = open(os.path.join(table_path, field_name + '.bin'), 'wb')
        table.df[field_name].values.tofile(table_info['files'][field_name])

    return table_info


def _append_to_table(table, table_info):
    LID = table.df.index.get_level_values(0).values[0]
    table_info['LIDs'].append(LID)
    EIDs = table.df.index.get_level_values(1).values
    index = None

    if not np.array_equal(table_info['EIDs'], EIDs):
        iEIDs = {EID: i for i, EID in enumerate(EIDs)}
        label = table_info['specs']['columns'][1] + 's'

        try:
            index = np.array([iEIDs[EID] for EID in table_info['EIDs']])
        except KeyError:
            print(f'WARNING: Missing {label}! The whole subcase will be omitted (LID: {LID})')
            return False

        if len(index) < len(EIDs):
            print(f'WARNING: Additional {label} found! These will be ommitted (LID: {LID})')

    for field_name in table_info['specs']['columns'][2:]:

        if index is None:
            table.df[field_name].values.tofile(table_info['files'][field_name])
        else:
            table.df[field_name].values[index].tofile(table_info['files'][field_name])

    return True


def _close_table(table_info):
    table_info['LIDs'] = np.array(table_info['LIDs'])
    table_info['EIDs'] = np.array(table_info['EIDs'])
    table_info['LIDs'].tofile(os.path.join(table_info['path'], table_info['specs']['columns'][0] + '.bin'))
    table_info['EIDs'].tofile(os.path.join(table_info['path'], table_info['specs']['columns'][1] + '.bin'))

    try:

        for file in table_info['files'].values():
            file.close()

    except KeyError:
        pass


def _create_table_header(table_info, load_cases_info, checksum):
    header = dict()
    header['name'] = table_info['name']
    header['columns'] = [(field_name, table_info['specs']['dtypes'][field_name]) for
                         field_name in table_info['specs']['columns']]
    header[get_plural(table_info['specs']['columns'][0])] = len(table_info['LIDs'])
    header[get_plural(table_info['specs']['columns'][1])] = len(table_info['EIDs'])
    header['checksum'] = checksum

    common_items = dict()

    for item in ['TITLE', 'SUBTITLE', 'LABEL']:
        common_items[item] = None
        unique_values = {load_case_info[item] for load_case_info in
                         load_cases_info.values()}

        if len(unique_values) == 1:
            common_items[item] = unique_values.pop()

            if common_items[item]:
                header[item] = common_items[item]

    if not all(item is None for item in common_items.values()):
        header['LOAD CASES INFO'] = dict()

        for LID in sorted(load_cases_info):

            if any(item for item in load_cases_info[LID].values()):
                header['LOAD CASES INFO'][LID] = dict()

                for item in ['TITLE', 'SUBTITLE', 'LABEL']:

                    if load_cases_info[LID][item] and common_items[item] is None:
                        header['LOAD CASES INFO'][LID][item] = load_cases_info[LID][item]

    with open(os.path.join(table_info['path'], '#header.json'), 'w') as f:
        json.dump(header, f, indent=4)


def _create_transpose(table_info, max_chunk_size):

    for field_name in table_info['specs']['columns'][2:]:

        with open(os.path.join(table_info['path'], field_name + '#T.bin'), 'wb') as f:
            n_LIDs = len(table_info['LIDs'])
            n_EIDs = len(table_info['EIDs'])
            dtype = table_info['specs']['dtypes'][field_name]
            dtype_size = np.dtype(dtype).itemsize
            field_file = os.path.join(table_info['path'], field_name + '.bin')
            field_array = np.memmap(field_file, dtype=dtype, shape=(n_LIDs, n_EIDs),
                                    mode='r')
            n_EIDs_per_chunk = int(max_chunk_size // (n_LIDs * dtype_size))
            n_chunks = int(n_EIDs // n_EIDs_per_chunk)
            n_EIDs_last_chunk = int(n_EIDs % n_EIDs_per_chunk)

            if n_EIDs != n_EIDs_per_chunk * n_chunks + n_EIDs_last_chunk:
                raise ValueError("Inconsistency found! ('{}')".format(table_name))

            chunks = list()

            if n_chunks:
                chunk = np.empty((n_EIDs_per_chunk, n_LIDs), dtype)
                chunks += [(chunk, n_EIDs_per_chunk)] * n_chunks

            if n_EIDs_last_chunk:
                last_chunk = np.empty((n_EIDs_last_chunk, n_LIDs), dtype)
                chunks.append((last_chunk, n_EIDs_last_chunk))

            i0 = 0
            i1 = 0

            for chunk, n_EIDs_per_chunk in chunks:
                i1 += n_EIDs_per_chunk
                chunk = field_array[:, i0:i1].T
                chunk.tofile(f)
                i0 += n_EIDs_per_chunk


def _create_checksums(table_info, checksum):
    files = [field for field in table_info['specs']['columns']]
    files += [field + '#T' for field in table_info['specs']['columns'][2:]]
    files = [os.path.join(table_info['path'], field + '.bin') for field in files]
    files.append(os.path.join(table_info['path'], '#header.json'))

    for file in files:

        with open(file, 'rb') as f_in, open(os.path.splitext(file)[0] + '.' + checksum, 'wb') as f_out:
            f_out.write(hash_bytestr(f_in, get_hasher(checksum)))


def _create_database_header(database_path, database_name, database_version,
                            database_project, tables_info):

    with open(os.path.join(database_path, '#header.json'), 'w') as f:

        if database_project is None:
            database_project = ''

        json.dump({'project': database_project,
                   'name': database_name,
                   'version': database_version,
                   'date': str(datetime.date.today()),
                   'tables': [table for table in tables_info]}, f, indent=4)

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

    return database.query(**query)


def create_database(files, database_path, database_name, database_version,
                    database_project=None, max_chunk_size=1e8, checksum='sha256'):

    if not os.path.exists(database_path):
        os.mkdir(database_path)

    if isinstance(files, str):
        files = [files]

    tables = dict()
    ignored_tables = set()
    load_cases_info = dict()

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

                if table_name not in tables:
                    tables[table_name] = dict()
                    load_cases_info[table_name] = dict()
                    table_path = os.path.join(database_path, table_name)

                    if not os.path.exists(table_path):
                        os.mkdir(table_path)

                    tables[table_name]['specs'] = {
                        'name': table_name,
                        'columns': tables_specs[table_name]['columns'],
                        'dtypes': tables_specs[table_name]['dtypes'],
                    }

                    tables[table_name]['LIDs'] = [table.df.index.get_level_values(0).values[0]]
                    tables[table_name]['EIDs'] = table.df.index.get_level_values(1).values
                    tables[table_name]['files'] = dict()

                    for field_name in tables_specs[table_name]['columns'][2:]:
                        tables[table_name]['files'][field_name] = open(os.path.join(table_path, field_name + '.bin'), 'wb')
                        table.df[field_name].values.tofile(tables[table_name]['files'][field_name])

                else:
                    LID = table.df.index.get_level_values(0).values[0]
                    tables[table_name]['LIDs'].append(LID)
                    EIDs = table.df.index.get_level_values(1).values
                    index = None

                    if not np.array_equal(tables[table_name]['EIDs'], EIDs):
                        iEIDs = {EID: i for i, EID in enumerate(EIDs)}
                        label = tables_specs[table_name]['columns'][1] + 's'

                        try:
                            index = np.array([iEIDs[EID] for EID in tables[table_name]['EIDs']])
                        except KeyError:
                            print(f'WARNING: Missing {label}! The whole subcase will be omitted (LID: {LID})')
                            continue

                        if len(index) < len(EIDs):
                            print(f'WARNING: Additional {label} found! These will be ommitted (LID: {LID})')

                    for field_name in tables_specs[table_name]['columns'][2:]:

                        if index is None:
                            table.df[field_name].values.tofile(tables[table_name]['files'][field_name])
                        else:
                            table.df[field_name].values[index].tofile(tables[table_name]['files'][field_name])

                load_cases_info[table_name][table.subcase] = {'TITLE': table.title,
                                                              'SUBTITLE': table.subtitle,
                                                              'LABEL': table.label}
    finally:

        for table in tables.values():

            try:

                for file in table['files'].values():
                    file.close()

            except KeyError:
                pass

    for table_name, table in tables.items():
        table_path = os.path.join(database_path, table_name)
        table['LIDs'] = np.array(table['LIDs'])
        table['EIDs'] = np.array(table['EIDs'])
        n_LIDs = len(table['LIDs'])
        n_EIDs = len(table['EIDs'])
        table['LIDs'].tofile(os.path.join(table_path, tables_specs[table_name]['columns'][0] + '.bin'))
        table['EIDs'].tofile(os.path.join(table_path, tables_specs[table_name]['columns'][1] + '.bin'))

        with open(os.path.join(table_path, '#header.json'), 'w') as f:
            header = dict()
            header['name'] = table_name
            header['columns'] = [(field_name, tables_specs[table_name]['dtypes'][field_name]) for
                                 field_name in tables_specs[table_name]['columns']]
            header[get_plural(tables_specs[table_name]['columns'][0])] = n_LIDs
            header[get_plural(tables_specs[table_name]['columns'][1])] = n_EIDs
            header['checksum'] = checksum

            common_items = dict()

            for item in ['TITLE', 'SUBTITLE', 'LABEL']:
                common_items[item] = None
                unique_values = {load_case_info[item] for load_case_info in
                                 load_cases_info[table_name].values()}

                if len(unique_values) == 1:
                    common_items[item] = unique_values.pop()

                    if common_items[item]:
                        header[item] = common_items[item]

            if not all(item is None for item in common_items.values()):
                header['LOAD CASES INFO'] = dict()

                for LID in sorted(load_cases_info[table_name]):

                    if any(item for item in load_cases_info[table_name][LID].values()):
                        header['LOAD CASES INFO'][LID] = dict()

                        for item in ['TITLE', 'SUBTITLE', 'LABEL']:

                            if load_cases_info[table_name][LID][item] and common_items[item] is None:
                                header['LOAD CASES INFO'][LID][item] = load_cases_info[table_name][LID][item]

            json.dump(header, f, indent=4)

        for field_name in tables_specs[table_name]['columns'][2:]:

            with open(os.path.join(table_path, field_name + '#T.bin'), 'wb') as f:
                dtype = tables_specs[table_name]['dtypes'][field_name]
                dtype_size = np.dtype(dtype).itemsize
                field_file = os.path.join(table_path, field_name + '.bin')
                field_size = os.path.getsize(field_file)
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

        files = [field for field in tables_specs[table_name]['columns']]
        files += [field + '#T' for field in tables_specs[table_name]['columns'][2:]]
        files = [os.path.join(table_path, field + '.bin') for field in files]
        files.append(os.path.join(table_path, '#header.json'))

        for file in files:

            with open(file, 'rb') as f_in, open(os.path.splitext(file)[0] + '.' + checksum, 'wb') as f_out:
                f_out.write(hash_bytestr(f_in, get_hasher(checksum)))

    with open(os.path.join(database_path, '#header.json'), 'w') as f:

        if database_project is None:
            database_project = ''

        json.dump({'project': database_project,
                   'name': database_name,
                   'version': database_version,
                   'date': str(datetime.date.today()),
                   'tables': [table for table in tables]}, f, indent=4)

    print('Database created succesfully!')
    return DataBase(database_path)
import os
import json
import datetime
import numpy as np
from nastranpy.results.read_results import tables_in_pch
from nastranpy.results.tables_specs import get_tables_specs
from nastranpy.bdf.misc import get_hasher, hash_bytestr


def create_tables(database_path, files, tables_specs=None,
                  headers=None, load_cases_info=None,
                  table_generator=None):

    if headers is None:
        headers = dict()

    if load_cases_info is None:
        load_cases_info = dict()

    if not tables_specs:
        tables_specs = get_tables_specs()

    if not table_generator:
        table_generator = (table for file in files for table in
                           tables_in_pch(file, tables_specs))

    ignored_tables = set()

    try:

        for table in table_generator:

            if table.name not in tables_specs:

                if table.name not in ignored_tables:
                    print("WARNING: '{}' is not supported!".format(table.name))
                    ignored_tables.add(table.name)

                continue

            if table.name not in headers:
                load_cases_info[table.name] = dict()
                headers[table.name] = {
                    'name': table.name,
                    'path': os.path.join(database_path, table.name),
                    'columns': [(field, tables_specs[table.name]['dtypes'][field]) for field in
                                tables_specs[table.name]['columns']],
                    'batches': list(),
                    'LIDs': list(),
                    'IDs': None
                }

                open_table(headers[table.name], new_table=True)

            if append_to_table(table, headers[table.name]):
                load_cases_info[table.name][table.subcase] = {'TITLE': table.title,
                                                              'SUBTITLE': table.subtitle,
                                                              'LABEL': table.label}
    finally:

        for header in headers.values():
            close_table(header)

    return headers, load_cases_info


def open_table(header, new_table=False):

    if not os.path.exists(header['path']):
        os.mkdir(header['path'])

    for field, dtype in header['columns'][2:]:
        file = os.path.join(header['path'], field + '.bin')

        if new_table:
            f = open(file, 'wb')
        else:
            f = open(file, 'rb+')
            f.seek(len(header['LIDs']) * len(header['IDs']) * np.dtype(dtype).itemsize)

        if 'files' not in header:
            header['files'] = dict()

        header['files'][field] = f


def append_to_table(table, header):
    LID = table.data[header['columns'][0][0]][0]

    if LID in header['LIDs']:
        print(f'WARNING: Subcase already in the database! It will be skipped (LID: {LID})')
        return False

    header['LIDs'].append(LID)
    IDs = table.data[header['columns'][1][0]]
    index = None

    if header['IDs'] is None:
        header['IDs'] = IDs
    elif not np.array_equal(header['IDs'], IDs):
        iIDs = {ID: i for i, ID in enumerate(IDs)}
        label = header['columns'][1][0]

        try:
            index = np.array([iIDs[ID] for ID in header['IDs']])
        except KeyError:
            print(f'WARNING: Missing {label}/s! The whole subcase will be omitted (LID: {LID})')
            return False

        if len(index) < len(IDs):
            print(f'WARNING: Additional {label}/s found! These will be ommitted (LID: {LID})')

    for field, _ in header['columns'][2:]:

        if index is None:
            table.data[field].tofile(header['files'][field])
        else:
            table.data[field][index].tofile(header['files'][field])

    return True


def close_table(header):
    np.array(header['LIDs']).tofile(os.path.join(header['path'], header['columns'][0][0] + '.bin'))
    np.array(header['IDs']).tofile(os.path.join(header['path'], header['columns'][1][0] + '.bin'))

    try:

        for file in header['files'].values():
            file.close()

        del header['files']

    except KeyError:
        pass


def finalize_database(database_path, database_name, database_version, database_project,
                       headers, load_cases_info, batches, max_chunk_size, checksum='sha256'):

    for name, header in headers.items():
        create_transpose(header, max_chunk_size)
        create_table_header(header, load_cases_info[name], batches[-1][0], checksum)

    create_database_header(database_path, database_name, database_version,
                           database_project, headers, batches, checksum)


def create_transpose(header, max_chunk_size):

    for field, dtype in header['columns'][2:]:
        field_file = os.path.join(header['path'], field + '.bin')
        n_LIDs = len(header['LIDs'])
        n_IDs = len(header['IDs'])
        dtype_size = np.dtype(dtype).itemsize
        field_array = np.memmap(field_file, dtype=dtype, shape=(n_LIDs, n_IDs),
                                mode='r')
        n_IDs_per_chunk = int(max_chunk_size // (n_LIDs * dtype_size))
        n_chunks = int(n_IDs // n_IDs_per_chunk)
        n_IDs_last_chunk = int(n_IDs % n_IDs_per_chunk)

        if n_IDs != n_IDs_per_chunk * n_chunks + n_IDs_last_chunk:
            raise ValueError(f"Inconsistency found! (table: '{header['name']}', field: '{field}')")

        chunks = list()

        if n_chunks:
            chunk = np.empty((n_IDs_per_chunk, n_LIDs), dtype)
            chunks += [(chunk, n_IDs_per_chunk)] * n_chunks

        if n_IDs_last_chunk:
            last_chunk = np.empty((n_IDs_last_chunk, n_LIDs), dtype)
            chunks.append((last_chunk, n_IDs_last_chunk))

        with open(field_file, 'ab') as f:
            i0 = 0
            i1 = 0

            for chunk, n_IDs_per_chunk in chunks:
                i1 += n_IDs_per_chunk
                chunk = field_array[:, i0:i1].T
                chunk.tofile(f)
                i0 += n_IDs_per_chunk


def create_table_header(header, load_cases_info, batch_name, checksum):
    table_header = dict()
    table_header['name'] = header['name']
    table_header['columns'] = header['columns']
    set_restore_points(header, batch_name, checksum)
    table_header['batches'] = header['batches']

    common_items = dict()

    for item in ['TITLE', 'SUBTITLE', 'LABEL']:

        if item in header:
            common_items[item] = header[item]
        else:
            common_items[item] = None
            unique_values = {load_case_info[item] for load_case_info in
                             load_cases_info.values()}

            if len(unique_values) == 1:
                common_items[item] = unique_values.pop()

                if common_items[item]:
                    table_header[item] = common_items[item]

    table_header['LOAD CASES INFO'] = dict()

    for LID in sorted(load_cases_info):

        if any(item for item in load_cases_info[LID].values()):
            table_header['LOAD CASES INFO'][LID] = dict()

            for item in ['TITLE', 'SUBTITLE', 'LABEL']:

                if load_cases_info[LID][item] and load_cases_info[LID][item] != common_items[item]:
                    table_header['LOAD CASES INFO'][LID][item] = load_cases_info[LID][item]

    with open(os.path.join(header['path'], '#header.json'), 'w') as f:
        json.dump(table_header, f, indent=4)


def set_restore_points(header, batch_name, checksum):

    if header['batches'] and header['batches'][-1][0] == batch_name:
        check = True
    else:
        check = False

    if not check:
        header['batches'].append([batch_name, len(header['LIDs']), dict()])

    for field, _ in header['columns']:

        with open(os.path.join(header['path'], field + '.bin'), 'rb') as f:

            if check:

                if header['batches'][-1][2][field + '.bin'] != hash_bytestr(f, get_hasher(checksum)):
                    print(f"ERROR: '{os.path.join(header['path'], field + '.bin')} is corrupted!'")

            else:
                header['batches'][-1][2][field + '.bin'] = hash_bytestr(f, get_hasher(checksum))


def create_database_header(database_path, database_name, database_version,
                           database_project, headers, batches, checksum):

    if database_project is None:
        database_project = ''

    if batches[-1][1] is None:
        batches[-1][1] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    checksums = dict()

    for table in headers:

        with open(os.path.join(database_path, table, '#header.json'), 'rb') as f:
            checksums[table] = hash_bytestr(f, get_hasher(checksum))

    database_header = {'project': database_project,
                       'name': database_name,
                       'version': database_version,
                       'date': str(datetime.date.today()),
                       'checksum_method': checksum,
                       'checksums': checksums,
                       'batches': batches}

    database_header_file = os.path.join(database_path, '##header.json')

    with open(database_header_file, 'w') as f:
        json.dump(database_header, f, indent=4)

    with open(database_header_file, 'rb') as f_in, open(os.path.splitext(database_header_file)[0] + '.' + checksum, 'wb') as f_out:
        f_out.write(hash_bytestr(f_in, get_hasher(checksum), ashexstr=False))


def truncate_file(file, offset):

    with open(file, 'rb+') as f:
        f.seek(offset)
        f.truncate()

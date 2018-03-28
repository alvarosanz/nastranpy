import os
from nastranpy.results.read_results import tables_in_pch
from nastranpy.results.database_creation import create_tables, finalize_database
from nastranpy.results.database import Database, get_query_from_file
from nastranpy.results.tables_specs import get_tables_specs


def get_tables(files):
    tables_specs = get_tables_specs()

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


def query(query=None, file=None):

    if not query:
        query = get_query_from_file(file)

    database = Database(query['path'])

    if query['check']:
        database.check()

    df = database.query(**query)

    if query['output_path']:
        print(f"Writing '{os.path.basename(query['output_path'])}' ...")
        df.to_csv(query['output_path'])

    return df


def create_database(files, database_path, database_name, database_version,
                    database_project=None, tables_specs=None,
                    max_chunk_size=1e8, checksum='sha256', table_generator=None,
                    overwrite=False, **kwargs):
    print('Creating database ...')

    if not os.path.exists(database_path):
        os.mkdir(database_path)
    elif not overwrite:
        raise FileExistsError(f"Database already exists at '{database_path}'!")

    if isinstance(files, str):
        files = [files]

    if not tables_specs:
        tables_specs = get_tables_specs()

    if not 'filenames' in kwargs:
        filenames = [os.path.basename(file) for file in files]
    else:
        filenames = [os.path.basename(file) for file in kwargs['filenames']]

    batches = [['Initial batch', None, filenames]]
    headers, load_cases_info = create_tables(database_path, files, tables_specs,
                                             checksum=checksum, table_generator=table_generator)
    finalize_database(database_path, database_name, database_version, database_project,
                      headers, load_cases_info, batches, max_chunk_size)

    print('Database created succesfully!')
    return Database(database_path)

import os
from nastranpy.results.database import Database, get_query_from_file
from nastranpy.results.client import DatabaseClient


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


def query_server(file):

    with open(file) as f:
        query = json.load(f)

    client = DatabaseClient((query['host'], query['port']), query['path'])
    return client.request(query)



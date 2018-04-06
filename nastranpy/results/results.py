from nastranpy.results.database import Database, get_query_from_file
from nastranpy.results.client import DatabaseClient


def query(query=None, file=None):

    if not query:
        query = get_query_from_file(file)

    if 'host' in query and query['host']:
        db = DatabaseClient((query['host'], query['port']), query['path'])
    else:
        db = Database(query['path'])

    return db.query(**query)

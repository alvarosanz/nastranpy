import csv
import json


def get_query_from_file(file):

    with open(file) as f:
        query = json.load(f)

    if query['LIDs']:

        with open(query['LIDs']) as f:
            rows = list(csv.reader(f))

        if any(len(row) > 1 for row in rows):
            query['LIDs'] = {int(row[0]): [[float(row[i]), int(row[i + 1])] for i in range(1, len(row), 2)] for row in rows}
        else:
            query['LIDs'] = [int(row[0]) for row in rows]

    if query['IDs']:

        with open(query['IDs']) as f:
            rows = list(csv.reader(f))

        query['IDs'] = [int(row[0]) for row in rows]

    if query['groups']:

        with open(query['groups']) as f:
            rows = list(csv.reader(f))

        query['groups'] = {row[0]: [int(ID) for ID in row[1:]] for row in rows}

    if query['geometry']:

        with open(query['geometry']) as f:
            rows = list(csv.reader(f))

        query['geometry'] = {field: {int(row[0]): float(row[i + 1]) for row in rows} for i, field in
                             enumerate(rows[0][1:])}

    if query['weights']:

        with open(query['weights']) as f:
            query['weights'] = {int(row[0]): float(row[1]) for row in csv.reader(f)}

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

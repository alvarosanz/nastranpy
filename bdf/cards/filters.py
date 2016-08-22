from nastranpy.bdf.id_pattern import IdPattern


def filter_factory(filters):

    try:
        tags = set(filters['tags'])
    except KeyError:
        tags = None

    try:
        types = set(filters['types'])
    except KeyError:
        types = None

    try:
        names = set(filters['names'])
    except KeyError:
        names = None

    try:
        id_pattern = IdPattern(filters['id_pattern'])
    except KeyError:
        id_pattern = None

    def wrapped(card):

        if tags and not card.tag in tags:
            return False

        if types and not card.type in types:
            return False

        if names and not card.name in names:
            return False

        if id_pattern and not card.id in id_pattern:
            return False

        return True

    return wrapped

from nastranpy.bdf.cards.enums import str2type, str2tag
from nastranpy.bdf.object_handling import get_list
from nastranpy.bdf.id_pattern import IdPattern


def filter_factory(filters):

    try:
        tags = {str2tag(tag) for tag in get_list(filters['tags'])}
    except KeyError:
        tags = None

    try:
        types = {str2type(type) for type in get_list(filters['types'])}
    except KeyError:
        types = None

    try:
        names = {name for name in get_list(filters['names'])}
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

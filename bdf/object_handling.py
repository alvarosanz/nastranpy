from enum import Enum
from nastranpy.bdf.cards.card import Card
from nastranpy.bdf.include import Include


def get_list(value):

    if isinstance(value, (str, Enum, Card, Include)):
        return [value]
    else:
        return value


def get_objects(keys, mapping):
    keys = get_list(keys)

    try:
        return [mapping[key] for key in keys]
    except KeyError:
        return keys

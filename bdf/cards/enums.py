from enum import Enum


class Item(Enum):
    coord = 1
    elem = 2
    grid = 3
    mat = 4
    prop = 5

class Set(Enum):
    mpc = 6
    spc = 7
    load = 8

class Tag(Enum):
    e0D = 1
    e1D = 2
    e2D = 3
    e3D = 4
    eRigid = 5
    eSpring = 6
    ePlot = 7

class Coord(Enum):
    rectangular = 1
    cylindrical = 2
    spherical = 3


def str2type(value):
    mapping = {card_type.name: card_type for card_type in list(Item) + list(Set)}

    try:
        return mapping[value]
    except KeyError:

        if not value in Item and not value in Set:
            raise KeyError(value)

        return value

def str2tag(value):

    try:
        return Tag[value]
    except KeyError:

        if not value in Tag:
            raise KeyError(value)

        return value

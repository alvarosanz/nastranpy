from enum import Enum
from nastran_tools.bdf.write_bdf import print_card


class Item(Enum):
    grid = 'grid'
    elem = 'elem'
    prop = 'prop'
    mat = 'mat'
    coord = 'coord'
    load = 'load'
    spc = 'spc'
    mpc = 'mpc'


def iterate_items_factory(item_type):

    def wrapped(self):

        for field in self.fields:

            if isinstance(field, Card):

                if field.item_type is item_type:
                    yield field

    return wrapped


class Card(object):

    def __init__(self, fields, large_field=False, free_field=False):
        self.fields = list(fields)
        self.large_field = large_field
        self.free_field = free_field
        self.is_commented = False
        self.comment = ''
        self.print_comment = True
        self.item_type = None
        self.include = None

    def __repr__(self):
        return "'{} {}: {}'".format(self.type, self.id, super().__repr__())

    def __str__(self):

        if self.print_comment:
            comment = self.comment
        else:
            comment = ''

        return print_card(self.get_fields(), large_field=self.large_field, free_field=self.free_field,
                          comment=self.comment, is_commented=self.is_commented)

    @property
    def type(self):
        return self.fields[0]

    @property
    def id(self):
        return self.fields[1]

    @id.setter
    def id(self, value):
        self.fields[1] = int(value)

    def __getitem__(self, index):

        try:
            return self.fields[index]
        except IndexError:
            return ''

    def __setitem__(self, index, value):
        try:
            index0 = index.start
        except AttributeError:
            index0 = index

        try:
            self.fields[index0] = value
        except IndexError:
            self.fields += ['' for x in range(index0 + 1 - len(self.fields))]
            self.fields[index] = value

    def clear_tail(self):

        for last_index in range(len(self.fields) - 1, -1, -1):

            if self.fields[last_index] != '':
                break

        del self.fields[last_index + 1:]

    def get_fields(self):
        fields = list(self.fields)

        for index, field in enumerate(fields):

            if field != '':
                last_index = index

                if isinstance(field, Card):
                    fields[index] = field.id

        return fields[:last_index + 1]


for item_type in Item:
    setattr(Card, item_type.name + 's', iterate_items_factory(item_type))

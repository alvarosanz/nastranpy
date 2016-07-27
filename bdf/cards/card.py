import numpy as np
from nastranpy.bdf.write_bdf import print_card
from nastranpy.bdf.cards.enums import Item, Seq
from nastranpy.bdf.cards.grid_list import GridList
from nastranpy.bdf.cards.grid_set import GridSet


class Card(object):
    type = None
    tag = None
    scheme = None
    padding = None

    def __init__(self, fields, large_field=False, free_field=False):

        if self.padding:
            fields = self.padding.unpadded(fields)

        self._set_fields(fields)
        self.large_field = large_field
        self.free_field = free_field
        self.is_commented = False
        self.comment = ''
        self.print_comment = True
        self._include = None
        self.notify = None

    def __repr__(self):
        return "'{} {}: {}'".format(self.name, self.id, super().__repr__())

    def __str__(self):
        return print_card(self.get_fields(), large_field=False, free_field=False,
                          comment='', is_commented=False)

    def print_card(self, large_field=None, free_field=None, comment=None,
                   is_commented=None, comment_symbol='$: '):

        if large_field is None:
            large_field = self.large_field

        if free_field is None:
            free_field = self.free_field

        if comment is None:

            if self.print_comment:
                comment = self.comment
            else:
                comment = ''

        if is_commented is None:
            is_commented = self.is_commented

        return print_card(self.get_fields(), large_field=large_field, free_field=free_field,
                          comment=comment, is_commented=is_commented, comment_symbol=comment_symbol)

    @property
    def name(self):
        return self.fields[0]

    @property
    def id(self):
        return self.fields[1]

    @id.setter
    def id(self, value):

        if self.fields[1] != value:

            if self.notify:
                self.notify(self, new_id=value)

            self.fields[1] = int(value)

    def __contains__(self, value):
        return value in self.items()

    def items(self):
        return (field for field, field_type in self._get_fields() if
                field and field_type in Item)

    def _get_fields(self):

        if self.scheme:

            for field, field_info in zip(self.fields, self.scheme):

                if field_info.seq_type:

                    for subfield in field:

                        if field_info.subscheme:

                            for subsubfield, subsubfield_info in zip(subfield, field_info.subscheme.scheme):

                                if subsubfield_info.seq_type:

                                    for subsubsubfield in subsubfield:
                                        yield subsubsubfield, subsubfield_info.type
                                else:
                                    yield subsubfield, subsubfield_info.type
                        else:
                            yield subfield, field_info.type
                else:
                    yield field, field_info.type

        else:

            for field in self.fields:
                yield field, None

    def _set_fields(self, fields):
        fields = [field if field != '' else None for field in fields]

        while True:

            if fields[-1] is None:
                fields.pop()
            else:
                break

        if self.scheme:
            fields.reverse()
            self.fields = list()
            link_grids = None

            for field_info in self.scheme:

                if link_grids is None and field_info.update_grid:
                    link_grids = isinstance(fields[-1], Card)

                if field_info.seq_type:
                    subfields = list()

                    for i in range(1, len(fields) + 1):

                        if field_info.subscheme:
                            subfield = list()

                            for subsubfield_info in field_info.subscheme.scheme:

                                if subsubfield_info.seq_type:
                                    subfield.append(list())

                                    for j in range(1, len(fields) + 1):
                                        subfield[-1].append(fields.pop())

                                        if (not fields or
                                            j == subsubfield_info.length or
                                            not subsubfield_info.length and isinstance(fields[-1], (float, str))):
                                            break

                                else:

                                    try:
                                        subfield.append(fields.pop())
                                    except IndexError:
                                        subfield.append(None)

                            subfield = field_info.subscheme(subfield, self)
                        else:
                            subfield = fields.pop()

                        subfields.append(subfield)

                        if (not fields or
                            i == field_info.length or
                            not field_info.length and isinstance(fields[-1], (float, str))):
                            break

                    if field_info.seq_type is Seq.list:

                        if field_info.update_grid and link_grids:
                            field = GridList(self, grids=subfields)
                        else:
                            field = list(subfields)

                    elif field_info.seq_type is Seq.set:

                        if field_info.update_grid and link_grids:
                            field = GridSet(self, grids=subfields)
                        else:
                            field = set(subfields)
                    elif field_info.seq_type is Seq.vector:
                        field = np.array(subfields)

                else:

                    try:
                        field = fields.pop()

                        if field_info.update_grid and link_grids:
                            field.elems.add(self)
                    except IndexError:
                        field = None

                self.fields.append(field)
        else:
            self.fields = fields

    def get_fields(self):
        fields = ['' if field is None else field for field, field_type in self._get_fields()]

        if self.padding:
            fields = self.padding.padded(fields)

        for index, field in enumerate(fields):

            if field != '':
                last_index = index

                try:
                    fields[index] = field.id
                except AttributeError:
                    pass

        return fields[:last_index + 1]

    @property
    def include(self):
        return self._include

    @include.setter
    def include(self, value):

        if not self._include is value:

            if self._include:
                self._include.cards.remove(self)

            self._include = value

            if self._include:
                self._include.cards.add(self)

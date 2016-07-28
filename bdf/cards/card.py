import numpy as np
from nastranpy.bdf.write_bdf import print_card
from nastranpy.bdf.cards.enums import Item, Seq
from nastranpy.bdf.cards.grid_list import GridList
from nastranpy.bdf.cards.grid_set import GridSet
from nastranpy.bdf.cards.field_info import FieldInfo


class Card(object):
    type = None
    tag = None
    scheme = None
    optional_scheme = None
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

    def __contains__(self, value):
        return value in self.items()

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

    def items(self):
        return (field for field, field_info.type in self._get_fields() if
                field and field_info and field_info.type in Item)

    def get_fields(self):

        if self.padding:
            fields = ['' if field is None else field for field in self.padding.padded(self._get_fields())]
        else:
            fields = ['' if field is None else field for field, field_info in self._get_fields()]

        for index, field in enumerate(fields):

            if field != '':
                last_index = index

                try:
                    fields[index] = field.id
                except AttributeError:
                    pass

        return fields[:last_index + 1]

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

    def _get_fields(self):

        if self.scheme:

            for field, field_info in zip(self.fields, self.scheme):

                if field_info.optional:

                    if field is None:
                        continue
                    else:
                        yield field_info.name, FieldInfo(field_info.name, optional_flag=True)

                if field_info.seq_type:

                    for subfield in field:

                        if field_info.subscheme:

                            for subsubfield, subsubfield_info in zip(subfield, field_info.subscheme.scheme):

                                if subsubfield_info.seq_type:

                                    for subsubsubfield in subsubfield:
                                        yield subsubsubfield, subsubfield_info
                                else:
                                    yield subsubfield, subsubfield_info
                        else:
                            yield subfield, field_info

                elif field_info.subscheme:

                    for subfield, subfield_info in zip(field, field_info.subscheme.scheme):

                        if subfield_info.seq_type:

                            for subsubfield in subfield:
                                yield subsubfield, subfield_info
                        else:
                            yield subfield, subfield_info
                else:
                    yield field, field_info

        else:

            for field in self.fields:
                yield field, None

    def _set_fields(self, fields):

        def get_subscheme(subscheme):
            field = list()

            for field_info in subscheme.scheme:

                if field_info.seq_type:
                    field.append(list())

                    for i in range(1, len(fields) + 1):
                        field[-1].append(fields.pop())

                        if (not fields or
                            i == field_info.length or
                            not field_info.length and isinstance(fields[-1], (float, str))):
                            break
                else:

                    try:
                        field.append(fields.pop())
                    except IndexError:
                        field.append(None)

            return subscheme(field, self)

        def get_field(field_info):

            if field_info.seq_type:
                subfields = list()

                for i in range(1, len(fields) + 1):

                    if field_info.subscheme:
                        subfields.append(get_subscheme(field_info.subscheme))
                    else:
                        subfields.append(fields.pop())

                    if (not fields or
                        i == field_info.length or
                        not field_info.length and isinstance(fields[-1], (float, str))):
                        break

                if field_info.seq_type is Seq.list:

                    if field_info.update_grid:
                        field = GridList(self, grids=subfields)
                    else:
                        field = list(subfields)

                elif field_info.seq_type is Seq.set:

                    if field_info.update_grid:
                        field = GridSet(self, grids=subfields)
                    else:
                        field = set(subfields)
                elif field_info.seq_type is Seq.vector:
                    field = np.array(subfields)

            elif field_info.subscheme:
                field = get_subscheme(field_info.subscheme)
            else:

                try:
                    field = fields.pop()

                    if field_info.update_grid:

                        try:
                            field.elems.add(self)
                        except AttributeError:
                            pass

                except IndexError:
                    field = None

            return field


        fields = [field if field != '' else None for field in fields]

        while True:

            if fields[-1] is None:
                fields.pop()
            else:
                break

        if self.scheme:
            fields.reverse()
            self.fields = [None if field_info.optional else get_field(field_info) for
                           field_info in self.scheme]

            if self.optional_scheme:

                while fields:
                    field = fields.pop()

                    if field in self.optional_scheme:
                        index, optional_field_info = self.optional_scheme[field]
                        self.fields[index] = get_field(optional_field_info)
        else:
            self.fields = fields

import numpy as np
from nastranpy.bdf.observable import Observable
from nastranpy.bdf.write_bdf import print_card
from nastranpy.bdf.cards.enums import Item, Seq
from nastranpy.bdf.cards.card_list import CardList
from nastranpy.bdf.cards.card_set import CardSet
from nastranpy.bdf.cards.field_info import FieldInfo


class Card(Observable):
    type = None
    tag = None
    scheme = None
    optional_scheme = None
    padding = None

    def __init__(self, fields, large_field=False, free_field=False):
        super().__init__()

        if self.padding:
            fields = self.padding.unpadded(fields)

        self.fields = [field if field != '' else None for field in fields]

        while True:

            if self.fields[-1] is None:
                self.fields.pop()
            else:
                break

        self.large_field = large_field
        self.free_field = free_field
        self.is_commented = False
        self.comment = ''
        self.print_comment = True
        self._include = None

    def __repr__(self):
        return "'{} {}'".format(self.name, self.id)

    def __str__(self):
        fields = ['name: {}'.format(repr(self.name)),
                 'id: {}'.format(self.fields[1])]

        if self.type:
            fields.append('type: {}'.format(self.type.name))

        if self.tag:
            fields.append('tag: {}'.format(self.tag.name))

        if self.scheme:
            fields += ['{}: {}'.format(field_info.name, repr(field)) for
                       field_info, field in zip(self.scheme[2:], self.fields[2:]) if
                       field_info.name]
        else:
            fields += [repr(field) for field in self.fields[2:]]

        return '{{{}}}'.format(', '.join(fields))

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
            self.changed = True
            self.notify(new_id=value)
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

    def update(self, caller, **kwargs):
        pass

    def items(self):
        return (field for field, field_info in self._get_fields() if
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

    def print(self, large_field=None, free_field=None, comment=None, is_commented=None, comment_symbol='$: '):

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

    def process_fields(self, items=None):

        def get_subscheme(subscheme, items):
            field = list()

            for field_info in subscheme.scheme:

                if field_info.seq_type:
                    field.append(list())

                    for i in range(1, len(fields) + 1):
                        f = fields.pop()
                        field[-1].append(items[field_info.type][f] if items and field_info.type and f and isinstance(f, int) else f)

                        if (not fields or
                            i == field_info.length or
                            not field_info.length and isinstance(fields[-1], (float, str))):
                            break
                else:

                    try:
                        f = fields.pop()
                        field.append(items[field_info.type][f] if items and field_info.type and f and isinstance(f, int) else f)
                    except IndexError:
                        field.append(None)

            return subscheme(field, self)

        def get_field(field_info, items):

            if field_info.seq_type:
                subfields = list()

                for i in range(1, len(fields) + 1):

                    if field_info.subscheme:
                        subfields.append(get_subscheme(field_info.subscheme, items))
                    else:
                        f = fields.pop()
                        subfields.append(items[field_info.type][f] if items and field_info.type and f and isinstance(f, int) else f)

                    if (not fields or
                        i == field_info.length or
                        not field_info.length and isinstance(fields[-1], (float, str))):
                        break

                if field_info.seq_type is Seq.list:

                    if field_info.observed:
                        field = CardList(self, cards=subfields, update_grid=field_info.update_grid)
                    else:
                        field = list(subfields)

                elif field_info.seq_type is Seq.set:

                    if field_info.observed:
                        field = CardSet(self, cards=subfields, update_grid=field_info.update_grid)
                    else:
                        field = set(subfields)

                elif field_info.seq_type is Seq.vector:
                    field = np.array(subfields)

            elif field_info.subscheme:
                field = get_subscheme(field_info.subscheme, items)
            else:

                try:
                    f = fields.pop()
                    field = items[field_info.type][f] if items and field_info.type and f and isinstance(f, int) else f

                    if field_info.observed:

                        try:
                            field.subscribe(self)

                            if field_info.update_grid:
                                field.elems.add(self)
                        except AttributeError:
                            pass

                except IndexError:
                    field = None

            return field

        if self.scheme:
            fields = list(reversed(self.fields))
            self.fields = [None if field_info.optional else get_field(field_info, items) for
                           field_info in self.scheme]

            if self.optional_scheme:

                while fields:
                    field = fields.pop()

                    if field in self.optional_scheme:
                        index, optional_field_info = self.optional_scheme[field]
                        self.fields[index] = get_field(optional_field_info, items)

    def split(self):

        if self.scheme and self.scheme[-1].other_card:
            index = len(self.scheme) - 1

            if len(self.fields) > index and self.fields[index]:
                new_card = self._new_card([self.fields[0]] + self.fields[index:])
                new_card.split()
                del self.fields[index:]

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

    def _new_card(self, fields):
        new_card = type(self)(fields, large_field=self.large_field, free_field=self.free_field)
        new_card.is_commented = self.is_commented
        new_card.comment = self.comment
        new_card.print_comment = self.print_comment
        new_card.include = self.include
        new_card.observers = self.observers.copy()
        new_card.changed = True
        new_card.notify(new_card=new_card)
        return new_card

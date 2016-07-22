from nastranpy.bdf.write_bdf import print_card


class Card(object):
    type = None
    tag = None
    scheme = None

    def __init__(self, fields, large_field=False, free_field=False):
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
        return value in self._iter_fields()

    def _iter_fields(self):

        if self.scheme:

            for field, field_info in zip(self.fields, scheme):

                if field_info.seq_type:

                    for subfield in field:

                        if field_info.subscheme:

                            for subsubfield in subfield:
                                yield subsubfield
                        else:
                            yield subfield
                else:
                    yield field
        else:

            for field in self.fields:
                yield field

    def _get_fields(self):

        for field, field_info in zip(self.fields, scheme):

            if field_info.seq_type:

                for subfield in field:

                    if field_info.subscheme:

                        for subsubfield, subfield_info in (subfield, field_info.subscheme):
                            yield subsubfield, subfield_info
                    else:
                        yield subfield, field_info.type
            else:
                yield field, field_info.type if isinstance(field, (int, Card)) else None

    def _set_fields(self, fields):
        self.fields = [field if field != '' else None for field in fields]

    def get_fields(self):
        fields = ['' if field is None else field for field in self._iter_fields()]

        for index, field in enumerate(fields):

            if field != '':
                last_index = index

                if isinstance(field, Card):
                    fields[index] = field.id

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

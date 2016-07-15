from nastran_tools.bdf.write_bdf import print_card


class Card(object):
    type = None
    tag = None

    def __init__(self, fields, large_field=False, free_field=False):
        self.fields = list(fields)
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

            if not self.fields[last_index] in ('', None):
                break

        del self.fields[last_index + 1:]

    def get_fields(self):
        fields = list(self.fields)

        for index, field in enumerate(fields):

            if not field in ('', None):
                last_index = index

                if isinstance(field, Card):
                    fields[index] = field.id

            if field is None:
                fields[index] = ''

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
            self._include.cards.add(self)

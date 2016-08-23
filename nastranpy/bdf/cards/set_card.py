from nastranpy.bdf.cards.card import Card


class SetCard(Card):

    def __init__(self, fields, large_field=False, free_field=False):
        super().__init__(fields, large_field=large_field, free_field=free_field)
        self._set = None

    @property
    def id(self):
        return self.fields[1]

    @id.setter
    def id(self, value):

        if self.fields[1] != value:

            if self._set:
                self._set.id = value

    @property
    def set(self):
        return self._set

    @set.setter
    def set(self, value):

        if not self._set is value:

            if self._set:
                self._set.cards.remove(self)

            self._set = value

            if self._set:
                self._set.cards.add(self)

    def _new_card(self, fields):
        new_card = super()._new_card(fields)
        new_card.set = self.set
        return new_card

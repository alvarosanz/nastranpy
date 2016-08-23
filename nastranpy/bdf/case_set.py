from nastranpy.bdf.observable import Observable


class CaseSet(Observable):

    def __init__(self, id, type):
        super().__init__()
        self._id = id
        self._type = type
        self.clear()

    def clear(self):
        self.cards = set()

    def __repr__(self):
        return "'{} set {}'".format(self.type.upper(), self.id)

    def __str__(self):
        return '{} set {}'.format(self.type.upper(), self.id)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):

        if self._id != value:
            self.changed = True
            self._notify(new_id=value)
            self._id = value

            for card in self.cards:
                card.fields[1] = int(value)

    @property
    def type(self):
        return self._type

class CardSet(object):

    def __init__(self, observer, cards=None, update_grid=False):
        self._observer = observer
        self._cards = set()
        self._update_grid = update_grid

        if cards:

            for card in cards:

                if card:
                    self.add(card)

    def __repr__(self):
        return repr(self._cards)

    def __str__(self):
        return str(self._cards)

    def __len__(self):
        return len(self._cards)

    def __iter__(self):
        return iter(self._cards)

    def __contains__(self, value):
        return value in self._cards

    def __ior__(self, other):

        for card in other:
            self.add(card)

    def add(self, value):

        try:
            value._subscribe(self._observer)

            if self._update_grid:
                value.elems.add(self._observer)
        except AttributeError:
            pass

        self._cards.add(value)

    def remove(self, value):

        try:
            value._unsubscribe(self._observer)

            if self._update_grid:
                value.elems.remove(self._observer)
        except AttributeError:
            pass

        self._cards.remove(value)

    def clear(self):

        for card in self._cards:

            try:
                card._unsubscribe(self._observer)

                if self._update_grid:
                    card.elems.remove(self._observer)
            except AttributeError:
                pass

        self._cards.clear()

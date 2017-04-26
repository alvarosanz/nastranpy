class CardList(object):

    def __init__(self, observer, cards=None, update_grid=False):
        self._observer = observer
        self._cards = list()
        self._update_grid = update_grid

        if cards:

            for card in cards:
                self.append(card)

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

    def __getitem__(self, index):
        return self._cards[index]

    def __setitem__(self, index, value):
        old_value = self._cards[index]

        if not value is old_value:

            try:
                old_value._unsubscribe(self._observer)

                if self._update_grid:
                    old_value.elems.remove(self._observer)
            except AttributeError:
                pass

            try:
                value._subscribe(self._observer)

                if self._update_grid:
                    value.elems.add(self._observer)
            except AttributeError:
                pass

            self._cards[index] = value

    def __delitem__(self, index):

        try:
            self._cards[index]._unsubscribe(self._observer)

            if self._update_grid:
                self._cards[index].elems.remove(self._observer)
        except AttributeError:
            pass

        del self._cards[index]

    def __iadd__(self, other):

        for card in other:
            self.append(card)

    def append(self, value):

        try:
            value._subscribe(self._observer)

            if self._update_grid:
                value.elems.add(self._observer)
        except AttributeError:
            pass

        self._cards.append(value)

    def clear(self):

        for card in self._cards:

            try:
                card._unsubscribe(self._observer)

                if self._update_grid:
                    card.elems.remove(self._observer)
            except AttributeError:
                pass

        self._cards.clear()

    def index(self, value):
        return self._cards.index(value)

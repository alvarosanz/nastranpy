class GridList(object):

    def __init__(self, element, grids=None):
        self._element = element
        self._grids = list()

        if grids:

            for grid in grids:
                self.append(grid)

    def __len__(self):
        return len(self._grids)

    def __iter__(self):
        return iter(self._grids)

    def __contains__(self, value):
        return value in self._grids

    def __getitem__(self, index):
        return self._grids[index]

    def __setitem__(self, index, value):
        old_value = self._grids[index]

        if not value is old_value:

            try:
                old_value.elems.remove(self._element)
            except AttributeError:
                pass

            try:
                value.elems.add(self._element)
            except AttributeError:
                pass

            self._grids[index] = value

    def __delitem__(self, index):

        try:
            self._grids[index].elems.remove(self._element)
        except AttributeError:
            pass

        del self._grids[index]

    def __iadd__(self, other):

        for grid in other:
            self.append(grid)

    def append(self, value):

        try:
            value.elems.add(self._element)
        except AttributeError:
            pass

        self._grids.append(value)

    def clear(self):

        for grid in self._grids:

            try:
                grid.elems.remove(self._element)
            except AttributeError:
                pass

        self._grids.clear()

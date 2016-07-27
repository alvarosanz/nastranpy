class GridSet(object):

    def __init__(self, element, grids=None):
        self._element = element
        self._grids = set()

        if grids:

            for grid in grids:

                if grid:
                    self.add(grid)

    def __len__(self):
        return len(self._grids)

    def __iter__(self):
        return iter(self._grids)

    def __contains__(self, value):
        return value in self._grids

    def __ior__(self, other):

        for grid in other:
            self.add(grid)

    def add(self, value):

        try:
            value.elems.add(self._element)
        except AttributeError:
            pass

        self._grids.add(value)

    def remove(self, value):

        try:
            value.elems.remove(self._element)
        except AttributeError:
            pass

        self._grids.remove(value)

    def clear(self):

        for grid in self._grids:

            try:
                grid.elems.remove(self._element)
            except AttributeError:
                pass

        self._grids.clear()

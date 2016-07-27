class GridSet(object):

    def __init__(self, element, grids=None):
        self.element = element
        self.grids = set()

        if grids:

            for grid in grids:

                if grid:
                    self.add(grid)

    def __len__(self):
        return len(self.grids)

    def __iter__(self):
        return iter(self.grids)

    def __contains__(self, value):
        return value in self.grids

    def __ior__(self, other):

        for grid in other:
            self.add(grid)

    def add(self, value):
        value.elems.add(self.element)
        self.grids.add(value)

    def remove(self, value):
        value.elems.remove(self.element)
        self.grids.remove(value)

    def clear(self):

        for grid in self.grids:
            grid.elems.remove(self.element)

        self.grids.clear()

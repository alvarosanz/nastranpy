class GridList(object):

    def __init__(self, element, grids=None):
        self.element = element
        self.grids = list()

        if grids:

            for grid in grids:
                self.append(grid)

    def __len__(self):
        return len(self.grids)

    def __iter__(self):
        return iter(self.grids)

    def __contains__(self, value):
        return value in self.grids

    def __getitem__(self, index):
        return self.grids[index]

    def __setitem__(self, index, value):
        old_value = self.grids[index]

        if not value is old_value:

            if old_value:
                old_value.elems.remove(self.element)

            if value:
                value.elems.add(self.element)

            self.grids[index] = value

    def __delitem__(self, index):
        old_value = self.grids[index]

        if old_value:
            old_value.elems.remove(self.element)

        del self.grids[index]

    def __iadd__(self, other):

        for grid in other:
            self.append(grid)

    def append(self, value):
        value.elems.add(self.element)

        if value:
            value.elems.add(self.element)

        self.grids.append(value)

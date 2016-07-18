import numpy as np
from nastran_tools.bdf.cards.card_interfaces import Item, Set
from nastran_tools.bdf.cards.class_factory import class_factory


GRID = class_factory(Item.grid, ['GRID', None, [Item.coord, 'CP'], [None, 'X1'], [None, 'X2'], [None, 'X3'], [Item.coord, 'CD'], [None, 'PS'], [None, 'SEID']])

class Grid(GRID):
    type = Item.grid

    def __init__(self, fields, large_field=False, free_field=False):
        super().__init__(fields, large_field=large_field, free_field=free_field)
        self.xyz0 = None
        self.elems = set()


#    @property
#    def xyz(self):
#
#        if self.cp:
#            return self.cp.get_xyz(self.xyz0)
#        else:
#            return self.xyz0
#
#    @xyz.setter
#    def xyz(self, value):
#        self._card[3:6] = value
#
#        if self.cp:
#            self.xyz0 = self.cp.get_xyz0(value)
#        else:
#            self.xyz0 = value

from nastranpy.bdf.cards.enums import Coord
from nastranpy.bdf.cards.coord_system import CoordSystem
from nastranpy.bdf.cards.card import Card


def update_fields(func):

    def wrapped(self):

        if self.fields[0][-2] == '2':
            cp = self.fields[2]

            if cp:

                try:
                    self.fields[3] = cp.get_xyz(self.a0)
                    self.fields[4] = cp.get_xyz(self.b0)
                    self.fields[5] = cp.get_xyz(self.c0)
                except AttributeError:
                    pass

        return func(self)

    return wrapped


class CoordCard(Card):

    def __init__(self, fields, large_field=False, free_field=False):
        super().__init__(fields, large_field=large_field, free_field=free_field)
        char2type = {'R': Coord.rectangular, 'C': Coord.cylindrical, 'S': Coord.spherical}
        self.coord_type = char2type[self.fields[0][-1]]
        self.a0 = None
        self.b0 = None
        self.c0 = None
        self.origin = None
        self.M = None

    @update_fields
    def __str__(self):
        return super().__str__()

    def settle(self):

        try:

            if self.fields[0][-2] == '1':
                self.a0 = self.fields[2].xyz0
                self.b0 = self.fields[3].xyz0
                self.c0 = self.fields[4].xyz0
            else:
                self.a0 = self.fields[3]
                self.b0 = self.fields[4]
                self.c0 = self.fields[5]
                cp = self.fields[2]

                if cp:
                    self.a0 = cp.get_xyz0(self.a0)
                    self.b0 = cp.get_xyz0(self.b0)
                    self.c0 = cp.get_xyz0(self.c0)
        except AttributeError:
            self.log.error('Cannot settle {}'.format(repr(self)))

        self.compute_matrix(self.a0, self.b0, self.c0)

    def update(self, caller, **kwargs):
        super().update(caller, **kwargs)

        for key, value in kwargs.items():

            if key == 'grid_changed':
                self.settle()

    @update_fields
    def get_fields(self):
        return super().get_fields()

CoordCard.compute_matrix = CoordSystem.compute_matrix
CoordCard.get_xyz = CoordSystem.get_xyz
CoordCard.get_xyz0 = CoordSystem.get_xyz0

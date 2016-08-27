import numpy as np
from nastranpy.bdf.cards.coord_system import CoordSystem
from nastranpy.bdf.cards.card import Card


def update_fields(func):

    def wrapped(self):

        if self._is_processed and self.fields[0][-2] == '2':
            cp = self.fields[2]

            if cp:

                try:
                    self.fields[3] = cp.get_xyz(self._A)
                    self.fields[4] = cp.get_xyz(self._B)
                    self.fields[5] = cp.get_xyz(self._C)
                except AttributeError:
                    pass
            else:
                self.fields[3] = self._A
                self.fields[4] = self._B
                self.fields[5] = self._C

        return func(self)

    return wrapped


class CoordCard(Card):

    def __init__(self, fields, large_field=False, free_field=False):
        super().__init__(fields, large_field=large_field, free_field=free_field)
        self.coord_type = self.fields[0][-1]
        self._A = None
        self._B = None
        self._C = None
        self._origin = None
        self._M = None

    @update_fields
    def __str__(self):
        return super().__str__()

    def _settle(self):

        try:

            if self.fields[0][-2] == '1':
                self._A = self.fields[2].xyz0
                self._B = self.fields[3].xyz0
                self._C = self.fields[4].xyz0
            else:
                self._A = self.fields[3]
                self._B = self.fields[4]
                self._C = self.fields[5]
                cp = self.fields[2]

                if cp:
                    self._A = cp.get_xyz0(self._A)
                    self._B = cp.get_xyz0(self._B)
                    self._C = cp.get_xyz0(self._C)
        except AttributeError:
            self._log.error('Cannot settle {}'.format(repr(self)))

        self.compute_matrix(self._A, self._B, self._C)

    @property
    def origin(self):
        return self._origin

    @property
    def M(self):
        return self._M

    @property
    def A(self):
        return self._A

    @property
    def B(self):
        return self._B

    @property
    def C(self):
        return self._C

    def set_ABC(self, A, B, C):

        if self.fields[0][-2] == '1':
            raise TypeError('Cannot change {} cards this way'.format(self.fields[0]))

        self._A = A
        self._B = B
        self._C = C
        self.compute_matrix(self._A, self._B, self._C)
        self.changed = True
        self._notify(coord_changed=self)

    def invert_axes(self, invert_x=False, invert_y=False, invert_z=False):

        if sum((1 if invert else 0 for invert in [invert_x, invert_y, invert_z])) in [1, 3]:
            raise ValueError('Two axes must be inverted (or none at all)')

        if invert_z:
            B_new = np.array([0.0, 0.0, -1.0])
        else:
            B_new = np.array([0.0, 0.0, 1.0])

        if invert_x:
            C_new = np.array([-1.0, 0.0, 0.0])
        else:
            C_new = np.array([1.0, 0.0, 0.0])

        B_new *= 10000.0
        C_new *= 10000.0
        self.set_ABC(self.A, self.get_xyz0(B_new), self.get_xyz0(C_new))

    def rotate(self, axis='x', angle=0.0):
        angle = np.radians(angle)
        sin = np.sin(angle)
        cos = np.cos(angle)

        if axis == 'x':
            B_new = np.array([0.0, -sin, cos])
            C_new = np.array([1.0, 0.0, 0.0])
        elif axis == 'y':
            B_new = np.array([sin, 0.0, cos])
            C_new = np.array([cos, 0.0, -sin])
        elif axis == 'z':
            B_new = np.array([0.0, 0.0, 1.0])
            C_new = np.array([cos, sin, 0.0])

        B_new *= 10000.0
        C_new *= 10000.0
        self.set_ABC(self.A, self.get_xyz0(B_new), self.get_xyz0(C_new))

    def _update(self, caller, **kwargs):
        super()._update(caller, **kwargs)

        for key, value in kwargs.items():

            if key == 'grid_changed':
                self._settle()

    @update_fields
    def get_fields(self):
        return super().get_fields()

CoordCard.compute_matrix = CoordSystem.compute_matrix1
CoordCard.get_xyz = CoordSystem.get_xyz
CoordCard.get_xyz0 = CoordSystem.get_xyz0

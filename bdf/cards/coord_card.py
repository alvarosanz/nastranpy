import numpy as np
from nastranpy.bdf.cards.enums import Coord
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

        self.origin = self.a0
        e2 = np.cross(self.b0 - self.a0, self.c0 - self.a0)
        e2 /= np.linalg.norm(e2)
        e1 = np.cross(e2, self.b0 - self.a0)
        e1 /= np.linalg.norm(e1)
        e3 = np.cross(e1, e2)
        self.M = np.array([e1, e2, e3])

    def update(self, caller, **kwargs):
        super().update(caller, **kwargs)

        for key, value in kwargs.items():

            if key == 'grid_changed':
                self.settle()

    def get_xyz(self, xyz0, is_vector=False):

        if not is_vector:
            xyz0 = xyz0 - self.origin

        xyz = np.dot(xyz0, self.M.T)

        if self.coord_type is Coord.cylindrical:
            return np.array(cart2cyl(*xyz))
        elif self.coord_type is Coord.spherical:
            return np.array(cart2sph(*xyz))

        return xyz

    def get_xyz0(self, xyz, is_vector=False):

        if self.coord_type is Coord.cylindrical:
            xyz = cyl2cart(*xyz)
        elif self.coord_type is Coord.spherical:
            xyz = sph2cart(*xyz)

        if is_vector:
            return np.dot(xyz, self.M)
        else:
            return self.origin + np.dot(xyz, self.M)

    @update_fields
    def get_fields(self):
        return super().get_fields()


def cart2cyl(x, y, z):
    theta = np.arctan2(y, x)
    rho = np.hypot(x, y)
    return rho, theta, z

def cyl2cart(rho, theta, z):
    x = rho * np.cos(theta)
    y = rho * np.sin(theta)
    return x, y, z

def cart2sph(x, y, z):
    hxy = np.hypot(x, y)
    r = np.hypot(hxy, z)
    el = np.arctan2(z, hxy)
    az = np.arctan2(y, x)
    return r, az, el

def sph2cart(r, az, el):
    rcos_theta = r * np.cos(el)
    x = rcos_theta * np.cos(az)
    y = rcos_theta * np.sin(az)
    z = r * np.sin(el)
    return x, y, z

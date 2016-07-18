import numpy as np
from nastranpy.bdf.cards.enums import Coord
from nastranpy.bdf.cards.card import Card


class CoordCard(Card):

    def __init__(self, fields, large_field=False, free_field=False):
        super().__init__(fields, large_field=large_field, free_field=free_field)
        char2type = {'R': Coord.rectangular, 'C': Coord.cylindrical, 'S': Coord.spherical}
        self.coord_type = char2type[self.fields[0][-1]]
        self.origin = None
        self.M = None

    def update(self):

        try:
            v0 = self.fields[2].xyz0
            v1 = self.fields[3].xyz0
            v2 = self.fields[4].xyz0
        except AttributeError:
            v0 = np.array(self.fields[3:6])
            v1 = np.array(self.fields[6:9])
            v2 = np.array(self.fields[9:12])
            cp = self.fields[2]

            if cp:
                v0 = cp.get_xyz0(v0)
                v1 = cp.get_xyz0(v1)
                v2 = cp.get_xyz0(v2)

        self.origin = v0
        e2 = np.cross(v1 - v0, v2 - v0)
        e2 /= np.linalg.norm(e2)
        e1 = np.cross(e2, v1 - v0)
        e1 /= np.linalg.norm(e1)
        e3 = np.cross(e1, e2)
        self.M = np.array([e1, e1, e3])

    def get_xyz(self, xyz0):
        xyz = np.dot(xyz0 - self.origin, self.M.T)

        if self.coord_type is Coord.cylindrical:
            return np.array(cart2cyl(*xyz))
        elif self.coord_type is Coord.spherical:
            return np.array(cart2sph(*xyz))

        return xyz

    def get_xyz0(self, xyz):

        if self.coord_type is Coord.cylindrical:
            xyz = cyl2cart(*xyz)
        elif self.coord_type is Coord.spherical:
            xyz = sph2cart(*xyz)

        return self.origin + np.dot(xyz, self.M)


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

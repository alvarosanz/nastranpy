import numpy as np


class CoordSystem(object):

    def __init__(self, a0, b0, c0, coord_type='R', method=1):
        self.coord_type = coord_type

        if method == 1:
            self.compute_matrix1(a0, b0, c0)
        elif method == 2:
            self.compute_matrix2(a0, b0, c0)

    @property
    def origin(self):
        return self._origin

    @property
    def M(self):
        return self._M

    def compute_matrix1(self, a0, b0, c0):
        self._origin = a0
        e2 = np.cross(b0 - a0, c0 - a0)
        e2 /= np.linalg.norm(e2)
        e1 = np.cross(e2, b0 - a0)
        e1 /= np.linalg.norm(e1)
        e3 = np.cross(e1, e2)
        self._M = np.array([e1, e2, e3])

    def compute_matrix2(self, a0, b0, c0):
        self._origin = a0
        e1 = b0 - a0
        e1 /= np.linalg.norm(e1)
        e3 = np.cross(e1, c0 - a0)
        e3 /= np.linalg.norm(e3)
        e2 = np.cross(e3, e1)
        self._M = np.array([e1, e2, e3])

    def get_xyz(self, xyz0, is_vector=False):

        if not is_vector:
            xyz0 = xyz0 - self._origin

        xyz = np.dot(xyz0, self._M.T)

        if self.coord_type == 'C':
            return np.array(cart2cyl(*xyz))
        elif self.coord_type == 'S':
            return np.array(cart2sph(*xyz))

        return xyz

    def get_xyz0(self, xyz, is_vector=False):

        if self.coord_type == 'C':
            xyz = cyl2cart(*xyz)
        elif self.coord_type == 'S':
            xyz = sph2cart(*xyz)

        if is_vector:
            return np.dot(xyz, self._M)
        else:
            return self._origin + np.dot(xyz, self._M)


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

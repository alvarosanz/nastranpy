import numpy as np
from nastranpy.bdf.cards.coord_system import CoordSystem


def bar_settle_factory(card_name):

    def wrapped(self):
        G1, G2 = (grid.xyz0 for grid in self.grids)

        if card_name == 'CROD' or card_name == 'CONROD':

            if (G2 - G1)[2] == 0:
                v = np.array([0.0, 0.0, 1.0])
            else:
                v = np.array([1.0, 0.0, 0.0])

        elif card_name == 'CBAR' or card_name == 'CBEAM':

            if self.G0:
                v = self.G0.xyz0 - G1
            else:
                v = self.v

                if self.grids[0].coord:
                    self.grids[0].coord.get_xyz0(v, is_vector=True)

            G1 += self.offsetA
            G2 += self.offsetB

        self._coord = CoordSystem(G1, G2, G1 + v, method=2)
        self._axis = self._coord.M[0]

    return wrapped

def shell_settle_factory(card_name):

    if card_name == 'CTRIA3':

        def wrapped(self):
            G1, G2, G3 = (grid.xyz0 for grid in self.grids)
            self._coord = CoordSystem(G1, G2, G3, method=2)
            self._normal = self._coord.M[2]
            self._area = 0.5 * np.linalg.norm(G2 - G1) * np.dot(G3 - G1, self._coord.M[1])
            self._centroid = (G1 + G2 + G3) / self._area

    elif card_name == 'CQUAD4':

        def wrapped(self):
            G1, G2, G3, G4 = (grid.xyz0 for grid in self.grids)
            v1 = G2 - G4
            v1 /= np.linalg.norm(v1)
            v2 = G3 - G1
            diagonal = np.linalg.norm(v2)
            v2 /= diagonal
            n = np.cross(v1, v2)
            n_alt = np.cross(n, v1)
            d = np.dot(G1 - G4, n) # Minimum distance between two diagonals
            mean_offset = 0.5 * d * n
            origin = G1 - np.dot(n_alt, G1 - G4) / np.dot(n_alt, v2) * v2 - mean_offset
            self._coord = CoordSystem(origin, origin + n, origin + v1 + v2)
            self._normal = self._coord.M[2]
            # Project grid points over the mean plane
            G1 -=  mean_offset
            G2 +=  mean_offset
            G3 -=  mean_offset
            G4 +=  mean_offset
            area1 = 0.5 * diagonal * np.dot(G1 - G2, np.cross(self._coord.M[2], v2))
            area2 = 0.5 * diagonal * np.dot(G4 - G1, np.cross(self._coord.M[2], v2))
            self._area = area1 + area2
            self._centroid = (area1 * (G1 + G2 + G3) + area2 * (G1 + G3 + G4)) / self._area

    return wrapped

def bar_axis(self):

    if not self._coord:
        self.settle()

    return self._area

def shell_normal(self):

    if not self._coord:
        self.settle()

    return self._normal

def shell_area(self):

    if not self._coord:
        self.settle()

    return self._area

def shell_centroid(self):

    if not self._coord:
        self.settle()

    return self._centroid

def shell_thickness(self):
    return self.prop.thickness

def PCOMP_thickness(self):
    return sum((ply.T for ply in self.plies))

card_interfaces_additional = {
#   card_name: {
#       method_name: (function, is_property),
#   },
    # Grids
    # Materials
    # Elements
    'CROD': {
        'settle': (bar_settle_factory('CROD'), False),
        'axis': (bar_axis, True),
    },
    'CONROD': {
        'settle': (bar_settle_factory('CONROD'), False),
        'axis': (bar_axis, True),
    },
    'CBAR': {
        'settle': (bar_settle_factory('CBAR'), False),
        'axis': (bar_axis, True),
    },
    'CBEAM': {
        'settle': (bar_settle_factory('CBEAM'), False),
        'axis': (bar_axis, True),
    },
    'CQUAD4': {
        'settle': (shell_settle_factory('CQUAD4'), False),
        'normal': (shell_normal, True),
        'area': (shell_area, True),
        'centroid': (shell_centroid, True),
        'thickness': (shell_thickness, True),
    },
    'CTRIA3': {
        'settle': (shell_settle_factory('CTRIA3'), False),
        'normal': (shell_normal, True),
        'area': (shell_area, True),
        'centroid': (shell_centroid, True),
        'thickness': (shell_thickness, True),
    },
    # Properties
    'PCOMP': {
        'thickness': (PCOMP_thickness, True),
    },
    'PCOMPG': {
        'thickness': (PCOMP_thickness, True),
    },
    # MPCs
    # SPCs
    # LOADs
}


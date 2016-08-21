import numpy as np
from nastranpy.bdf.cards.enums import Item, Seq, Tag
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

    return wrapped

def bar_axis(self):
    return self.coord.M[0]

def CQUAD4_settle(self):
    G1, G2, G3, G4 = (grid.xyz0 for grid in self.grids)
    v1 = G2 - G4
    v1 /= np.linalg.norm(v1)
    v2 = G3 - G1
    v2 /= np.linalg.norm(v2)
    n = np.cross(v1, v2)
    n_alt = np.cross(n, v1)
    d = np.dot(G1 - G4, n) # Minimum distance between two diagonals
    origin = G1 - np.dot(n_alt, G1 - G4) / np.dot(n_alt, v2) * v2 - 0.5 * d * n
    self._coord = CoordSystem(origin, origin + n, origin + v1 + v2)

def CTRIA3_settle(self):
    G1, G2, G3 = (grid.xyz0 for grid in self.grids)
    self._coord = CoordSystem(G1, G2, G3, method=2)

def shell_normal(self):
    return self.coord.M[2]

additional_card_methods = {
    'CROD': {
        'settle': bar_settle_factory('CROD'),
        'axis': bar_axis,
    },
    'CONROD': {
        'settle': bar_settle_factory('CONROD'),
        'axis': bar_axis,
    },
    'CBAR': {
        'settle': bar_settle_factory('CBAR'),
        'axis': bar_axis,
    },
    'CBEAM': {
        'settle': bar_settle_factory('CBEAM'),
        'axis': bar_axis,
    },
    'CQUAD4': {
        'settle': CQUAD4_settle,
        'normal': shell_normal,
    },
    'CTRIA3': {
        'settle': CTRIA3_settle,
        'normal': shell_normal,
    },
}


import numpy as np
from nastranpy.bdf.cards.enums import Item, Seq, Tag
from nastranpy.bdf.cards.coord_system import CoordSystem


def SHELL_normal(self):
    return self.coord.M[2]

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

additional_card_methods = {
    'CQUAD4': {
        'settle': CQUAD4_settle,
        'normal': SHELL_normal,
    },
    'CTRIA3': {
        'settle': CTRIA3_settle,
        'normal': SHELL_normal,
    },
}


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
        self._centroid = (G1 + G2) / 2

    return wrapped


def shell_settle_factory(card_name):

    if card_name == 'CTRIA3':

        def wrapped(self):
            G1, G2, G3 = (grid.xyz0 for grid in self.grids)
            self._coord = CoordSystem(G1, G2, G3, method=2)
            self._normal = self._coord.M[2]
            self._area = 0.5 * np.linalg.norm(G2 - G1) * np.dot(G3 - G1, self._coord.M[1])
            self._centroid = (G1 + G2 + G3) / 3

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
            self._centroid = (area1 * (G1 + G2 + G3) / 3 + area2 * (G1 + G3 + G4) / 3) / self._area

    return wrapped


def prop_area_factory(card_name):

    if card_name in ('PROD', 'PBAR', 'PBEAM'):

        def wrapped(self):
            return self.A

    elif card_name == 'PBARL':

        def wrapped(self):
            section_type = self.TYPE
            data = self.data

            if section_type == 'ROD':
                area = np.pi * data[0] ** 2
            elif section_type == 'TUBE':
                area = np.pi * (data[0] ** 2 - data[1] ** 2)
            elif section_type == 'TUBE2':
                area = np.pi * (data[0] ** 2 - (data[0] - data[1]) ** 2)
            elif section_type == 'I':
                area = data[2] * data[5] + (data[0] - data[5] - data[4]) * data[3] + data[1] * data[4]
            elif section_type == 'CHAN':
                area = data[1] * data[2] + 2 * data[3] * (data[0] - data[2])
            elif section_type == 'T':
                area = data[0] * data[2] + data[3] * (data[1] - data[2])
            elif section_type == 'BOX':
                area = data[0] * data[1] - (data[0] - 2 * data[3]) * (data[1] - 2 * data[2])
            elif section_type == 'BAR':
                area = data[0] * data[1]
            elif section_type == 'CROSS':
                area = data[1] * data[2] + data[0] * data[3]
            elif section_type == 'H':
                area = data[1] * data[2] + data[0] * data[3]
            elif section_type == 'T1':
                area = data[0] * data[2] + data[1] * data[3]
            elif section_type == 'I1':
                area = data[1] * data[3] + data[0] * (data[3] - data[2])
            elif section_type == 'CHAN1':
                area = data[1] * data[3] + data[0] * (data[3] - data[2])
            elif section_type == 'Z':
                area = data[1] * data[3] + data[0] * (data[3] - data[2])
            elif section_type == 'CHAN2':
                area = 2 * data[0] * data[2] + data[1] * (data[3] - 2 * data[0])
            elif section_type == 'T2':
                area = data[0] * data[2] + data[3] * (data[1] - data[2])
            elif section_type == 'BOX1':
                area = data[0] * (data[2] + data[3]) + (data[1] - data[2] - data[3]) * (data[4] + data[5])
            elif section_type == 'HEXA':
                area = data[2] * (data[1] - data[0])
            elif section_type == 'HAT':
                area = 2 * data[1] + (data[3] + data[0]) + data[1] * (data[2] - 2 * data[1])
            elif section_type == 'HAT1':
                area = data[0] * (data[3] + data[4]) + 2 * data[3] * (data[1] - data[3] - data[4])
            elif section_type == 'DBOX':
                area = None

            return area

    elif card_name == 'PBEAML':

        def wrapped(self):
            return 0.0

    return wrapped


def bar_area_factory(card_name):

    if card_name == 'CONROD':

        def wrapped(self):
            return self.A

    else:

        def wrapped(self):
            return self.prop.area

    return wrapped


def bar_axis(self):

    if not self._coord:
        self._settle()

    return self._axis


def shell_normal(self):

    if not self._coord:
        self._settle()

    return self._normal


def shell_area(self):

    if not self._coord:
        self._settle()

    return self._area


def centroid(self):

    if not self._coord:
        self._settle()

    return self._centroid


def shell_thickness(self):
    return self.prop.thickness


def PCOMP_thickness(self):
    return sum((ply.T for ply in self.plies))


def get_nearby_grids(self, grid):
    index = self.grids.index(grid)
    n_grids = len(self.grids)
    index_prev = (index - 1) % n_grids
    index_next = (index + 1) % n_grids
    return (self.grids[index_prev], self.grids[index_next])


card_interfaces_additional = {
#   card_name: {
#       method_name: (function, is_property),
#   },
    # Grids
    # Materials
    # Elements
    'CROD': {
        '_settle': (bar_settle_factory('CROD'), False),
        'axis': (bar_axis, True),
        'area': (bar_area_factory('CROD'), True),
        'centroid': (centroid, True),
    },
    'CONROD': {
        '_settle': (bar_settle_factory('CONROD'), False),
        'axis': (bar_axis, True),
        'area': (bar_area_factory('CONROD'), True),
        'centroid': (centroid, True),
    },
    'CBAR': {
        '_settle': (bar_settle_factory('CBAR'), False),
        'axis': (bar_axis, True),
        'area': (bar_area_factory('CBAR'), True),
        'centroid': (centroid, True),
    },
    'CBEAM': {
        '_settle': (bar_settle_factory('CBEAM'), False),
        'axis': (bar_axis, True),
        'area': (bar_area_factory('CBEAM'), True),
        'centroid': (centroid, True),
    },
    'CQUAD4': {
        '_settle': (shell_settle_factory('CQUAD4'), False),
        'normal': (shell_normal, True),
        'area': (shell_area, True),
        'centroid': (centroid, True),
        'thickness': (shell_thickness, True),
        'get_nearby_grids': (get_nearby_grids, False),
    },
    'CTRIA3': {
        '_settle': (shell_settle_factory('CTRIA3'), False),
        'normal': (shell_normal, True),
        'area': (shell_area, True),
        'centroid': (centroid, True),
        'thickness': (shell_thickness, True),
        'get_nearby_grids': (get_nearby_grids, False),
    },
    # Properties
    'PROD': {
        'area': (prop_area_factory('PROD'), True),
    },
    'PBAR': {
        'area': (prop_area_factory('PBAR'), True),
    },
    'PBEAM': {
        'area': (prop_area_factory('PBEAM'), True),
    },
    'PBARL': {
        'area': (prop_area_factory('PBARL'), True),
    },
    'PBEAML': {
        'area': (prop_area_factory('PBEAML'), True),
    },
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


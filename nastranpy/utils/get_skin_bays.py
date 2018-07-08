import csv
import numpy as np
from nastranpy.bdf.model import Model


def get_skin_bays_geom(bdf_files, EIDs_file, output_file):
    # Import model
    model_0 = Model()
    model_0.read(bdf_files)


    # Generate dummy mesh
    skin_bays = dict()
    model = Model()

    with open(EIDs_file) as f:

        for bay_id, row in enumerate(csv.reader(f)):
            bay_name = row[0]
            skin_bays[bay_name] = None
            shells = set(model_0.cards('elem', [int(EID) for EID in row[1:]]))
            grids = set()
            corner_grids = [set(), set(), set(), set()]

            for shell in shells:

                for i, grid in enumerate(shell.grids):
                    grids.add(grid)
                    corner_grids[i].add(grid)

            bay_grids = list()

            try:

                for i0, i1, i2 in [(1, 2, 3), (0, 2, 3), (0, 1, 3), (0, 1, 2)]:
                    grids_temp = grids - corner_grids[i0] - corner_grids[i1] - corner_grids[i2]

                    if len(grids_temp) == 1:
                        bay_grids.append(grids_temp.pop())
                    else:
                        raise ValueError()

            except ValueError:
                print(f"WARNING: Bay '{bay_name}' skipped")
                continue

            for grid in bay_grids:

                if grid.id not in model.grids:
                    model.create_card(['GRID', grid.id, None, grid.xyz0[0], grid.xyz0[1], grid.xyz0[2]])

            skin_bays[bay_name] = SkinBay(model.create_card(['CQUAD4', bay_id + 1, None] + [grid.id for grid in bay_grids]))


    # Link bays & process geometry
    bay_geometry = dict()

    for bay_name, skin_bay in skin_bays.items():

        if skin_bay:
            skin_bay.link()
            bay_geometry[bay_name] = skin_bay.get_geometry()
        else:
            bay_geometry[bay_name] = None


    # Print output file
    with open(output_file, 'w') as f:
        writer = csv.writer(f, lineterminator='\n')

        for bay_name, geometry in bay_geometry.items():

            if geometry:
                writer.writerow([bay_name] + list(geometry))
            else:
                writer.writerow([bay_name, 'N/A', 'N/A', 'N/A'])


class SkinBay(object):

    def __init__(self, bay):
        self.me = bay
        self.left = None
        self.right = None
        self.fwd = None
        self.aft = None

    def link(self):

        for bay in self.me.extend('e2D', steps=1):

            if self.me.grids[0] is bay.grids[3] and self.me.grids[1] is bay.grids[2]:
                self.left = bay

            if self.me.grids[3] is bay.grids[0] and self.me.grids[2] is bay.grids[1]:
                self.right = bay

            if self.me.grids[0] is bay.grids[1] and self.me.grids[3] is bay.grids[2]:
                self.fwd = bay

            if self.me.grids[1] is bay.grids[0] and self.me.grids[2] is bay.grids[3]:
                self.aft = bay

    def get_geometry(self):
        a = (np.linalg.norm(self.me.grids[1].xyz0 - self.me.grids[0].xyz0) +
             np.linalg.norm(self.me.grids[2].xyz0 - self.me.grids[3].xyz0)) / 2
        b = (np.linalg.norm(self.me.grids[3].xyz0 - self.me.grids[0].xyz0) +
             np.linalg.norm(self.me.grids[2].xyz0 - self.me.grids[1].xyz0)) / 2

        p_left = (self.me.grids[0].xyz0 + self.me.grids[1].xyz0) / 2
        p_right = (self.me.grids[3].xyz0 + self.me.grids[2].xyz0) / 2
        d = np.linalg.norm(p_right - p_left)
        Rs = list()

        if self.left:
            v_left = self.me.normal + self.left.normal
            v_left /= np.linalg.norm(v_left)
            Rs.append((d / 2) / np.linalg.norm(np.cross(self.me.normal, v_left)))

        if self.right:
            v_right = self.me.normal + self.right.normal
            v_right /= np.linalg.norm(v_right)
            Rs.append((d / 2) / np.linalg.norm(np.cross(v_right, self.me.normal)))

        R = sum(Rs) / len(Rs)

        return a, b, R

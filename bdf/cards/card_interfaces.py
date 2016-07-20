from nastranpy.bdf.cards.enums import Item, Set, Tag, Field
from nastranpy.bdf.cards.class_factory import class_factory
from nastranpy.bdf.cards.card_factory import CardFactory
from nastranpy.bdf.cards.grid import Grid


class F(object):

    def __init__(self, name=None, type=None,
                 seq_type=None, length=None, repeat_pattern=None, subscheme=None,
                 other_name=None, other_type=None):
        self.name = name
        self.type = type
        self.seq_type = seq_type
        self.length = length
        self.repeat_pattern = repeat_pattern
        self.subscheme = subscheme
        self.other_name = other_name
        self.other_type = other_type


card_interfaces = {
    # Grids
    'GRID': Grid,#class_factory(Item.grid, ['GRID', None, [Item.coord, 'CP'], [None, 'X1'], [None, 'X2'], [None, 'X3'], [Item.coord, 'CD'], [None, 'PS'], [None, 'SEID']]),
    # Coordinate systems
    'CORD2R': class_factory('CORD2R', Item.coord, [F(), F(), F('RID', Item.coord), F('A1'), F('A2'), F('A3'), F('B1'), F('B2'), F('B3'),
                                                   F('C1'), F('C2'), F('C3')]),
    'CORD2C': class_factory('CORD2C', Item.coord, [F(), F(), F('RID', Item.coord), F('A1'), F('A2'), F('A3'), F('B1'), F('B2'), F('B3'),
                                                   F('C1'), F('C2'), F('C3')]),
    'CORD2S': class_factory('CORD2S', Item.coord, [F(), F(), F('RID', Item.coord), F('A1'), F('A2'), F('A3'), F('B1'), F('B2'), F('B3'),
                                                   F('C1'), F('C2'), F('C3')]),
    'CORD1R': class_factory('CORD1R', Item.coord, [F(), F(), F('G1A', Item.grid), F('G2A', Item.grid), F('G3A', Item.grid)]), # To implement alternate format
    'CORD1C': class_factory('CORD1C', Item.coord, [F(), F(), F('G1A', Item.grid), F('G2A', Item.grid), F('G3A', Item.grid)]), # To implement alternate format
    'CORD1S': class_factory('CORD1S', Item.coord, [F(), F(), F('G1A', Item.grid), F('G2A', Item.grid), F('G3A', Item.grid)]), # To implement alternate format
    # Materials
    'MAT1': class_factory('MAT1', Item.mat, [F(), F(), F('E'), F('G'), F('NU'), F('RHO'), F('A'), F('TREF'), F('GE'),
                                             F('ST'), F('SC'), F('SS'), F('MCSID', Item.coord)]),
    'MAT2': class_factory('MAT2', Item.mat, [F(), F(), F('G11'), F('G12'), F('G13'), F('G22'), F('G23'), F('G33'), F('RHO'),
                                             F('A1'), F('A2'), F('A3'), F('TREF'), F('GE'), F('ST'), F('SC'), F('SS'),
                                             F('MCSID', Item.coord)]),
    'MAT3': class_factory('MAT3', Item.mat, [F(), F(), F('EX'), F('ETH'), F('EZ'), F('NUXTH'), F('NUTHZ'), F('NUZX'), F('RHO'),
                                             F(), F(), F('GZX'), F('AX'), F('ATH'), F('AZ'), F('TREF'), F('GE')]),
    'MAT8': class_factory('MAT8', Item.mat, [F(), F(), F('E1'), F('E2'), F('NU12'), F('G12'), F('G1Z'), F('G2Z'), F('RHO'),
                                             F('A1'), F('A2'), F('TREF'), F('Xt'), F('Xc'), F('Yt'), F('Yc'), F('S'),
                                             F('GE'), F('F12'), F('STRN')]),
    'MAT9': class_factory('MAT9', Item.mat, [F(), F(), F('G11'), F('G12'), F('G13'), F('G14'), F('G15'), F('G16'), F('G22'),
                                             F('G23'), F('G24'), F('G25'), F('G26'), F('G33'), F('G34'), F('G35'), F('G36'),
                                             F('G44'), F('G45'), F('G46'), F('G55'), F('G56'), F('G66'), F('RHO'), F('A1'),
                                             F('A2'), F('A3'), F('A4'), F('A5'), F('A6'), F('TREF'), F('GE')]),
    # Elements
    'CROD': class_factory('CROD', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Field.list, 2)], tag=Tag.e1D),
    'CONROD': class_factory('CONROD', Item.elem, [F(), F(), F('grids', Item.grid, Field.list, 2), F(Item.mat.name, Item.mat), F('A'), F('J'), F('C'), F('NSM')], tag=Tag.e1D),
    'CBAR': class_factory('CBAR', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Field.list, 2), Field('X1', other_name='G0', other_type=Item.grid), F('X2'), F('X3'), F('OFFT'),
                                              F('PA'), F('PB'), F('W1A'), F('W2A'), F('W3A'), F('W1B'), F('W2B'), F('W3B')], tag=Tag.e1D),
    'CBEAM': class_factory('CBEAM', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Field.list, 2), Field('X1', other_name='G0', other_type=Item.grid), F('X2'), F('X3'), F('OFFT'),
                                                F('PA'), F('PB'), F('W1A'), F('W2A'), F('W3A'), F('W1B'), F('W2B'), F('W3B'),
                                                F('SA', Item.grid), F('SB', Item.grid)], tag=Tag.e1D),
    'CQUAD4': class_factory('CQUAD4', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Field.list, 4), Field('THETA', other_name='MCID', other_type=Item.coord), F('ZOFFS'),
                                                  F(), F('TFLAG'), F('T1'), F('T2'), F('T3'), F('T4')], tag=Tag.e2D),
    'CTRIA3': class_factory('CTRIA3', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Field.list, 3), Field('THETA', other_name='MCID', other_type=Item.coord), F('ZOFFS'),
                                                  F(), F('TFLAG'), F('T1'), F('T2'), F('T3')], tag=Tag.e2D),
    'CTETRA': class_factory('CTETRA', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Field.list, 10)], tag=Tag.e3D),
    'CPENTA': class_factory('CPENTA', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Field.list, 15)], tag=Tag.e3D),
    'CHEXA': class_factory('CHEXA', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Field.list, 20)], tag=Tag.e3D),
    'RBE2': class_factory('RBE2', Item.elem, [F(), F('grid0', Item.grid), F('CM'), F('grids', Item.grid, Field.set), F('ALPHA')], tag=Tag.eRigid), # To implement
    'RBE3': class_factory('RBE3', Item.elem, ['RBE3', None], tag=Tag.eRigid), # To implement
    'CBUSH': class_factory('CBUSH', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Field.list, 2), Field('X1', other_name='G0', other_type=Item.grid), F('X2'), F('X3'), F('CID', Item.coord),
                                                F('S'), F('OCID', Item.coord), F('S1'), F('S2'), F('S3')], tag=Tag.eSpring),
    'CONM2': class_factory('CONM2', Item.elem, [F(), F(), F('grids', Item.grid, Field.list, 1), F('CID', Item.coord), F('M'), F('X1'), F('X2'), F('X3'), F(),
                                                F('I11'), F('I21'), F('I22'), F('I31'), F('I32'), F('I33')], tag=Tag.e0D),
    'PLOTEL': class_factory('PLOTEL', Item.elem, [F(), F(), F('grids', Item.grid, Field.list, 2)], tag=Tag.ePlot),
    # Properties
    'PROD': class_factory('PROD', Item.prop, [F(), F(), F(Item.mat.name, Item.mat), F('A'), F('J'), F('C'), F('NSM')]),
    'PBAR': class_factory('PBAR', Item.prop, [F(), F(), F(Item.mat.name, Item.mat), F('A'), F('I1'), F('I2'), F('J'), F('NSM'), F(),
                                              F('C1'), F('C2'), F('D1'), F('D2'), F('E1'), F('E2'), F('F1'), F('F2'),
                                              F('K1'), F('K2'), F('I12')]),
    'PBARL': class_factory('PBARL', Item.prop, [F(), F(), F(Item.mat.name, Item.mat), F('GROUP'), F('TYPE')]), # To implement NSM and DIMs
    'PBEAM': class_factory('PBEAM', Item.prop, [F(), F(), F(Item.mat.name, Item.mat)]), # To implement NSM and DIMs
    'PBEAML': class_factory('PBEAML', Item.prop, [F(), F(), F(Item.mat.name, Item.mat), F('GROUP'), F('TYPE')]), # To implement NSM and DIMs
    'PSHELL': class_factory('PSHELL', Item.prop, [F(), F(), F('mat1', Item.mat), F('T'), F('mat2', Item.mat), F('BMIR'), F('mat3', Item.mat), F('TST'), F('NSM'),
                                                  F('Z1'), F('Z2'), F('mat4', Item.mat)]),
    'PCOMP': class_factory('PCOMP', Item.prop, [F(), F()]), # To implement
    'PCOMPG': class_factory('PCOMPG', Item.prop, [F(), F()]), # To implement
    'PSOLID': class_factory('PSOLID', Item.prop, [F(), F(), F(Item.mat.name, Item.mat), F('CORDM', Item.coord), F('IN'), F('STRESS'), F('ISOP'), F('FCTN'), F('COROT')]),
    'PBUSH': class_factory('PBUSH', Item.prop, [F(), F()]), # To implement
    # MPCs
    'MPC': class_factory('MPC', Set.mpc, [F(), F()]), # To implement
    'MPCADD': class_factory('MPCADD', Set.mpc, [F(), F()]), # To implement
    # SPCs
    'SPC': class_factory('SPC', Set.spc, [F(), F()]),
    # LOADs
    'FORCE': class_factory('FORCE', Set.load, [F(), F()]),
}

card_factory = CardFactory(card_interfaces)

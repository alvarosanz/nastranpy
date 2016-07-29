from nastranpy.bdf.cards.enums import Item, Set, Tag, Seq
from nastranpy.bdf.cards.class_factory import class_factory
from nastranpy.bdf.cards.card_factory import CardFactory
from nastranpy.bdf.cards.padding import Padding
from nastranpy.bdf.cards.field_info import FieldInfo as F


card_interfaces = {
    # Grids
    'GRID': class_factory('GRID', Item.grid, [F(), F(), F('CP', Item.coord), F(seq_type=Seq.vector, length=3), F('CD', Item.coord, alternate_name=Item.coord.name), F('PS'), F('SEID')]),
    # Coordinate systems
    'CORD2R': class_factory('CORD2R', Item.coord, [F(), F(), F('RID', Item.coord), F('A', seq_type=Seq.vector, length=3), F('B', seq_type=Seq.vector, length=3),
                                                   F('C', seq_type=Seq.vector, length=3)]),
    'CORD2C': class_factory('CORD2C', Item.coord, [F(), F(), F('RID', Item.coord), F('A', seq_type=Seq.vector, length=3), F('B', seq_type=Seq.vector, length=3),
                                                   F('C', seq_type=Seq.vector, length=3)]),
    'CORD2S': class_factory('CORD2S', Item.coord, [F(), F(), F('RID', Item.coord), F('A', seq_type=Seq.vector, length=3), F('B', seq_type=Seq.vector, length=3),
                                                   F('C', seq_type=Seq.vector, length=3)]),
    'CORD1R': class_factory('CORD1R', Item.coord, [F(), F(), F('G1A', Item.grid), F('G2A', Item.grid), F('G3A', Item.grid), F(seq_type=Seq.list, length=4, other_card=True)]),
    'CORD1C': class_factory('CORD1C', Item.coord, [F(), F(), F('G1A', Item.grid), F('G2A', Item.grid), F('G3A', Item.grid), F(seq_type=Seq.list, length=4, other_card=True)]),
    'CORD1S': class_factory('CORD1S', Item.coord, [F(), F(), F('G1A', Item.grid), F('G2A', Item.grid), F('G3A', Item.grid), F(seq_type=Seq.list, length=4, other_card=True)]),
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
    'CROD': class_factory('CROD', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Seq.list, 2, update_grid=True)], card_tag=Tag.e1D),
    'CONROD': class_factory('CONROD', Item.elem, [F(), F(), F('grids', Item.grid, Seq.list, 2, update_grid=True), F(Item.mat.name, Item.mat), F('A'), F('J'), F('C'), F('NSM')], card_tag=Tag.e1D),
    'CBAR': class_factory('CBAR', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Seq.list, 2, update_grid=True), F('X1', Item.grid, alternate_name='G0'), F('X2'), F('X3'), F('OFFT'),
                                              F('PA'), F('PB'), F('W1A'), F('W2A'), F('W3A'), F('W1B'), F('W2B'), F('W3B')], card_tag=Tag.e1D),
    'CBEAM': class_factory('CBEAM', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Seq.list, 2, update_grid=True), F('X1', Item.grid, alternate_name='G0'), F('X2'), F('X3'), F('OFFT'),
                                                F('PA'), F('PB'), F('W1A'), F('W2A'), F('W3A'), F('W1B'), F('W2B'), F('W3B'),
                                                F('SA', Item.grid), F('SB', Item.grid)], card_tag=Tag.e1D),
    'CQUAD4': class_factory('CQUAD4', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Seq.list, 4, update_grid=True), F('THETA', Item.coord, alternate_name='MCID'), F('ZOFFS'),
                                                  F(), F('TFLAG'), F('T1'), F('T2'), F('T3'), F('T4')], card_tag=Tag.e2D),
    'CTRIA3': class_factory('CTRIA3', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Seq.list, 3, update_grid=True), F('THETA', Item.coord, alternate_name='MCID'), F('ZOFFS'),
                                                  F(), F('TFLAG'), F('T1'), F('T2'), F('T3')], card_tag=Tag.e2D),
    'CTETRA': class_factory('CTETRA', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Seq.list, 10, update_grid=True)], card_tag=Tag.e3D),
    'CPENTA': class_factory('CPENTA', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Seq.list, 15, update_grid=True)], card_tag=Tag.e3D),
    'CHEXA': class_factory('CHEXA', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Seq.list, 20, update_grid=True)], card_tag=Tag.e3D),
    'RBE2': class_factory('RBE2', Item.elem, [F(), F(), F('master_grid', Item.grid, update_grid=True), F('CM'), F('slave_grids', Item.grid, Seq.set, update_grid=True), F('ALPHA')], card_tag=Tag.eRigid),
    'RBE3': class_factory('RBE3', Item.elem, [F(), F(), F(), F('master_grid', Item.grid, update_grid=True), F('REFC'),
                                              F('slaves', seq_type=Seq.list, subscheme=[F('WT'), F('C'), F('grids', Item.grid, Seq.set, update_grid=True)]),
                                              F('UM', seq_type=Seq.list, subscheme=[F('GM', Item.grid), F('CM')], optional=True),
                                              F('ALPHA', optional=True)], card_tag=Tag.eRigid),
    'CBUSH': class_factory('CBUSH', Item.elem, [F(), F(), F(Item.prop.name, Item.prop), F('grids', Item.grid, Seq.list, 2, update_grid=True), F('X1', Item.grid, alternate_name='G0'), F('X2'), F('X3'), F('CID', Item.coord),
                                                F('S'), F('OCID', Item.coord), F('S1'), F('S2'), F('S3')], card_tag=Tag.eSpring),
    'CONM2': class_factory('CONM2', Item.elem, [F(), F(), F('grids', Item.grid, Seq.list, 1, update_grid=True), F('CID', Item.coord), F('M'), F('X1'), F('X2'), F('X3'), F(),
                                                F('I11'), F('I21'), F('I22'), F('I31'), F('I32'), F('I33')], card_tag=Tag.eMass),
    'PLOTEL': class_factory('PLOTEL', Item.elem, [F(), F(), F('grids', Item.grid, Seq.list, 2, update_grid=True)], card_tag=Tag.ePlot),
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
    'PCOMP': class_factory('PCOMP', Item.prop, [F(), F(), F('Z0'), F('NSM'), F('SB'), F('FT'), F('TREF'), F('GE'), F('LAM'),
                                                F('plies', seq_type=Seq.list, subscheme=[F(Item.mat.name, Item.mat), F('T'), F('THETA'), F('SOUT')])]),
    'PCOMPG': class_factory('PCOMPG', Item.prop, [F(), F(), F('Z0'), F('NSM'), F('SB'), F('FT'), F('TREF'), F('GE'), F('LAM'),
                                                  F('plies', seq_type=Seq.list, subscheme=[F('id'), F(Item.mat.name, Item.mat), F('T'), F('THETA'), F('SOUT')])], card_padding=Padding(9, {6, 7, 8})),
    'PSOLID': class_factory('PSOLID', Item.prop, [F(), F(), F(Item.mat.name, Item.mat), F('CORDM', Item.coord), F('IN'), F('STRESS'), F('ISOP'), F('FCTN'), F('COROT')]),
    'PBUSH': class_factory('PBUSH', Item.prop, [F(), F(),
                                                F('K', subscheme=[F('K1'), F('K2'), F('K3'), F('K4'), F('K5'), F('K6')], optional=True),
                                                F('B', subscheme=[F('B1'), F('B2'), F('B3'), F('B4'), F('B5'), F('B6')], optional=True),
                                                F('GE', subscheme=[F('GE1'), F('GE2'), F('GE3'), F('GE4'), F('GE5'), F('GE6')], optional=True),
                                                F('RCV', subscheme=[F('SA'), F('ST'), F('EA'), F('ET')], optional=True),
                                                F('M', optional=True)], card_padding=Padding(9, {1})),
    # MPCs
    'MPC': class_factory('MPC', Set.mpc, [F(), F(), F('equation', seq_type=Seq.list, subscheme=[F('G', Item.grid), F('C'), F('A')])], card_padding=Padding(8, {1, 8})),
    'MPCADD': class_factory('MPCADD', Set.mpc, [F(), F(), F('sets', Set.mpc, seq_type=Seq.set)]),
    # SPCs
    'SPC': class_factory('SPC', Set.spc, [F(), F(), F('G1', Item.grid), F('C1'), F('D1'), F('G2', Item.grid), F('C2'), F('D2')]),
    'SPCD': class_factory('SPCD', Set.spc, [F(), F(), F('G1', Item.grid), F('C1'), F('D1'), F('G2', Item.grid), F('C2'), F('D2')]),
    'SPC1': class_factory('SPC1', Set.spc, [F(), F(), F('C'), F('grids', Item.grid, Seq.set)]),
    'SPCADD': class_factory('SPCADD', Set.spc, [F(), F(), F('sets', Set.spc, seq_type=Seq.set)]),
    # LOADs
    'FORCE': class_factory('FORCE', Set.load, [F(), F(), F('G', Item.grid), F('CID', Item.coord), F('F'), F('n', seq_type=Seq.vector, length=3)]),
    'MOMENT': class_factory('MOMENT', Set.load, [F(), F(), F('G', Item.grid), F('CID', Item.coord), F('M'), F('n', seq_type=Seq.vector, length=3)]),
    'PLOAD4': class_factory('PLOAD4', Set.load, [F(), F(), F(Item.elem.name, Item.elem), F('P1'), F('P2'), F('P3'), F('P4'), F('G1', Item.grid), F('G3', Item.grid, alternate_name='G4'),
                                                 F('CID', Item.coord), F('n', seq_type=Seq.vector, length=3), F('SORL'), F('LDIR')])
}

card_factory = CardFactory(card_interfaces)

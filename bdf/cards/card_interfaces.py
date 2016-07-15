from nastran_tools.bdf.cards.enums import Item, Set, Tag
from nastran_tools.bdf.cards.class_factory import class_factory
from nastran_tools.bdf.cards.card_factory import CardFactory
from nastran_tools.bdf.cards.grid import Grid


card_interfaces = {
    # Grids
    'GRID': Grid,#class_factory(Item.grid, ['GRID', None, [Item.coord, 'CP'], [None, 'X1'], [None, 'X2'], [None, 'X3'], [Item.coord, 'CD'], [None, 'PS'], [None, 'SEID']]),
    # Coordinate systems
    'CORD2R': class_factory(Item.coord, ['CORD2R', None, [Item.coord, 'RID'], [None, 'A1'], [None, 'A2'], [None, 'A3'], [None, 'B1'], [None, 'B2'], [None, 'B3'],
                                         [None, 'C1'], [None, 'C2'], [None, 'C3']]),
    'CORD2C': class_factory(Item.coord, ['CORD2C', None, [Item.coord, 'RID'], [None, 'A1'], [None, 'A2'], [None, 'A3'], [None, 'B1'], [None, 'B2'], [None, 'B3'],
                                         [None, 'C1'], [None, 'C2'], [None, 'C3']]),
    'CORD2S': class_factory(Item.coord, ['CORD2S', None, [Item.coord, 'RID'], [None, 'A1'], [None, 'A2'], [None, 'A3'], [None, 'B1'], [None, 'B2'], [None, 'B3'],
                                         [None, 'C1'], [None, 'C2'], [None, 'C3']]),
    'CORD1R': class_factory(Item.coord, ['CORD1R', None, [Item.grid, 'G1A'], [Item.grid, 'G2A'], [Item.grid, 'G3A']]), # To implement alternate format
    'CORD1C': class_factory(Item.coord, ['CORD1C', None, [Item.grid, 'G1A'], [Item.grid, 'G2A'], [Item.grid, 'G3A']]), # To implement alternate format
    'CORD1S': class_factory(Item.coord, ['CORD1S', None, [Item.grid, 'G1A'], [Item.grid, 'G2A'], [Item.grid, 'G3A']]), # To implement alternate format
    # Materials
    'MAT1': class_factory(Item.mat, ['MAT1', None, [None, 'E'], [None, 'G'], [None, 'NU'], [None, 'RHO'], [None, 'A'], [None, 'TREF'], [None, 'GE'],
                                     [None, 'ST'], [None, 'SC'], [None, 'SS'], [Item.coord, 'MCSID']]),
    'MAT2': class_factory(Item.mat, ['MAT2', None, [None, 'G11'], [None, 'G12'], [None, 'G13'], [None, 'G22'], [None, 'G23'], [None, 'G33'], [None, 'RHO'],
                                     [None, 'A1'], [None, 'A2'], [None, 'A3'], [None, 'TREF'], [None, 'GE'], [None, 'ST'], [None, 'SC'], [None, 'SS'],
                                     [Item.coord, 'MCSID']]),
    'MAT3': class_factory(Item.mat, ['MAT3', None, [None, 'EX'], [None, 'ETH'], [None, 'EZ'], [None, 'NUXTH'], [None, 'NUTHZ'], [None, 'NUZX'], [None, 'RHO'],
                                     None, None, [None, 'GZX'], [None, 'AX'], [None, 'ATH'], [None, 'AZ'], [None, 'TREF'], [None, 'GE']]),
    'MAT8': class_factory(Item.mat, ['MAT8', None, [None, 'E1'], [None, 'E2'], [None, 'NU12'], [None, 'G12'], [None, 'G1Z'], [None, 'G2Z'], [None, 'RHO'],
                                     [None, 'A1'], [None, 'A2'], [None, 'TREF'], [None, 'Xt'], [None, 'Xc'], [None, 'Yt'], [None, 'Yc'], [None, 'S'],
                                     [None, 'GE'], [None, 'F12'], [None, 'STRN']]),
    'MAT9': class_factory(Item.mat, ['MAT9', None, [None, 'G11'], [None, 'G12'], [None, 'G13'], [None, 'G14'], [None, 'G15'], [None, 'G16'], [None, 'G22'],
                                     [None, 'G23'], [None, 'G24'], [None, 'G25'], [None, 'G26'], [None, 'G33'], [None, 'G34'], [None, 'G35'], [None, 'G36'],
                                     [None, 'G44'], [None, 'G45'], [None, 'G46'], [None, 'G55'], [None, 'G56'], [None, 'G66'], [None, 'RHO'], [None, 'A1'],
                                     [None, 'A2'], [None, 'A3'], [None, 'A4'], [None, 'A5'], [None, 'A6'], [None, 'TREF'], [None, 'GE']]),
    # Elements
    'CROD': class_factory(Item.elem, ['CROD', None, [Item.prop, 'PID', 'property'], [Item.grid, 'grids', 'G1'], [Item.grid, 'grids', 'G2']], tag=Tag.e1D),
    'CONROD': class_factory(Item.elem, ['CONROD', None, [Item.grid, 'grids', 'G1'], [Item.grid, 'grids', 'G2'], [Item.mat, 'MID', 'material'], [None, 'A'], [None, 'J'], [None, 'C'], [None, 'NSM']], tag=Tag.e1D),
    'CBAR': class_factory(Item.elem, ['CBAR', None, [Item.prop, 'PID', 'property'], [Item.grid, 'grids', 'GA'], [Item.grid, 'grids', 'GB'], [Item.grid, 'X1', 'G0'], [None, 'X2'], [None, 'X3'], [None, 'OFFT'],
                                      [None, 'PA'], [None, 'PB'], [None, 'W1A'], [None, 'W2A'], [None, 'W3A'], [None, 'W1B'], [None, 'W2B'], [None, 'W3B']], tag=Tag.e1D),
    'CBEAM': class_factory(Item.elem, ['CBEAM', None, [Item.prop, 'PID', 'property'], [Item.grid, 'grids', 'GA'], [Item.grid, 'grids', 'GB'], [Item.grid, 'X1', 'G0'], [None, 'X2'], [None, 'X3'], [None, 'OFFT', 'BIT'],
                                      [None, 'PA'], [None, 'PB'], [None, 'W1A'], [None, 'W2A'], [None, 'W3A'], [None, 'W1B'], [None, 'W2B'], [None, 'W3B'],
                                      [Item.grid, 'SA'], [Item.grid, 'SB']], tag=Tag.e1D),
    'CQUAD4': class_factory(Item.elem, ['CQUAD4', None, [Item.prop, 'PID', 'property'], [Item.grid, 'grids', 'G1'], [Item.grid, 'grids', 'G2'], [Item.grid, 'grids', 'G3'], [Item.grid, 'grids', 'G4'], [Item.coord, 'THETA', 'MCID'], [None, 'ZOFFS'],
                                        None, [None, 'TFLAG'], [None, 'T1'], [None, 'T2'], [None, 'T3'], [None, 'T4']], tag=Tag.e2D),
    'CTRIA3': class_factory(Item.elem, ['CTRIA3', None, [Item.prop, 'PID', 'property'], [Item.grid, 'grids', 'G1'], [Item.grid, 'grids', 'G2'], [Item.grid, 'grids', 'G3'], [Item.coord, 'THETA', 'MCID'], [None, 'ZOFFS'], None,
                                        None, [None, 'TFLAG'], [None, 'T1'], [None, 'T2'], [None, 'T3']], tag=Tag.e2D),
    'CTETRA': class_factory(Item.elem, ['CTETRA', None, [Item.prop, 'PID', 'property'], [Item.grid, 'grids', 'G1'], [Item.grid, 'grids', 'G2'], [Item.grid, 'grids', 'G3'], [Item.grid, 'grids', 'G4'], [Item.grid, 'grids', 'G5'], [Item.grid, 'grids', 'G6'],
                                        [Item.grid, 'grids', 'G7'], [Item.grid, 'grids', 'G8'], [Item.grid, 'grids', 'G9'], [Item.grid, 'grids', 'G10']], tag=Tag.e3D),
    'CPENTA': class_factory(Item.elem, ['CPENTA', None, [Item.prop, 'PID', 'property'], [Item.grid, 'grids', 'G1'], [Item.grid, 'grids', 'G2'], [Item.grid, 'grids', 'G3'], [Item.grid, 'grids', 'G4'], [Item.grid, 'grids', 'G5'], [Item.grid, 'grids', 'G6'],
                                        [Item.grid, 'grids', 'G7'], [Item.grid, 'grids', 'G8'], [Item.grid, 'grids', 'G9'], [Item.grid, 'grids', 'G10'], [Item.grid, 'grids', 'G11'], [Item.grid, 'grids', 'G12'], [Item.grid, 'grids', 'G13'], [Item.grid, 'grids', 'G14'],
                                        [Item.grid, 'grids', 'G15']], tag=Tag.e3D),
    'CHEXA': class_factory(Item.elem, ['CHEXA', None, [Item.prop, 'PID', 'property'], [Item.grid, 'grids', 'G1'], [Item.grid, 'grids', 'G2'], [Item.grid, 'grids', 'G3'], [Item.grid, 'grids', 'G4'], [Item.grid, 'grids', 'G5'], [Item.grid, 'grids', 'G6'],
                                        [Item.grid, 'grids', 'G7'], [Item.grid, 'grids', 'G8'], [Item.grid, 'grids', 'G9'], [Item.grid, 'grids', 'G10'], [Item.grid, 'grids', 'G11'], [Item.grid, 'grids', 'G12'], [Item.grid, 'grids', 'G13'], [Item.grid, 'grids', 'G14'],
                                        [Item.grid, 'grids', 'G15'], [Item.grid, 'grids', 'G16'], [Item.grid, 'grids', 'G17'], [Item.grid, 'grids', 'G18'], [Item.grid, 'grids', 'G19'], [Item.grid, 'grids', 'G20'], ], tag=Tag.e3D),
    'RBE2': class_factory(Item.elem, ['RBE2', None], tag=Tag.eRigid), # To implement
    'RBE3': class_factory(Item.elem, ['RBE3', None], tag=Tag.eRigid), # To implement
    'CBUSH': class_factory(Item.elem, ['CBUSH', None, [Item.prop, 'PID', 'property'], [Item.grid, 'GA'], [Item.grid, 'GB'], [Item.grid, 'X1', 'G0'], [None, 'X2'], [None, 'X3'], [Item.coord, 'CID'],
                                       [None, 'S'], [Item.coord, 'OCID'], [None, 'S1'], [None, 'S2'], [None, 'S3']], tag=Tag.eSpring),
    'CONM2': class_factory(Item.elem, ['CONM2', None, [Item.grid, 'G'], [Item.coord, 'CID'], [None, 'M'], [None, 'X1'], [None, 'X2'], [None, 'X3'], None,
                                       [None, 'I11'], [None, 'I21'], [None, 'I22'], [None, 'I31'], [None, 'I32'], [None, 'I33']], tag=Tag.e0D),
    'PLOTEL': class_factory(Item.elem, ['PLOTEL', None, [Item.grid, 'G1'], [Item.grid, 'G2']], tag=Tag.ePlot),
    # Properties
    'PROD': class_factory(Item.prop, ['PROD', None, [Item.mat, 'MID', 'material'], [None, 'A'], [None, 'J'], [None, 'C'], [None, 'NSM']]),
    'PBAR': class_factory(Item.prop, ['PBAR', None, [Item.mat, 'MID', 'material'], [None, 'A'], [None, 'I1'], [None, 'I2'], [None, 'J'], [None, 'NSM'], None,
                                      [None, 'C1'], [None, 'C2'], [None, 'D1'], [None, 'D2'], [None, 'E1'], [None, 'E2'], [None, 'F1'], [None, 'F2'],
                                      [None, 'K1'], [None, 'K2'], [None, 'I12']]),
    'PBARL': class_factory(Item.prop, ['PBARL', None, [Item.mat, 'MID', 'material'], [None, 'GROUP'], [None, 'TYPE']]), # To implement NSM and DIMs
    'PBEAM': class_factory(Item.prop, ['PBEAM', None, [Item.mat, 'MID', 'material']]), # To implement NSM and DIMs
    'PBEAML': class_factory(Item.prop, ['PBEAML', None, [Item.mat, 'MID', 'material'], [None, 'GROUP'], [None, 'TYPE']]), # To implement NSM and DIMs
    'PSHELL': class_factory(Item.prop, ['PSHELL', None, [Item.mat, 'MID1'], [None, 'T'], [Item.mat, 'MID2'], [None, 'BMIR'], [Item.mat, 'MID3'], [None, 'TST'], [None, 'NSM'],
                                        [None, 'Z1'], [None, 'Z2'], [Item.mat, 'MID4']]),
    'PCOMP': class_factory(Item.prop, ['PCOMP', None]), # To implement
    'PCOMPG': class_factory(Item.prop, ['PCOMPG', None]), # To implement
    'PSOLID': class_factory(Item.prop, ['PSOLID', None, [Item.mat, 'MID', 'material'], [Item.coord, 'CORDM'], [None, 'IN'], [None, 'STRESS'], [None, 'ISOP'], [None, 'FCTN'], [None, 'COROT']]),
    'PBUSH': class_factory(Item.prop, ['PBUSH', None]), # To implement
    # MPCs
    'MPC': class_factory(Set.mpc, ['MPC', None]), # To implement
    'MPCADD': class_factory(Set.mpc, ['MPCADD', None]), # To implement
    # SPCs
    'SPC': class_factory(Set.spc, ['SPC', None]),
    # LOADs
    'FORCE': class_factory(Set.load, ['FORCE', None]),
}

card_factory = CardFactory(card_interfaces)

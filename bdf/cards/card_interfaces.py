from nastran_tools.bdf.cards.card import Card, Item


# card_interfaces = {
#     Item.grid: {
#         'GRID': (None, Item.coord, None, None, None, Item.coord, None, None)
#     }
#     Item.elem: {
#         'CROD': tuple()
#     }
#     Item.prop: {
#         'PBAR': tuple()
#     }
#     Item.mat: {
#         'MAT1': (None, None, None, None, None, None, None, None,
#                  None, None, None, Item.coord),
#         'MAT2': (None, None, None, None, None, None, None, None,
#                  None, None, None, None, None, None, None, None,
#                  Item.coord),
#         'MAT3': (None, None, None, None, None, None, None, None,
#                  None, None, None, None, None, None, None, None),
#         'MAT4': (None, None, None, None, None, None, None, None,
#                  None, None, None, None),
#         'MAT5': (None, None, None, None, None, None, None, None,
#                  None, None),
#         'MAT8': (None, None, None, None, None, None, None, None,
#                  None, None, None, None, None, None, None, None,
#                  None, None, None),
#         'MAT9': (None, None, None, None, None, None, None, None,
#                  None, None, None, None, None, None, None, None,
#                  None, None, None, None, None, None, None, None,
#                  None, None, None, None, None, None, None)
#     }
#     Item.coord: {
#         'CORD2R': (None, Item.coord, None, None, None, None, None, None,
#                    None, None, None),
#         'CORD2C': (None, Item.coord, None, None, None, None, None, None,
#                    None, None, None),
#         'CORD2S': (None, Item.coord, None, None, None, None, None, None,
#                    None, None, None),
#         'CORD1R': (None, Item.grid, Item.grid, Item.grid),
#         'CORD1C': (None, Item.grid, Item.grid, Item.grid),
#         'CORD1S': (None, Item.grid, Item.grid, Item.grid)
#     }
#     Item.load: {
#         'FORCE': tuple()
#     }
#     Item.spc: {
#         'SPC': tuple()
#     }
#     Item.mpc: {
#         'MPC': tuple()
#     }
# }

card_interfaces = {
    Item.grid: {
        'GRID': Card
    },
    Item.coord: {
        'CORD2R': Card,
        'CORD2C': Card,
        'CORD2S': Card,
        'CORD1R': Card,
        'CORD1C': Card,
        'CORD1S': Card
    },
    Item.mat: {
        'MAT1': Card,
        'MAT2': Card,
        'MAT3': Card,
        'MAT4': Card,
        'MAT5': Card,
        'MAT8': Card,
        'MAT9': Card
    },
    Item.elem: {
        'CROD': Card,
        'CONROD': Card,
        'CBAR': Card,
        'CBEAM': Card,
        'CQUAD4': Card,
        'CTRIA3': Card,
        'CHEXA': Card,
        'CPENTA': Card,
        'CTETRA': Card,
        'RBE2': Card,
        'RBE3': Card,
        'CBUSH': Card,
        'CONM2': Card,
        'PLOTEL': Card
    },
    Item.prop: {
        'PROD': Card,
        'PBAR': Card,
        'PBARL': Card,
        'PBEAM': Card,
        'PBEAML': Card,
        'PSHELL': Card,
        'PCOMP': Card,
        'PCOMPG': Card,
        'PSOLID': Card,
        'PBUSH': Card
    },
    Item.load: {
        'FORCE': Card
    },
    Item.spc: {
        'SPC': Card
    },
    Item.mpc: {
        'MPC': Card,
        'MPCADD': Card
    },
}

def card_factory(fields, large_field=False, free_field=False):
    card_type = fields[0]

    for item_type in Item:

        if card_type in card_interfaces[item_type]:
            card = card_interfaces[item_type][card_type](fields, large_field=large_field,
                                                         free_field=free_field)
            card.item_type = item_type
            return card

    return Card(fields, large_field=large_field, free_field=free_field)

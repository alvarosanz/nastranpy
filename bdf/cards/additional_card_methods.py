from nastranpy.bdf.cards.enums import Item, Seq, Tag
from nastranpy.bdf.cards.coord_system import CoordSystem


def CBUSH_settler(self):
    pass

additional_card_methods = {
    'CBUSH': {
        'settle': CBUSH_settler,
    },
}


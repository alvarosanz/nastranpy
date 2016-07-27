from nastranpy.bdf.cards.enums import Seq
from nastranpy.bdf.cards.grid_list import GridList
from nastranpy.bdf.cards.grid_set import GridSet


class Subscheme(object):
    scheme = None

    def __init__(self, fields, card):
        self.fields = list()

        for field, field_info in zip(fields, self.scheme):

            if field_info.seq_type is Seq.list:

                if field_info.update_grid:
                    field = GridList(card, grids=field)
                else:
                    field = list(field)

            elif field_info.seq_type is Seq.set:

                if field_info.update_grid:
                    field = GridSet(card, grids=field)
                else:
                    field = set(field)

            self.fields.append(field)

        self.card = card

    def __iter__(self):
        return iter(self.fields)

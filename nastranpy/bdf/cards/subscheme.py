from nastranpy.bdf.cards.card_list import CardList
from nastranpy.bdf.cards.card_set import CardSet


class Subscheme(object):
    scheme = None

    def __init__(self, fields, card):

        if not fields:
            fields = [None for field_info in self.scheme]

        self.fields = list()

        for field, field_info in zip(fields, self.scheme):

            if field_info.seq_type:

                if field_info.seq_type == 'list':

                    if field_info.type:
                        field = CardList(card, cards=field, update_grid=field_info.update_grid)
                    else:
                        field = list(field)

                elif field_info.seq_type == 'set':

                    if field_info.type:
                        field = CardSet(card, cards=field, update_grid=field_info.update_grid)
                    else:
                        field = set(field)

                elif field_info.seq_type == 'vector':

                    if not field or all(subfield is None for subfield in field):
                        field = None
                    else:

                        if field_info.type == 'grid' and isinstance(field[0], Card):
                            vector = [field[0], None, None]
                        else:
                            vector = [0.0, 0.0, 0.0]
                            vector[:len(field)] = [x if x else 0.0 for x in field]

                        field = np.array(vector)
            else:

                if field_info.type:

                    try:
                        field._subscribe(card)

                        if field_info.update_grid:
                            field.elems.add(card)
                    except AttributeError:
                        pass

            self.fields.append(field)

        self.card = card

    def __repr__(self):
        return '{{{}}}'.format(', '.join(('{}: {}'.format(field_info.name, repr(field)) for
                                          field, field_info in zip(self.fields, self.scheme))))

    def __str__(self):
        return '{{{}}}'.format(', '.join(('{}: {}'.format(field_info.name, str(field)) for
                                          field, field_info in zip(self.fields, self.scheme))))

    def __iter__(self):
        return iter(self.fields)

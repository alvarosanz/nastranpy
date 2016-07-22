from nastranpy.bdf.cards.enums import Item, Set, Tag
from nastranpy.bdf.cards.card import Card
from nastranpy.bdf.cards.set_card import SetCard
from nastranpy.bdf.cards.coord_card import CoordCard
from nastranpy.bdf.cards.grid_card import GridCard


def class_factory(card_name, card_type, fields_pattern, tag=None):

    if card_type in Set:
        cls_parents = (SetCard,)
    elif card_type is Item.coord:
        cls_parents = (CoordCard,)
    elif card_type is Item.grid:
        cls_parents = (GridCard,)
    else:
        cls_parents = (Card,)

    cls = type(card_name, cls_parents, {})
    cls.type = card_type
    cls.tag = tag

    def get_field_factory(index):

        def wrapped(self):
            return self.fields[index]

        return wrapped

    def set_field_factory(index, update_grid):

        def wrapped(self, value):
            self.fields[index] = value

        return wrapped

    item_indexes = list()
    set_indexes = list()
    attributes = dict()

    for index, field_pattern in enumerate(fields_pattern):
        update_grid = False

        try:
            field_type = field_pattern[0]
            update_grid = card_type is Item.elem and 'grids' in field_pattern

            for attr_name in field_pattern[1:]:

                if not attr_name in attributes:
                    attributes[attr_name] = list()

                attributes[attr_name].append((index, field_type, update_grid))

        except TypeError:
            field_type = field_pattern

        if field_type in Item:
            item_indexes.append((index, field_type, update_grid))
        elif field_type in Set:
            set_indexes.append((index, field_type, update_grid))

    setattr(cls, 'items', property(get_items_factory(item_indexes, True),
                                   set_items_factory(item_indexes)))
    setattr(cls, 'sets', property(get_items_factory(set_indexes, True),
                                  set_items_factory(set_indexes)))

    for attr_name, indexes in attributes.items():

        if len(indexes) > 1 or card_type is Item.elem and attr_name == 'grids':
            setattr(cls, attr_name, property(get_items_factory(indexes),
                                             set_items_factory(indexes)))
        else:
            index, field_type, update_grid = indexes[0]
            setattr(cls, attr_name, property(get_field_factory(index),
                                             set_field_factory(index, update_grid)))

    return cls

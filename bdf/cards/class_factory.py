from nastran_tools.bdf.cards.enums import Item, Set, Tag
from nastran_tools.bdf.cards.card import Card
from nastran_tools.bdf.cards.set_card import SetCard


def class_factory(card_type, fields_pattern, tag=None):

    if card_type in Set:
        cls_parents = (SetCard,)
    else:
        cls_parents = (Card,)

    card_name = fields_pattern[0]
    cls = type(card_name, cls_parents, {})
    cls.type = card_type
    cls.tag = tag

    def init_factory(card_name, card_length):

        def wrapped(self, fields, large_field=False, free_field=False):
            fields = fields + ['' for i in range(card_length - len(fields))]
            super(cls, self).__init__(fields, large_field=large_field, free_field=free_field)

        return wrapped

    def get_items_factory(indexes, return_types=False):

        if return_types:
            indexes = [(index, card_type) for index, card_type, update_grid in indexes]

            def wrapped(self):
                return [(self.fields[index], card_type) if
                        self.fields[index] and isinstance(self.fields[index], (int, Card)) else
                        (None, card_type) for index, card_type in indexes]

        else:
            indexes = [index for index, card_type, update_grid in indexes]

            def wrapped(self):
                return [self.fields[index] if
                        self.fields[index] and isinstance(self.fields[index], (int, Card)) else
                        None for index in indexes]

        return wrapped

    def set_items_factory(indexes):

        def wrapped(self, value):

            for (index, card_type, update_grid), new_item in zip(indexes, value):

                if update_grid:
                    old_grid = self.fields[index]

                    if new_item is old_grid:
                        continue

                    try:
                        old_grid.elems.remove(self)
                    except AttributeError:
                        pass

                    try:
                        new_item.elems.add(self)
                    except AttributeError:
                        pass

                self.fields[index] = new_item

        return wrapped

    def get_field_factory(index):

        def wrapped(self):
            return self.fields[index]

        return wrapped

    def set_field_factory(index, update_grid):

        def wrapped(self, value):
            self.fields[index] = value

        return wrapped

    cls.__init__ = init_factory(card_name, len(fields_pattern))

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

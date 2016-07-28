from nastranpy.bdf.cards.enums import Item, Set, Tag, Seq
from nastranpy.bdf.cards.card import Card
from nastranpy.bdf.cards.set_card import SetCard
from nastranpy.bdf.cards.coord_card import CoordCard
from nastranpy.bdf.cards.grid_card import GridCard
from nastranpy.bdf.cards.subscheme import Subscheme
from nastranpy.bdf.cards.padding import Padding
from nastranpy.bdf.misc import get_singular


def class_factory(card_name, card_type, card_scheme=None, card_tag=None, card_padding=None):

    def get_grids_factory(card_scheme):
        fields_info = [(index, field_info) for index, field_info in enumerate(card_scheme) if
                       field_info.update_grid or
                       field_info.subscheme and any(x.update_grid for x in field_info.subscheme.scheme)]

        def wrapped(self):
            grids = set()

            for index, field_info in fields_info:

                if field_info.seq_type:

                    if field_info.subscheme:

                        for subfield in self.fields[index]:

                            for subsubfield, subsubfield_info in zip(subfield, field_info.subscheme.scheme):

                                if subsubfield_info.seq_type:
                                    grids |= set(subsubfield)
                                else:
                                    grids.add(subsubfield)
                    else:
                        grids |= set(self.fields[index])
                else:
                    grids.add(self.fields[index])

            return grids

        return wrapped

    def get_field_factory(index):

        def wrapped(self):
            return self.fields[index]

        return wrapped

    def set_field_factory(index, field_info, is_subfield=False):

        if field_info.update_grid and not field_info.seq_type:

            def wrapped(self, value):
                old_value = self.fields[index]

                if not value is old_value:

                    if is_subfield:
                        card = self.card
                    else:
                        card = self

                    try:
                        old_value.elems.remove(card)
                    except AttributeError:
                        pass

                    try:
                        value.elems.add(card)
                    except AttributeError:
                        pass

                    self.fields[index] = value
        else:

            def wrapped(self, value):
                self.fields[index] = value

        return wrapped

    def add_subscheme_factory(index, field_info):

        def wrapped(self, *args, **kwargs):

            if kwargs:
                args = list()

                for subfield_info in field_info.subscheme.scheme:

                    try:
                        args.append(kwargs[subfield_info.name])
                    except KeyError:
                        args.append(None)

            subscheme = field_info.subscheme(args, self)

            if field_info.seq_type is Seq.list:
                self.fields[index].append(subscheme)
            elif field_info.seq_type is Seq.set:
                self.fields[index].add(subscheme)

        return wrapped

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
    cls.tag = card_tag
    cls.scheme = card_scheme
    cls.optional_scheme = {field_info.name: (index, field_info) for
                           index, field_info in enumerate(card_scheme) if
                           field_info.optional}
    cls.padding = card_padding

    if cls.optional_scheme and not cls.padding:
        cls.padding = Padding()

    if card_scheme:

        for index, field_info in enumerate(card_scheme):

            if field_info.subscheme:
                subscheme_cls = type('{}_{}'.format(card_name, field_info.name), (Subscheme,), {})
                subscheme_cls.scheme = field_info.subscheme
                field_info.subscheme = subscheme_cls

                if field_info.seq_type:
                    setattr(cls, 'add_{}'.format(get_singular(field_info.name)),
                            add_subscheme_factory(index, field_info))

                for subindex, subfield_info in enumerate(field_info.subscheme.scheme):

                    if subfield_info.name:
                        setattr(field_info.subscheme, subfield_info.name,
                                property(get_field_factory(subindex),
                                         set_field_factory(subindex, subfield_info, True)))

            if field_info.name:
                setattr(cls, field_info.name,
                        property(get_field_factory(index),
                                 set_field_factory(index, field_info)))

                if field_info.alternate_name:
                    setattr(cls, field_info.alternate_name,
                            property(get_field_factory(index),
                                     set_field_factory(index, field_info)))

        if card_type is Item.elem and not 'grids' in [x.name for x in card_scheme]:
            setattr(cls, 'grids', property(get_grids_factory(card_scheme)))

    return cls

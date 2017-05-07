import numpy as np
from nastranpy.bdf.cards.card import Card
from nastranpy.bdf.cards.set_card import SetCard
from nastranpy.bdf.cards.coord_card import CoordCard
from nastranpy.bdf.cards.elem_card import ElemCard
from nastranpy.bdf.cards.grid_card import GridCard
from nastranpy.bdf.cards.include_card import IncludeCard
from nastranpy.bdf.cards.vector_card import VectorCard
from nastranpy.bdf.cards.subscheme import Subscheme
from nastranpy.bdf.cards.padding import Padding
from nastranpy.bdf.cards.card_interfaces import card_interfaces
from nastranpy.bdf.cards.card_interfaces_additional import card_interfaces_additional
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

                                if subsubfield_info.update_grid:

                                    if subsubfield_info.seq_type:
                                        grids |= set(subsubfield)
                                    else:
                                        grids.add(subsubfield)
                    else:

                        if field_info.update_grid:
                            grids |= set(self.fields[index])
                else:

                    if field_info.subscheme:

                        for subfield, subfield_info in zip(field, field_info.subscheme.scheme):

                            if subfield_info.update_grid:

                                if subfield_info.seq_type:
                                    grids |= set(subfield)
                                else:
                                    grids.add(subfield)
                    else:

                        if field_info.update_grid:
                            grids.add(self.fields[index])

            return grids

        return wrapped

    def get_field_factory(index, field_info, alternate_name=False):

        if field_info.seq_type == 'vector':

            if field_info.type == 'grid':

                def wrapped(self):
                    vector = self.fields[index]

                    if alternate_name:

                        if not vector is None and isinstance(vector[0], (int, Card)):
                            vector = vector[0]
                        else:
                            vector = None
                    else:

                        if vector is None:
                            vector = np.array([0.0, 0.0, 0.0])
                        elif isinstance(vector[0], Card):
                            vector = None

                    return vector
            else:

                def wrapped(self):
                    vector = self.fields[index]

                    if vector is None:
                        vector = np.array([0.0, 0.0, 0.0])

                    return vector
        else:

            def wrapped(self):
                return self.fields[index]

        return wrapped

    def set_field_factory(index, field_info, is_subfield=False, alternate_name=False):

        if field_info.type:

            if field_info.seq_type:

                if field_info.seq_type == 'vector':

                    def wrapped(self, value):
                        old_value = self.fields[index]

                        if is_subfield:
                            card = self.card
                        else:
                            card = self

                        try:
                            old_value[0]._unsubscribe(card)

                            if field_info.update_grid:
                                old_value[0].elems.remove(card)
                        except (TypeError, AttributeError):
                            pass

                        if value is None:
                            self.fields[index] = None
                        else:
                            vector = value

                            try:
                                value._subscribe(card)
                                vector = [value, None, None]

                                if field_info.update_grid:
                                    value.elems.add(card)
                            except AttributeError:
                                pass

                            self.fields[index] = np.array(vector)
                else:

                    def wrapped(self, value):
                        raise AttributeError("can't set attribute")

            else:

                def wrapped(self, value):
                    old_value = self.fields[index]

                    if not value is old_value:

                        if is_subfield:
                            card = self.card
                        else:
                            card = self

                        try:
                            old_value._unsubscribe(card)

                            if field_info.update_grid:
                                old_value.elems.remove(card)
                        except AttributeError:
                            pass

                        try:
                            value._subscribe(card)

                            if field_info.update_grid:
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

            if field_info.seq_type == 'list':
                self.fields[index].append(subscheme)
            elif field_info.seq_type == 'set':
                self.fields[index].add(subscheme)

        return wrapped

    if card_name in ('FORCE', 'MOMENT'):
        cls_parents = (SetCard, VectorCard,)
    elif card_type in ('mpc', 'spc', 'load'):
        cls_parents = (SetCard,)
    elif card_type == 'coord':
        cls_parents = (CoordCard,)
    elif card_type == 'elem':
        cls_parents = (ElemCard,)
    elif card_type == 'grid':
        cls_parents = (GridCard,)
    elif card_type == 'include':
        cls_parents = (IncludeCard,)
    else:
        cls_parents = (Card,)

    cls = type(card_name, cls_parents, {})
    cls.type = card_type
    cls.tag = card_tag
    cls._scheme = card_scheme

    if card_scheme:
        cls._optional_scheme = {field_info.name: (index, field_info) for
                               index, field_info in enumerate(card_scheme) if
                               field_info.optional}

    cls._padding = card_padding

    if cls._optional_scheme and not cls._padding:
        cls._padding = Padding()

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
                                property(get_field_factory(subindex, subfield_info),
                                         set_field_factory(subindex, subfield_info,
                                                           is_subfield=True)))

            if field_info.name and not hasattr(cls, field_info.name):
                setattr(cls, field_info.name,
                        property(get_field_factory(index, field_info),
                                 set_field_factory(index, field_info)))

            if field_info.alternate_name and not hasattr(cls, field_info.alternate_name):
                setattr(cls, field_info.alternate_name,
                        property(get_field_factory(index, field_info, alternate_name=True),
                                 set_field_factory(index, field_info, alternate_name=True)))

        if card_type == 'elem' and not 'grids' in [x.name for x in card_scheme]:
            setattr(cls, 'grids', property(get_grids_factory(card_scheme)))

    if card_name in card_interfaces_additional:

        for method_name, (function, is_property) in card_interfaces_additional[card_name].items():

            if is_property:
                setattr(cls, method_name, property(function))
            else:
                setattr(cls, method_name, function)

    return cls


card_classes = {card_name: class_factory(*card_interface) for card_name, card_interface in card_interfaces.items()}

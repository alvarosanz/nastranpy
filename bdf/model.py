import os
import csv
from itertools import chain
from nastran_tools.bdf.cards.enums import Item, Set, Tag, str2type, str2tag
from nastran_tools.bdf.cards.card_interfaces import card_factory
from nastran_tools.bdf.include import Include
from nastran_tools.bdf.case_set import CaseSet
from nastran_tools.bdf.misc import sorted_cards, get_plural
from nastran_tools.bdf.cards.filters import filter_factory
from nastran_tools.bdf.id_pattern import IdPattern
from nastran_tools.bdf.object_handling import get_list, get_objects
from nastran_tools.time_tools import timeit


class Model(object):

    def __init__(self):
        self.path = None
        self.includes = dict()
        self.clear()

    def clear(self):
        self.items = {item_type: dict() for item_type in Item}
        self.sets = {set_type: dict() for set_type in Set}
        self.unsupported_cards = set()

        for include in self.includes.values():
            include.clear()

        for item_type in self.items:
            setattr(self, get_plural(item_type.name), self.items[item_type])

        for set_type in self.sets:
            setattr(self, get_plural(set_type.name), self.sets[set_type])

    def add_files(self, files, link_cards=True):
        includes = [Include(file) for file in files]

        for include in includes:
            include.notity = self._update
            self.includes[include.file] = include

        self.read(includes, link_cards)

    @timeit
    def read(self, includes=None, link_cards=True):

        if not includes:
            includes = self.includes.values()
            self.clear()

        includes = get_objects(includes, self.includes)
        os.chdir(self.path)
        print('Model path: {}'.format(self.path))
        counter = 0

        for include in includes:
            counter += 1
            print('\rReading file ({} of {}): {}'.format(str(counter), len(includes), include.file), end='')
            include.read()

            for card in include.cards:
                self._classify_card(card)

        print('\rAll files readed succesfully!')

        if link_cards:
            print('\rLinking cards ...', end='')

            for card in self.cards():
                self._link_card(card)

            print('\rCards linked succesfully!')

    @timeit
    def write(self, includes=None):

        if not includes:
            includes = self.includes.values()

        includes = get_objects(includes, self.includes)
        os.chdir(self.path)
        print('Model path: {}'.format(self.path))
        counter = 0

        for include in includes:
            counter += 1
            print('\rWritting file ({} of {}): {}'.format(str(counter), len(includes), include.file), end='')
            include.write()

        print('\rAll files written succesfully!')

    def _classify_card(self, card):

        if card.type in self.items:
            card.notify = self._update

            if card.id in self.items[card.type]:
                previous_card = self.items[card.type][card.id]
                raise ValueError('There is a conflict between two cards!\nOld card:\n{}\n{}\n\nNew card:\n{}\n{}'.format(
                                    previous_card.include.file, previous_card,
                                    card.include.file, card))

            self.items[card.type][card.id] = card
        elif card.type in self.sets:

            if not card.id in self.sets[card.type]:
                self.sets[card.type][card.id] = CaseSet(card.id, card.type)
                self.sets[card.type][card.id].notify = self._update

            card.set = self.sets[card.type][card.id]
        else:
            self.unsupported_cards.add(card)

    def _link_card(self, card):

        try:

            try:
                card.items = [self.items[type][id] if id else None for id, type in card.items]
                card.sets = [self.sets[type][id] if id else None for id, type in card.sets]
            except KeyError:
                raise KeyError('Cannot link the following card:\n{}'.format(card))

        except AttributeError:
            pass

    def _update(self, caller, **kwargs):

        for key, value in kwargs.items():

            if key == 'new_id':

                if caller.type in self.items:
                    self._update_mapping(self.items[caller.type], caller, caller.id, value,
                                         '{} ID already used!'.format(caller.type.name.upper()))
                elif caller.type in self.sets:
                    self._update_mapping(self.sets[caller.type], caller, caller.id, value,
                                         '{} ID already used!'.format(caller.type.name.upper()))
            elif key == 'new_include_name':
                self._update_mapping(self.includes, caller, caller.file, value,
                                     'Include name already used!')

    @staticmethod
    def _update_mapping(mapping, caller, old_key, new_key, error_message=''):

        if new_key in mapping:
            raise ValueError(error_message)

        if old_key in mapping:
            del mapping[old_key]

        mapping[new_key] = caller

    def cards(self, card_type=None):

        for item_type in self.items:

            if not card_type or card_type is item_type:

                for card in self.items[item_type].values():
                    yield card

        for set_type in self.sets:

            if not card_type or card_type is set_type:

                for case_set in self.sets[set_type].values():

                    for card in case_set.cards:
                        yield card

        if not card_type:

            for card in self.unsupported_cards:
                yield card

    def cards_by_id(self, card_type, card_ids):
        card_type = str2type(card_type)

        if card_type in self.items:
            return (self.items[card_type][card_id] for card_id in card_ids)
        elif card_type in self.sets:
            return (card for card_id in card_ids for card in self.sets[card_type][card_id].cards)

    def cards_by_id_pattern(self, card_type, id_pattern):
        card_type = str2type(card_type)
        id_pattern = IdPattern(id_pattern)
        return (card for card in self.cards(card_type) if card.id in id_pattern)

    def cards_by_type(self, card_types, includes=None):
        card_types = [str2type(card_type) for card_type in get_list(card_types)]

        if includes:
            includes = get_objects(includes, self.includes)
            return (card for include in includes for card in include.cards if
                    card.type in card_types)
        else:
            return (card for card_type in card_types for card in self.cards(card_type))

    def cards_by_tag(self, card_tags, includes=None):
        card_tags = [str2tag(card_tag) for card_tag in get_list(card_tags)]

        if includes:
            includes = get_objects(includes, self.includes)
            return (card for include in includes for card in include.cards if
                    card.tag in card_tags)
        else:
            card_types = card_factory.get_card_types(card_tags)
            return (card for card_type in card_types for card in self.cards(card_type) if
                    card.tag in card_tags)

    def cards_by_name(self, card_names, includes=None):
        card_names = get_list(card_names)

        if includes:
            includes = get_objects(includes, self.includes)
            return (card for include in includes for card in include.cards if
                    card.name in card_names)
        else:
            card_types = card_factory.get_card_types(card_names)
            return (card for card_type in card_types for card in self.cards(card_type) if
                    card.name in card_names)

    def cards_by_include(self, includes):
        includes = get_objects(includes, self.includes)
        return (card for include in includes for card in include.cards)

    def get_unsupported(self):
        return {card.name for card in self.unsupported_cards}

    def get_info(self):

        for item_type in Item:
            print('{}: {}'.format(get_plural(item_type.name).title(), len(self.items[item_type])))

        print('')

        for set_type in Set:
            print('{}: {}'.format(set_type.name.upper() + ' sets', len(self.sets[set_type])))

        if self.unsupported_cards:
            print('\nUnsupported cards: {}\n'.format(len(self.unsupported_cards)))
            print('\t{}'.format(self.get_unsupported()))

        print('\nIncludes: {}'.format(len(self.includes)))
        print('\nModel path: {}'.format(self.path))

    def print_summary(self, file=None):

        if not file:
            os.chdir(self.path)
            file = 'model_summary.csv'

        with open(file, 'w') as f:
            csv_writer = csv.writer(f, lineterminator='\n')
            row = ['Include']

            for item_type in Item:
                item_type_name = get_plural(item_type.name).title()
                row += ['{}: number'.format(item_type_name),
                        '{}: min'.format(item_type_name),
                        '{}: max'.format(item_type_name)]

            csv_writer.writerow(row)

            for include in self.includes.values():
                row = [include.file]

                for item_type in Item:
                    row += include.get_info(item_type)

                csv_writer.writerow(row)

    def print_cards(self, cards, file=None):

        if not file:
            os.chdir(self.path)
            file = 'cards.csv'

        with open(file, 'w') as f:
            csv_writer = csv.writer(f, lineterminator='\n')

            for card in sorted_cards(cards):
                csv_writer.writerow(card.get_fields())

    def renumber(self, card_type, cards=None, start=None, end=None, step=None,
                 id_pattern=None, correlation=None):
        card_type = str2type(card_type)

        if card_type in self.items:
            mapping = self.items[card_type]
        elif card_type in self.sets:
            mapping = self.sets[card_type]

        used_ids = set(mapping)

        if correlation:
            affected_ids = set(correlation)
            assert(affected_ids <= used_ids)
            non_affected_ids = used_ids - affected_ids
            requested_ids = set(correlation.values())
            assert(not (requested_ids & non_affected_ids))

            temp_id = 0
            temp_correlation = dict()

            for old_id, new_id in correlation.items():
                temp_id -= 1
                temp_correlation[temp_id] = new_id
                mapping[old_id].id = temp_id
        else:
            cards = set(cards)
            affected_ids = set((card.id for card in cards))
            non_affected_ids = used_ids - affected_ids

            if id_pattern:
                id_seq = iter(IdPattern(id_pattern))
            else:

                if not end:
                    end = 99999999

                if not step:
                    id_seq = range(start, end)
                else:
                    id_seq = range(start, end, step)

            temp_id = 0
            temp_correlation = dict()

            for card, new_id in zip(cards, id_seq):

                if not card.id in non_affected_ids:
                    temp_id -= 1
                    temp_correlation[temp_id] = new_id
                    card.id = temp_id

        for temp_id, new_id in temp_correlation.items():
            mapping[temp_id].id = new_id

    def move(self, cards, include, move_element_grids=False):
        cards = list(get_list(cards))

        try:
            include = self.includes[include]
        except KeyError:
            pass

        for card in cards:
            card.include = include

            if move_element_grids and card.type is Item.elem:

                for grid in card.grids:
                    grid.include = include

    def extend(self, elements, steps=1, filters=None, extend_all=False, max_steps=10000):

        if extend_all:
            steps = max_steps

        elms = set(get_list(elements))
        elms_diff = elms
        filter_element = filter_factory(filters)

        for i in range(steps):
            elms_ext = {elm_ext for elm in elms_diff for grid in elm.grids for
                        elm_ext in grid.elems if filter_element(elm_ext)}
            elms_diff = elms_ext - elms

            if elms_diff:
                elms |= elms_diff
            else:
                break

        return elms

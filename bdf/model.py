import os
import csv
import logging
from nastranpy.bdf.cards.enums import Item, Set, Tag, str2type, str2tag
from nastranpy.bdf.cards.card_interfaces import card_factory
from nastranpy.bdf.include import Include
from nastranpy.bdf.case_set import CaseSet
from nastranpy.bdf.misc import sorted_cards, get_plural, indent, get_id_info, CallCounted
from nastranpy.bdf.id_pattern import IdPattern
from nastranpy.bdf.object_handling import get_list, get_objects
from nastranpy.time_tools import timeit


class Model(object):
    log = logging.getLogger('nastranpy')
    log.warning = CallCounted(log.warning)
    log.error = CallCounted(log.error)

    def __init__(self):
        self.path = None
        self.includes = dict()
        self.clear()

    def clear(self):
        self.items = {item_type: dict() for item_type in Item}
        self.sets = {set_type: dict() for set_type in Set}
        self.unsupported_cards = set()
        self.warnings = 0
        self.errors = 0

        for include in self.includes.values():
            include.clear()

        for item_type in self.items:
            setattr(self, get_plural(item_type.name), self.items[item_type])

        for set_type in self.sets:
            setattr(self, get_plural(set_type.name), self.sets[set_type])

    def add_files(self, files, link_cards=True):
        includes = [Include(file) for file in files]

        for include in includes:
            include.subscribe(self)
            self.includes[include.file] = include

        self.read(includes, link_cards)

    @timeit
    def read(self, includes=None, link_cards=True):
        self.log.warning.counter = 0
        self.log.error.counter = 0

        if not includes:
            includes = self.includes.values()
            self.clear()

        includes = get_objects(includes, self.includes)
        os.chdir(self.path)
        self.log.info('Model path: {}'.format(self.path))
        counter = 0

        for include in includes:
            counter += 1
            self.log.info('Reading file ({} of {}): {}'.format(str(counter), len(includes), include.file))
            include.read()

            for card in list(include.cards):
                self._classify_card(card)

        self.log.info('All files readed succesfully!')

        if link_cards:
            all_items = {card_type: self.items[card_type] if card_type in self.items else
                         self.sets[card_type] for
                         card_type in list(self.items) + list(self.sets)}
        else:
            all_items = None

        self.log.info('Processing cards ...')

        for card in self.cards():
            card.process_fields(all_items)

        if link_cards:
            self._arrange_grids()

        self.warnings += self.log.warning.counter
        self.errors += self.log.error.counter
        self.log.info('\n' + indent(self.info(print_to_screen=False)) + '\n')

        if self.unsupported_cards:
            self.log.warning('The following cards are not supported:\n{}'.format(indent(', '.join({card.name for card in self.unsupported_cards}))))

        if self.log.error.counter:
            self.log.info("Cards processed with errors! (see 'model.log' for more details)")
        elif self.log.warning.counter:
            self.log.info("Cards processed with warnings! (see 'model.log' for more details)")
        else:
            self.log.info('Cards processed succesfully!')

    @timeit
    def write(self, includes=None):

        if not includes:
            includes = self.includes.values()

        includes = get_objects(includes, self.includes)
        os.chdir(self.path)
        self.log.info('Model path: {}'.format(self.path))
        counter = 0

        for include in includes:
            counter += 1
            self.log.info('Writting file ({} of {}): {}'.format(str(counter), len(includes), include.file))
            include.write()

        self.log.info('All files written succesfully!')

    def _classify_card(self, card):

        if card.type in self.items:
            card.subscribe(self)

            if card.id in self.items[card.type]:
                previous_card = self.items[card.type][card.id]
                self.log.warning('Already existing card (the old one will be overwritten!)\n{}'.format(
                                    indent("\n<== Old card in '{}':\n{}\n\n==> New card in '{}':\n{}\n".format(
                                                    previous_card.include.file, indent(previous_card.head(), 8),
                                                    card.include.file, indent(card.head(), 8)))))

            self.items[card.type][card.id] = card
        elif card.type in self.sets:

            if not card.id in self.sets[card.type]:
                self.sets[card.type][card.id] = CaseSet(card.id, card.type)
                self.sets[card.type][card.id].subscribe(self)

            card.set = self.sets[card.type][card.id]
        else:
            self.unsupported_cards.add(card)

        card.split()

    def _arrange_grids(self):
        resolved_cards = set()
        unresolved_cards = set(self.cards_by_type([Item.grid, Item.coord]))

        while unresolved_cards:
            cards2resolve = set()

            for card in unresolved_cards:

                if all((linked_card in resolved_cards for linked_card in card.cards())):
                    cards2resolve.add(card)

            for card in cards2resolve:
                card.settle()

            if not cards2resolve:
                break

            unresolved_cards -= cards2resolve
            resolved_cards |= cards2resolve

    def update(self, caller, **kwargs):

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
            elif key == 'new_card':

                if not value.type in self.items:
                    raise TypeError('New card instance must be of an item type!')

                mapping = self.items[value.type]

                if value.id in mapping:
                    raise ValueError('{} ID already used!'.format(value.type.name.upper()))

                mapping[value.id] = value

    @staticmethod
    def _update_mapping(mapping, caller, old_key, new_key, error_message=''):

        if new_key in mapping:
            raise ValueError(error_message)

        if not caller is mapping[old_key]:
            raise ValueError('There is a conflict!')

        mapping[new_key] = mapping.pop(old_key)

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

    def elems_by_prop(self, PID):
        return (card for card in self.props[PID].dependent_cards(Item.elem))

    def props_by_mat(self, MID):
        return (card for card in self.mats[MID].dependent_cards(Item.prop))

    def get_unsupported(self):
        return {card.name for card in self.unsupported_cards}

    def info(self, print_to_screen=True):
        info = list()

        for item_type in Item:
            info.append('{}: {}'.format(get_plural(item_type.name).title(), len(self.items[item_type])))

        info.append('')

        for set_type in Set:
            info.append('{}: {}'.format(set_type.name.upper() + ' sets', len(self.sets[set_type])))

        if self.unsupported_cards:
            info.append('\nUnsupported cards: {}\n'.format(len(self.unsupported_cards)))
            info.append('\t{}'.format(self.get_unsupported()))

        info.append('\nIncludes: {}'.format(len(self.includes)))
        info.append('\nModel path: {}'.format(self.path))

        if self.warnings or self.errors:
            info.append('\nWarnings: {}'.format(self.warnings))
            info.append('Errors: {}'.format(self.errors))

        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info

    def get_id_info(self, card_type, detailed=False):
        ids = {card.id for card in self.cards(card_type)}
        return get_id_info(ids, detailed=detailed)

    def print_summary(self, file=None):

        if not file:
            os.chdir(self.path)
            file = 'model_summary.csv'

        with open(file, 'w') as f:
            csv_writer = csv.writer(f, lineterminator='\n')
            row = ['Include']

            for item_type in Item:
                item_type_name = get_plural(item_type.name).title()
                row += [item_type_name,
                        '{}: id min'.format(item_type_name),
                        '{}: id max'.format(item_type_name)]

            csv_writer.writerow(row)

            for include in self.includes.values():
                row = [include.file]

                for item_type in Item:
                    row += include.get_id_info(item_type)

                csv_writer.writerow(row)

    def print_cards(self, cards, file=None):

        if not file:
            os.chdir(self.path)
            file = 'cards.csv'

        with open(file, 'w') as f:
            csv_writer = csv.writer(f, lineterminator='\n')

            for card in sorted_cards(cards):
                csv_writer.writerow(card.get_fields())

    def create_card(self, fields, include, large_field=False, free_field=False):
        card = card_factory.get_card(card, large_field=large_field, free_field=free_field)

        try:
            card.update()
        except AttributeError:
            pass

        self._classify_card(card)

        try:
            card.include = self.includes[include]
        except KeyError:
            card.include = include

    def delete_card(self, card):

        if list(card.dependent_cards()):
            raise ValueError('{} is referred by other card/s!'.format(repr(card)))
        else:

            for linked_card in card.cards():
                linked_card.unsubscribe(card)

            if card.type in self.items:
                del self.items[card.type][card.id]
            elif card.type in self.sets:
                self.sets[card.type][card.id].cards.remove(card)

                if not self.sets[card.type][card.id].cards:
                    del self.sets[card.type][card.id]
            else:
                self.unsupported_cards.remove(card)

            card.include = None

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

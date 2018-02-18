import os
import csv
import logging
from nastranpy.bdf.cards.card_interfaces import item_types, set_types, sorted_cards
from nastranpy.bdf.cards.card_factory import card_factory
from nastranpy.bdf.read_bdf import cards_in_file
from nastranpy.bdf.case_set import CaseSet
from nastranpy.bdf.misc import timeit, get_plural, indent, get_id_info, humansize, CallCounted
from nastranpy.bdf.id_pattern import IdPattern


class Model(object):
    _log = logging.getLogger('nastranpy')
    _log.warning = CallCounted(_log.warning)
    _log.error = CallCounted(_log.error)

    def __init__(self, path=None, link_cards=True):
        """
        Initialize a Model instance.

        Parameters
        ----------
        path : str, optional
            Path of the model.
        link_cards : bool, optional
            Whether or not link cards among each other.
        """
        self.path = path
        self._link_cards = link_cards
        self.clear()

    @property
    def link_cards(self):
        return self._link_cards

    def clear(self):
        """Clear the model."""
        self.items = {item_type: dict() for item_type in item_types}
        self.sets = {set_type: dict() for set_type in set_types}
        self.unsupported_cards = set()
        self.warnings = 0
        self.errors = 0

        for item_type in self.items:
            setattr(self, get_plural(item_type), self.items[item_type])

        for set_type in self.sets:
            setattr(self, get_plural(set_type), self.sets[set_type])

    @timeit
    def read(self, files, card_names=None):
        """
        Read include files.

        Parameters
        ----------
        files : list of str
            List of include filenames.
        card_names : list of str, optional
            List of card names to read (the default is None, which implies that all
            cards will be imported).

        Example:
        --------
        Import all cards:
        >>> model.read(files)

        Import only coordinate system cards (much faster):
        >>> model.read(files, ['CORD2R', 'CORD2C', 'CORD2S', 'CORD1R', 'CORD1C', 'CORD1S'])
        """
        self._log.warning.counter = 0
        self._log.error.counter = 0

        if not self.path:
            model_path = min({os.path.dirname(file) for file in files}, key=len)
            assert all(file.startswith(model_path) for file in files), 'All include files must have a common parent path!'
            self.path = model_path
            files = [file.replace(self.path, '')[1:] for file in files]

        os.chdir(self.path)
        self._log.info('Reading files ...')

        for file in files:
            self._classify_card(card_factory.get_card(['INCLUDE', file]))

            for card in cards_in_file(file, card_names=card_names, generic_cards=False,
                                      logger=self._log):
                self._classify_card(card)

        self._log.info('All files readed succesfully!')

        self._log.info('Processing cards ...')

        if self._link_cards:
            all_items = {card_type: self.items[card_type] if card_type in self.items else
                         self.sets[card_type] for
                         card_type in list(self.items) + list(self.sets)}

            for card in self.cards():
                card._process_fields(all_items)

            self._arrange_grids()
        else:

            for card in self.cards():
                card._process_fields(None)

        self.warnings += self._log.warning.counter
        self.errors += self._log.error.counter
        self._log.info('\n' + indent(self.info(print_to_screen=False)) + '\n')

        if self.unsupported_cards:
            self._log.warning('The following cards are not supported:\n{}'.format(indent(', '.join({card.name for card in self.unsupported_cards}))))

        if self._log.error.counter:
            self._log.info("Cards processed with errors! (see 'model.log' for more details)")
        elif self._log.warning.counter:
            self._log.info("Cards processed with warnings! (see 'model.log' for more details)")
        else:
            self._log.info('Cards processed succesfully!')

    def write(self, includes=None):
        """
        Write include files.

        Parameters
        ----------
        includes : list of str, optional
            List of include filenames(the default is None, which implies all
            model includes will be written).
        """

        if not includes:
            includes = self.includes

        includes = [self.includes[include_name] for include_name in includes]
        os.chdir(self.path)
        self._log.info('Writting files ...')

        for include in includes:
            include.write()

        self._log.info('All files written succesfully!')

    def _classify_card(self, card):

        if card.include:
            card.include = self.includes[card.include]

        if card.type in self.items:
            card._subscribe(self)

            if card.id in self.items[card.type]:
                previous_card = self.items[card.type][card.id]
                self._log.warning('Already existing card (the old one will be overwritten!)\n{}'.format(
                                    indent("\n<== Old card in '{}':\n{}\n\n==> New card in '{}':\n{}\n".format(
                                                    previous_card.include, indent(previous_card.head(), 8),
                                                    card.include, indent(card.head(), 8)))))

            self.items[card.type][card.id] = card
        elif card.type in self.sets:

            if not card.id in self.sets[card.type]:
                self.sets[card.type][card.id] = CaseSet(card.id, card.type)
                self.sets[card.type][card.id]._subscribe(self)

            card.set = self.sets[card.type][card.id]
        else:
            self.unsupported_cards.add(card)

        card._split()

    def _arrange_grids(self):
        resolved_cards = set()
        unresolved_cards = set(self._cards(['grid', 'coord']))

        while unresolved_cards:
            cards2resolve = set()

            for card in unresolved_cards:

                if all((parent_card in resolved_cards for parent_card in card.parent_cards())):
                    cards2resolve.add(card)

            for card in cards2resolve:
                card._settle()

            if not cards2resolve:
                break

            unresolved_cards -= cards2resolve
            resolved_cards |= cards2resolve

    def _update(self, caller, **kwargs):

        for key, value in kwargs.items():

            if key == 'new_id':

                if caller.type in self.items:
                    self._update_mapping(self.items[caller.type], caller, caller.id, value,
                                         '{} ID already used!'.format(caller.type.upper()))
                elif caller.type in self.sets:
                    self._update_mapping(self.sets[caller.type], caller, caller.id, value,
                                         '{} ID already used!'.format(caller.type.upper()))
            elif key == 'new_include_name':
                self._update_mapping(self.includes, caller, caller.file, value,
                                     'Include name already used!')
            elif key == 'new_card':

                if not value.type in self.items:
                    raise TypeError('New card instance must be of an item type!')

                mapping = self.items[value.type]

                if value.id in mapping:
                    raise ValueError('{} ID already used!'.format(value.type.upper()))

                mapping[value.id] = value

    @staticmethod
    def _update_mapping(mapping, caller, old_key, new_key, error_message=''):

        if new_key in mapping:
            raise ValueError(error_message)

        if not caller is mapping[old_key]:
            raise ValueError('There is a conflict!')

        mapping[new_key] = mapping.pop(old_key)

    def cards(self, card_filters=None, card_ids=None, includes=None):
        """
        Get cards by specifying different queries.

        Parameters
        ----------
        card_filters : str or list of str, optional
            Any combination of the following options are available (the default is None,
            which implies all model cards are considered):

            Card types: 'include', 'coord', 'elem', 'grid', 'mat', 'prop', 'mpc', 'spc' or 'load'
            Card tags: 'e1D', 'e2D', 'e3D', 'eRigid', 'eSpring', 'eMass' or 'ePlot'
            Card names: 'GRID', 'CBAR', 'CORD1R', 'CQUAD4', 'PSHELL', etc ...

        card_ids : list of int (or list of str), optional
            List of card ids (or pattern of the id digits). The default is None,
            which implies all model cards are considered.
        includes : str or list of str, optional
            Include filename/s (the default is None, which implies all model cards
            are considered).


        Yields
        -------
        Card
            Card object.

        Examples
        --------
        Get grids by ids:
        >>> grids = [grid for grid in model.cards('grid', [34, 543453, 234233])]

        Get elements by an ID pattern:
        >>> elems = [elem for elem in model.cards('elem', ['9', '34', '*', '*', '*', '*', '1-8'])]

        Get all CQUAD4 and CTRIA cards:
        >>> elems = [elem for elem in model.cards(['CQUAD4', 'CTRIA3']]

        Get all shell element cards in a set includes:
        >>> elems = [elem for elem in model.cards('e2D', includes=['3C0734_Sp1_Hng_outbd_v04.bdf',
                                                                   'SMM3v2_S541700_Wing-Box_V16.2_08.bdf'])]
        """
        card_types = None
        card_tags = None
        card_names = None

        if card_filters:

            if isinstance(card_filters, str):
                card_filters = [card_filters]

            card_types = {card_type for card_type in card_filters if
                          card_type in self.items or card_type in self.sets}
            card_tags = {card_tag for card_tag in card_filters if
                         card_tag in card_factory.tags2types}
            card_names = {card_name for card_name in card_filters if
                          card_name in card_factory.names2types}
            card_types |= {card_factory.tags2types[card_tag] for card_tag in card_tags}
            card_types |= {card_factory.names2types[card_name] for card_name in card_names}

        by_ids = False

        if card_ids:

            if not isinstance(card_ids, str) and isinstance(card_ids[0], str):
                card_ids = IdPattern(card_ids)
            elif len(card_types) == 1:
                card_type = card_types.pop()

                if card_type in self.items:
                    yield from (self.items[card_type][card_id] for card_id in card_ids)
                elif card_type in self.sets:
                    yield from (card for card_id in card_ids for card in self.sets[card_type][card_id].cards)

                return
            else:
                card_ids = set(card_ids)
                by_ids = True

        if includes:

            yield from (card for include in includes for card in self.includes[include].cards if
                        (not card_types or card.type in card_types) and
                        (not card_tags or card.tag in card_tags) and
                        (not card_names or card.name in card_names) and
                        (not card_ids or card.id in card_ids))

        else:

            if by_ids:
                yield from (card for card in self._cards(card_types, card_ids) if
                            (not card_tags or card.tag in card_tags) and
                            (not card_names or card.name in card_names))

            else:
                yield from (card for card in self._cards(card_types) if
                            (not card_tags or card.tag in card_tags) and
                            (not card_names or card.name in card_names) and
                            (not card_ids or card.id in card_ids))

    def _cards(self, card_types=None, card_ids=None):

        for item_type in self.items:

            if not card_types or item_type in card_types:

                if card_ids:

                    for card_id in card_ids:
                        yield self.items[item_type][card_id]

                else:
                    yield from self.items[item_type].values()

        for set_type in self.sets:

            if not card_types or set_type in card_types:

                if card_ids:

                    for card_id in card_ids:
                        yield from self.sets[set_type][card_id].cards

                else:

                    for case_set in self.sets[set_type].values():
                        yield from case_set.cards

        if not card_types and not card_ids:
            yield from self.unsupported_cards

    @property
    def log_path(self):
        return [log_handler for log_handler in self._log.handlers if
                log_handler._name == 'model_log_file'][0].baseFilename

    def info(self, print_to_screen=True):
        """
        Get a brief model summary.

        Parameters
        ----------
        print_to_screen : bool, optional
            Whether or not print the info to screen.

        Returns
        -------
        str or None
            Brief model summary (None if `print_to_screen` is True).
        """
        info = list()

        for item_type in (item_type for item_type in item_types if item_type != 'include'):
            info.append('{}: {}'.format(get_plural(item_type).title(), len(self.items[item_type])))

        info.append('')

        for set_type in set_types:
            info.append('{}: {}'.format(set_type.upper() + ' sets', len(self.sets[set_type])))

        if self.unsupported_cards:
            info.append('\nUnsupported cards: {}\n'.format(len(self.unsupported_cards)))
            info.append('\t{}'.format({card.name for card in self.unsupported_cards}))

        info.append('\nIncludes: {}'.format(len(self.includes)))
        info.append('\nModel path: {}'.format(self.path))
        info.append('\nModel log: {}'.format(self.log_path))

        if self.warnings or self.errors:
            info.append('\nWarnings: {}'.format(self.warnings))
            info.append('Errors: {}'.format(self.errors))

        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info

    def get_id_info(self, card_type, detailed=False):
        """
        Get information about the used ids of the specified type.

        Parameters
        ----------
        card_type : {'coord', 'elem', 'grid', 'mat', 'prop', 'mpc', 'spc', 'load'}
            Card type.
        detailed : bool, optional
            Whether or not return the available ids of the specified type.

        Returns
        -------
        list of int
            Information about the used ids of the specified type.
                `detailed` is True:
                    [n_cards, id_min, id_max, free_slots]
                `detailed` is False:
                    [n_cards, id_min, id_max]

        Examples
        --------
        >>> model.get_id_info('mpc')
        [5, 10, 1002]

        >>> model.get_id_info('mpc', detailed=True)
        [5, 10, 1002, [(1, 9), (11, 19), (21, 999), (1003, 99999999)]]
        """
        ids = {card.id for card in self.cards(card_type)}
        return get_id_info(ids, detailed=detailed)

    def get_id_slot(self, card_type, min_size, id_pattern=None):
        """
        Get a slot for available ids for a given card type.

        Parameters
        ----------
        card_type : {'coord', 'elem', 'grid', 'mat', 'prop', 'mpc', 'spc', 'load'}
            Card type.
        min_size : int
            Minimum size for the id slot.
        id_pattern : list of str, optional
            Pattern of the id digits.

        Returns
        -------
        tuple of int or None
            Minimum and maximum available id for that given slot:
                (free_id_min, free_id_max)
            Returns None if no slot is found.

        Examples
        --------
        >>> model.get_id_slot('grid', 1000)
        (200999, 202000)

        >>> model.get_id_slot('grid', 1000, ['4', '7', '*', '*', '*', '*', '*'])
        (4703436, 4738579)
        """

        if id_pattern:
            id_pattern = IdPattern(id_pattern)

        free_slots = dict()

        for free_slot in self.get_id_info(card_type, detailed=True)[3]:
            slot_size = free_slot[1] - free_slot[0] + 1

            if slot_size in free_slots:
                free_slots[slot_size].append(free_slot)
            else:
                free_slots[slot_size] = [free_slot]

        for slot_size in sorted(free_slots):

            for free_slot in free_slots[slot_size]:

                if (slot_size >= min_size and
                    (not id_pattern or
                     id_pattern and all((id in id_pattern for id in
                                         range(free_slot[0], free_slot[1] + 1))))):
                    return free_slot

        return None

    def print_summary(self, file=None):
        """
        Print to a .csv file detailed information about the used ids (one row per include).

        Parameters
        ----------
        file : str, optional
            Output filename (default is None, which implies that the output filename
            will be 'model_summary.csv').
        """

        if not file:
            os.chdir(self.path)
            file = 'model_summary.csv'

        with open(file, 'w') as f:
            csv_writer = csv.writer(f, lineterminator='\n')
            row = ['Include']

            for item_type in (item_type for item_type in item_types if
                              item_type != 'include'):
                item_type_name = get_plural(item_type).title()
                row += [item_type_name,
                        '{}: id min'.format(item_type_name),
                        '{}: id max'.format(item_type_name)]

            csv_writer.writerow(row)

            for include in self.includes.values():
                row = [include.file]

                for item_type in (item_type for item_type in item_types if
                                  item_type != 'include'):
                    row += include.get_id_info(item_type)

                csv_writer.writerow(row)

    def print_cards(self, cards, file=None):
        """
        Print to a .csv file all the fields of the specified cards (one row per card).

        Parameters
        ----------
        cards : list of Card
            List of cards.
        file : str, optional
            Output filename (default is None, which implies that the output filename
            will be 'cards.csv').
        """

        if not file:
            os.chdir(self.path)
            file = 'cards.csv'

        with open(file, 'w') as f:
            csv_writer = csv.writer(f, lineterminator='\n')

            for card in sorted_cards(cards):
                csv_writer.writerow(card.get_fields())

    def create_card(self, fields, include, large_field=False, free_field=False):
        """
        Create a new card in the database.

        Parameters
        ----------
        fields : list of int, float or str
            List of fields.
        include : str
            Include filename.
        large_field : bool, optional
            Use large-field format.
        free_field : bool, optional
            Use free-field format.
        """
        card = card_factory.get_card(card, large_field=large_field, free_field=free_field)

        try:
            card._settle()
        except AttributeError:
            pass

        self._classify_card(card)
        card.include = self.includes[include]

    def delete_card(self, card):
        """
        Delete card from the database.

        Parameters
        ----------
        card : Card
            Card to be deleted.
        """

        if list(card.child_cards()):
            raise ValueError('{} is referred by other card/s!'.format(repr(card)))
        else:
            self._delete_card(card)

    def _delete_card(self, card):

        for parent_card in card.parent_cards():
            parent_card._unsubscribe(card)

        if card.type in self.items:
            del self.items[card.type][card.id]
        elif card.type in self.sets:
            self.sets[card.type][card.id].cards.remove(card)

            if not self.sets[card.type][card.id].cards:
                del self.sets[card.type][card.id]
        else:
            self.unsupported_cards.remove(card)

        card.include = None

    def get_unused_cards(self, card_type):
        """
        Get cards not referred by other cards.

        Parameters
        ----------
        card_type : {'coord', 'elem', 'grid', 'mat', 'prop', 'mpc', 'spc', 'load'}
            Card type.

        Returns
        -------
        set of Card
            Cards not referred by other cards.
        """
        return {card for card in self.cards(card_type) if not card.child_cards()}

    def delete_unused_cards(self, card_type):
        """
        Delete cards not referred by other cards.

        Parameters
        ----------
        card_type : {'coord', 'elem', 'grid', 'mat', 'prop', 'mpc', 'spc', 'load'}
            Card type.
        """
        unused_cards = self.get_unused_cards(card_type)

        for card in unused_cards:
            self._delete_card(card)

        self._log.info("{} unused cards of type '{}' deleted".format(len(unused_cards), card_type))

    def renumber(self, card_type, cards=None, start=None, step=None,
                 id_pattern=None, correlation=None):
        """
        Renumber cards of the specified type.

        Parameters
        ----------
        card_type : {'coord', 'elem', 'grid', 'mat', 'prop', 'mpc', 'spc', 'load'}
            Card type.
        cards : list of Card, optional
            List of cards to be ranamed. If not cards are supplied then a correlation must be supplied.
        start : int, optional
            First id.
            Only applicable if no correlation is supplied.
        step : int, optional
            Id step. If not supplied then step = 1.
            Only applicable if no correlation is supplied.
        id_pattern : list of str, optional
            Pattern of the id digits. The cards will be renumbered using the id pattern supplied.
            Only applicable if no correlation is supplied.
        correlation : dict of int, optional
            Correlation of ids (old_id: new_id).

        Examples
        --------
        >>> model.renumber('grids', grids, start=5000, step=5)

        >>> model.renumber('grids', grids, id_pattern=['9', '34', '*', '*', '*', '*', '1-8'])

        >>> model.renumber('grids', correlation={5001:9005001, 5002:9005002, 5003:9005003, 5004:9005004})
        """

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

                if not step:
                    id_seq = range(start, 99999999)
                else:
                    id_seq = range(start, 99999999, step)

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
        """
        Move cards to the specified include.

        Parameters
        ----------
        cards : list of Card
            List of cards to be moved.
        include : str
            Filename of the target include.
        move_elemen_grids : bool, optional
            Whether or not move associated element grids.
        """
        include = self.includes[include]

        for card in set(cards):
            card.include = include

            if move_element_grids and card.type == 'elem':

                for grid in card.grids:
                    grid.include = include

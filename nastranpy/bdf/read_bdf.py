import os
import re
from nastranpy.bdf.cards.card_factory import card_factory
from nastranpy.bdf.cards.card import Card


def cards_in_file(file, card_names=None, raw_output=False, only_ids=False, ignore_comments=False,
                  generic_cards=True, logger=None):
    """
    Get cards in file.

    Parameters
    ----------
    file : str
        Include file path.
    card_names : list of str, optional
        Card names to read. Other cards will be ignored (the default is None, which
        implies all cards will be readed).
    only_ids : bool, optional
        Use this if only it is necessary to know the id of each card.
    raw_output : bool, optional
        Whether or not process card fields.
    ignore_comments : bool, optional
        Whether or not to ignore the comments in the file.
    generic_cards : bool, optional
        Whether or not to return a generic Card or the corresponding card object
        (i.e. GRID card).
    logger : Logger Object

    Yields
    -------
    Card or list of int, float or str
            `only_ids` is True:
                [card_name (str), card_id (int)]
            `raw_output` is False:
                Card
            `raw_output` is True:
                [field1 (str), field2 (str), field3 (str), ...]

    Examples
    --------
    >>> grids = [grid for grid in cards_in_file(f, ['GRID'])]
    """

    if card_names:
        card_names = set(card_names)

    card = list()
    comment = ''

    with open(file) as f:

        for line in f:
            if re.search('^ *\$', line):
                comment += line
            else:
                line = re.sub(' *\$.*$', '', line)

                if re.search('^[a-zA-Z]', line):

                    if card:

                        if is_free_field:
                            card = get_fields_from_free_field_string(card)

                        if not raw_fields:
                            card = process_fields(card, not raw_output)

                        if raw_output:
                            yield card
                        else:

                            if generic_cards:
                                card = Card(card, large_field=is_large_field, free_field=is_free_field)
                            else:
                                card = card_factory.get_card(card, large_field=is_large_field, free_field=is_free_field)

                            card.include = file
                            card.comment = card_comment
                            yield card

                            if card.name == 'INCLUDE':

                                try:

                                    for card_in_file in cards_in_file(os.path.join(os.path.dirname(file), card.fields[1]),
                                                                      card_names, raw_output, only_ids, ignore_comments,
                                                                      generic_cards, logger):
                                        yield card_in_file

                                except FileNotFoundError:

                                    if logger:
                                        logger.warning("No such file: '{}'".format(card.fields[1]))
                                    else:
                                        raise

                    if ignore_comments:
                        card_comment = ''
                    else:
                        card_comment = comment

                    comment = ''
                    card = list()
                    fields_from_empty_lines = list()
                    card_name = line[:8].strip()
                    card_id = line[8:16].strip()
                    is_free_field = False
                    is_large_field = False
                    field_length = 8
                    n_fields = 8
                    raw_fields = False

                    if ',' in line[:8]: # Free-field format
                        is_free_field = True
                        card_name, card_id = line.split(',')[:2]

                    if card_name[-1] == '*': # Large-field format
                        is_large_field = True
                        field_length = 16
                        n_fields = 4
                        card_name = card_name[:-1]
                        card_id = line[8:24].strip()

                    card_name = card_name.upper()

                    if card_name == 'INCLUDE':
                        card = [card_name, line[8:-1].replace("'", "").strip()]
                        raw_fields = True
                        continue

                    if not card_names or card_name in card_names:

                        if only_ids:
                            yield [card_name, int(card_id)]
                            continue

                        if is_free_field:
                            card = line[:-1]
                        else:
                            card = [card_name]

                            for i in range(n_fields):
                                card.append(line[:-1][8 + i * field_length:8 + (i + 1) * field_length])

                elif card:

                    if card_name == 'INCLUDE':
                        card[-1] += line[:-1].replace("'", "").strip()
                        continue

                    if not line[:-1].strip():
                        fields_from_empty_lines += ['' for i in range(8)]
                        continue

                    comment = ''

                    if line[0] == '*':
                        is_large_field = True
                        field_length = 16
                        n_fields = 4
                    else:
                        field_length = 8
                        n_fields = 8

                    if is_free_field:
                        card += line[:-1]
                    else:
                        card += fields_from_empty_lines
                        fields_from_empty_lines = list()

                        for i in range(n_fields):
                            card.append(line[:-1][8 + i * field_length:8 + (i + 1) * field_length])

        if card:

            if is_free_field:
                card = get_fields_from_free_field_string(card)

            if not raw_fields:
                card = process_fields(card, not raw_output)

            if raw_output:
                yield card
            else:

                if generic_cards:
                    card = Card(card, large_field=is_large_field, free_field=is_free_field)
                else:
                    card = card_factory.get_card(card, large_field=is_large_field, free_field=is_free_field)

                card.include = file
                card.comment = card_comment
                yield card

                if card.name == 'INCLUDE':

                    try:

                        for card_in_file in cards_in_file(os.path.join(os.path.dirname(file), card.fields[1]),
                                                          card_names, raw_output, only_ids, ignore_comments,
                                                          generic_cards, logger):
                            yield card_in_file

                    except FileNotFoundError:

                        if logger:
                            logger.warning("No such file: '{}'".format(card.fields[1]))
                        else:
                            raise


def get_fields_from_free_field_string(free_field_string):
    fields = [field for field in (x.strip() for x in free_field_string.split(',')) if
              not field[:1] in ('+', '*')]

    if fields[0][-1] == '*':
        fields[0] = fields[0][:-1]

    return fields


def process_fields(fields, convert_to_numbers):
    processed_fields = list()

    for field in fields:
        field = field.strip().upper()
        nastran_exp_match = re.search('(.+[^E])([\+-].+)', field)

        if (nastran_exp_match):
            field = nastran_exp_match.group(1) + 'E' + nastran_exp_match.group(2)

        if convert_to_numbers:

            if '.' in field:
                field = float(field)
            else:

                try:
                    field = int(field)
                except ValueError:
                    pass

        processed_fields.append(field)

    return processed_fields

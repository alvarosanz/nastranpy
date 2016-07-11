import re
from nastran_tools.bdf.cards.card import Card

def cards_in_file(file, card_types=[], raw_output=False, only_ids=False):
    card = list()
    card_type = ''
    item_id = 0
    is_free_field = False
    is_large_field = False
    field_length = 8
    n_fields = 8

    for line in file:

        if re.search('^[a-zA-Z]', line):

            if card:
                card = process_fields(card, not raw_output)

                if not raw_output:
                    card = Card(card)

                yield card
                card = list()

            if ',' in line[:8]: # Free-field format
                is_free_field = True
                fields = line.split(',')
                card_type = fields[0].upper()
                item_id = fields[1]
            elif line[:8].strip()[-1] == '*': # Large-field format
                is_free_field = False
                is_large_field = True
                field_length = 16
                n_fields = 4
                card_type = line[:8].strip()[:-1].upper()
                item_id = line[8:24].strip()
            else:  # Small-field format
                is_free_field = False
                is_large_field = False
                field_length = 8
                n_fields = 8
                card_type = line[:8].strip().upper()
                item_id = line[8:16].strip()

            if not card_types or card_type in card_types:

                if only_ids:
                    yield [card_type, int(item_id)]
                    continue

                if is_free_field:
                    card = line[:-1].split(',')
                else:
                    card = [card_type]

                    for i in range(n_fields):
                        card.append(line[:-1][8 + i * field_length:8 + (i + 1) * field_length])

        elif card and not re.search('^ *\$', line) and len(line[:-1].strip()):

            if is_free_field:
                card += line[:-1].split(',')[1:]
            else:

                for i in range(n_fields):
                    card.append(line[:-1][8 + i * field_length:8 + (i + 1) * field_length])

    if card:
        card = process_fields(card, not raw_output)

        if not raw_output:
            card = Card(card)

        yield card


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

from nastranpy.bdf.cards.card_interfaces import item_types, set_types, tag_types, card_interfaces
from nastranpy.bdf.id_pattern import IdPattern


def filter_factory(card_filters):

    if isinstance(card_filters, str):
        card_filters = [card_filters]

    card_types = {card_type for card_type in card_filters if
                  card_type in item_types or card_type in set_types}
    card_tags = {card_tag for card_tag in card_filters if card_tag in tag_types}
    card_names = {card_name for card_name in card_filters if card_name in card_interfaces}

    if (all(isinstance(x, str) for x in card_filters) and
        all(char in '0123456789-*' for x in card_filters for char in x)):
        id_pattern = IdPattern(card_filters)
    else:
        id_pattern = None

    def wrapped(card):

        if card_types and not card.type in card_types:
            return False

        if card_tags and not card.tag in card_tags:
            return False

        if card_names and not card.name in card_names:
            return False

        if id_pattern and not card.id in id_pattern:
            return False

        return True

    return wrapped

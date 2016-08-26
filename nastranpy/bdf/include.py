from nastranpy.bdf.observable import Observable
from nastranpy.bdf.cards.card_interfaces import item_types, set_types, sorted_cards
from nastranpy.bdf.misc import get_plural, get_id_info, assure_path_exists


def iter_items_factory(card_type):

    def wrapped(self):
        return (card for card in self.cards if card.type == card_type)

    return wrapped


class Include(Observable):

    def __init__(self, file=None):
        super().__init__()
        self._file = file
        self.id_pattern = None
        self.clear()

    def clear(self):
        self.cards = set()
        self.commentted_cards = set()

    def __repr__(self):
        return repr(self.file)

    def __str__(self):
        return str(self.file)

    @property
    def file(self):
        return self._file

    @file.setter
    def file(self, value):

        if self._file != value:
            self.changed = True
            self._notify(new_include_name=value)
            self._file = value

    def get_id_info(self, card_type, detailed=False):
        ids = {card.id for card in self.cards if card.type == card_type}
        return get_id_info(ids, detailed=detailed)

    def write(self):
        assure_path_exists(self._file)

        with open(self._file, 'w') as f:

            for card in sorted_cards(self.commentted_cards):
                f.write(card.print(is_commented=True, comment_symbol='$ -> ') + '\n')

            for card in sorted_cards(self.cards):
                f.write(card.print() + '\n')

    def clear_commented_cards(self):
        self.commentted_cards.clear()

    def is_self_contained(self):
        return all((linked_card in self.cards or
                    linked_card in self.commentted_cards) for
                    card in self.cards for linked_card in card.cards())

    def make_self_contained(self, move_cards=False):
        cards = self.cards.copy()
        cards_diff = cards

        for i in range(10):
            cards_ext = {linked_card for card in cards_diff for linked_card in card.cards()}
            cards_diff = cards_ext - cards

            if cards_diff:
                cards |= cards_diff
            else:
                cards2add = cards - self.cards
                break

        if not move_cards:
            self.commentted_cards = cards2add
        else:

            for card in cards2add:
                card.include = self


for card_type in list(item_types) + list(set_types):
    setattr(Include, get_plural(card_type), iter_items_factory(card_type))

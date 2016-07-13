from nastran_tools.bdf.cards.card import Item
from nastran_tools.bdf.read_bdf import cards_in_file

class Include(object):

    def __init__(self, file=None):
        self.file = file
        self.cards = set()

    def get_info(self, item_type):
        ids = [card.id for card in self.cards if card.item_type is item_type]

        if ids:
            return len(ids), min(ids), max(ids)
        else:
            return 0, None, None

    def read(self):

        with open(self.file) as f:

            for card in cards_in_file(f):
                card.include = self

    def write(self):

        with open(self.file, 'w') as f:

            for card in sorted(self.cards, key=lambda card: ('99' if card.item_type is None else
                                                             str(card.item_type.value).ljust(2, '0')) +
                                                            card.type.ljust(8, '0') +
                                                            str(card.id).rjust(8, '0')):
                f.write(str(card))

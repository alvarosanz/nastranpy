import os
from nastran_tools.bdf.cards.card import Item
from nastran_tools.bdf.include import Include


class Model(object):

    def __init__(self):
        self.path = None
        self.cards = {item_type: dict() for item_type in Item}
        self.unsupported_cards = set()
        self.includes = list()

    def read(self, link_items=True):
        os.chdir(self.path)
        print('Model path: {}'.format(self.path))
        counter = 0

        for include in self.includes:
            counter += 1
            print('Reading file ({} of {}): {}'.format(str(counter), len(self.includes), include.file))
            include.read()

            for card in include.cards:
                card.notify = self.update

                if card.item_type is None:
                    self.unsupported_cards.add(card)
                else:
                    self.cards[card.item_type][card.id] = card

        if link_items:
            pass

    def write(self):
        os.chdir(self.path)
        print('Model path: {}'.format(self.path))
        counter = 0

        for include in self.includes:
            counter += 1
            print('Writting file ({} of {}): {}'.format(str(counter), len(self.includes), include.file))
            include.write()

    def update(self, item, **kwargs):

        for key, value in kwargs.items():

            if key == 'new_id':

                if not item.item_type is None:

                    if value in self.cards[item.item_type]:
                        raise ValueError('ID already used!')

                    self.cards[item.item_type].pop(item.id)
                    self.cards[item.item_type][value] = item


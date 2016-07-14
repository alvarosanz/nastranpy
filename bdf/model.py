import os
import csv
from nastran_tools.bdf.cards.card import Item
from nastran_tools.bdf.include import Include


class Model(object):

    def __init__(self):
        self.path = None
        self.cards = {item_type: dict() for item_type in Item}
        self.unsupported_cards = set()
        self.includes = list()

        for item_type in Item:
            setattr(self, item_type.name + 's', self.cards[item_type])

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

    def get_unsupported(self):
        return {card.type for card in self.unsupported_cards}

    def get_info(self):
        print('Model path: {}\n'.format(self.path))
        print('Includes: {}\n'.format(len(self.includes)))

        for item_type in Item:
            print('{}: {}'.format((item_type.name + 's').title(), len(self.cards[item_type])))

        if self.unsupported_cards:
            print('\nUnsupported cards: {}\n'.format(len(self.unsupported_cards)))
            print('\t{}\n'.format(self.get_unsupported()))


    def print_summary(self, file=None):

        if not file:
            os.chdir(self.path)
            file = 'model_summary.csv'

        with open(file, 'w') as f:
            csv_writer = csv.writer(f, lineterminator='\n')
            row = ['Include']

            for item_type in Item:
                item_type_name = (item_type.name + 's').title()
                row += ['{}: number'.format(item_type_name),
                        '{}: min'.format(item_type_name),
                        '{}: max'.format(item_type_name)]

            csv_writer.writerow(row)

            for include in self.includes:
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

            for card in cards:
                csv_writer.writerow(card.get_fields())



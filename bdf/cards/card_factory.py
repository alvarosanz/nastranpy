from nastranpy.bdf.cards.card import Card


class CardFactory(object):

    def __init__(self, card_interfaces):
        self.card_interfaces = card_interfaces
        self.names2types = {card_name: cls.type for card_name, cls in card_interfaces.items()}
        self.tags2types = {cls.tag: cls.type for cls in card_interfaces.values()}

    def get_card(self, fields, large_field=False, free_field=False):

        try:
            return self.card_interfaces[fields[0]](fields, large_field=large_field, free_field=free_field)
        except KeyError:
            return Card(fields, large_field=large_field, free_field=free_field)

    def get_card_types(self, keys):

        try:
            return [self.names2types[key] for key in keys]
        except KeyError:
            pass

        try:
            return [self.tags2types[key] for key in keys]
        except KeyError:
            pass

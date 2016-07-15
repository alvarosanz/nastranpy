def sorted_cards(cards):
    return sorted(cards, key=lambda card: ('99' if card.type is None else
                                           str(card.type.value).ljust(2, '0')) +
                                          card.name.ljust(8, '0') +
                                          str(card.id).rjust(8, '0'))


def get_plural(name):

    if name[-1] == 'y':
        return name[:-1] + 'ies'
    elif name[-1] == 's':
        return name + 'es'
    else:
        return name + 's'

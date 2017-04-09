from nastranpy.bdf.model import Model
from nastranpy.bdf.cards.card import Card
from nastranpy.bdf.cards.class_factory import card_classes
from nastranpy.bdf.read_bdf import cards_in_file
from nastranpy.bdf.write_bdf import print_card
from nastranpy.setup_logging import setup_logging


def card_help(card_name):
    """
    Print help of a given card.

    Parameters
    ----------
    card_name : str
        Name of the card.

    Examples
    --------
    >>> get_card_help('GRID')
    """

    help(card_classes[card_name])


setup_logging()

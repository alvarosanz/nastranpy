import numpy as np
from nastranpy.bdf.cards.card import Card


def update_fields(func):

    def wrapped(self):

        if self._is_processed:
            vector = self.vector0
            vector_norm = np.linalg.norm(vector)

            if self.coord:
                vector = self.coord.get_xyz(vector, is_vector=True)

            self._vector = vector / vector_norm
            self._scale_factor = vector_norm

        return func(self)

    return wrapped


class VectorCard(Card):

    def __init__(self, fields, large_field=False, free_field=False):
        super().__init__(fields, large_field=large_field, free_field=free_field)
        self._vector0 = None

    def __str__(self):
        card_str = super().__str__()
        return  card_str[:-1] + ', vector0: {}'.format(repr(self.vector0)) + card_str[-1:]

    def _settle(self):

        if self._vector0 is None:

            if self.coord:

                try:
                    self._vector0 = self.coord.get_xyz0(self._vector, is_vector=True) * self._scale_factor
                except AttributeError:
                    self._log.error('Cannot settle {}'.format(repr(self)))
            else:
                self._vector0 = self._vector * self._scale_factor

    @property
    def vector0(self):

        if self._vector0 is None:
            self._settle()

        return self._vector0

    @vector0.setter
    def vector0(self, value):
        self._vector0 = value

    @update_fields
    def get_fields(self):
        return super().get_fields()

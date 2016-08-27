import numpy as np
from nastranpy.bdf.cards.card import Card


def update_fields(func):

    def wrapped(self):

        if self._is_processed:
            self.fields[3] = self.xyz

        return func(self)

    return wrapped


class GridCard(Card):

    def __init__(self, fields, large_field=False, free_field=False):
        super().__init__(fields, large_field=large_field, free_field=free_field)
        self._xyz0 = None
        self.elems = set()

    @update_fields
    def __str__(self):
        return super().__str__()

    def _settle(self):

        if self._xyz0 is None:
            cp = self.fields[2]

            if cp:

                try:
                    self._xyz0 = cp.get_xyz0(self.fields[3])
                except AttributeError:
                    self._log.error('Cannot settle {}'.format(repr(self)))
            else:
                self._xyz0 = self.fields[3]

    @property
    def xyz0(self):
        return self._xyz0

    @xyz0.setter
    def xyz0(self, value):

        if not np.allclose(self._xyz0, value):
            self._xyz0 = value
            self.changed = True
            self._notify(grid_changed=self)

    @property
    def xyz(self):

        if self._xyz0 is None:
            return self.fields[3]

        cp = self.fields[2]

        if cp:
            return cp.get_xyz(self._xyz0)
        else:
            return self._xyz0

    @xyz.setter
    def xyz(self, value):
        self.fields[3] = value
        cp = self.fields[2]

        if cp:
            self.xyz0 = cp.get_xyz0(value)
        else:
            self.xyz0 = value

    @update_fields
    def get_fields(self):
        return super().get_fields()

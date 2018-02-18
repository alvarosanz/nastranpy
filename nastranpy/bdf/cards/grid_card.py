import numpy as np
from nastranpy.bdf.cards.card import Card
from nastranpy.bdf.cards.filters import filter_factory


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

    def extend(self, card_filters=None, steps=None, max_steps=10000):
        """
        Get adjacent elements.

        Parameters
        ----------
        card_filters : str or list of str, optional
            Any combination of the following options are available (the default is None,
            which implies all model cards are considered):

            Card tags: 'e1D', 'e2D', 'e3D', 'eRigid', 'eSpring', 'eMass' or 'ePlot'
            Card names: 'CROD', 'CBAR', 'CQUAD4', etc ...
            ID pattern (i. e. ['9', '34', '*', '*', '*', '*', '1-8'])

        steps : int, optional
            Number of steps to extend. If not supplied, then all the elements attached
            to the element will be returned.
        max_steps : int, optional
            Maximum number of steps to extend (only when `steps` is not supplied).

        Returns
        -------
        set of Card
            Elements attached to the grid.

        Examples
        --------
        >>> grid.extend(steps=1)
        >>> grid.extend('e2D')
        """
        if not steps:
            steps = max_steps

        steps -= 1

        if card_filters:
            filter_element = filter_factory(card_filters)

        elms = {elm for elm in self.elems if not card_filters or filter_element(elm)}
        elms_diff = elms

        for i in range(steps):
            elms_ext = {elm_ext for elm in elms_diff for grid in elm.grids for
                        elm_ext in grid.elems if not card_filters or filter_element(elm_ext)}
            elms_diff = elms_ext - elms

            if elms_diff:
                elms |= elms_diff
            else:
                break

        return elms

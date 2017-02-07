from nastranpy.bdf.cards.card import Card
from nastranpy.bdf.cards.filters import filter_factory


class ElemCard(Card):

    def __init__(self, fields, large_field=False, free_field=False):
        super().__init__(fields, large_field=large_field, free_field=free_field)
        self._coord = None

    def _settle(self):
        pass

    @property
    def coord(self):

        if not self._coord:
            self._settle()

        return self._coord

    def _update(self, caller, **kwargs):
        super()._update(caller, **kwargs)

        for key, value in kwargs.items():

            if key == 'grid_changed':

                if self._coord:
                    self._settle()

    def extend(self, steps=None, max_steps=10000, **kwargs):
        """
        Get adjacent elements.

        Parameters
        ----------
        steps : int, optional
            Number of steps to extend. If not supplied, then all the elements attached
            to the element will be returned.
        max_steps : int, optional
            Maximum number of steps to extend (only when `steps` is not supplied).

        Returns
        -------
        set of Card
            Elements attached to the element.

        Examples
        --------
        >>> element.extend(1)
        >>> element.extend(tags=['e2D'])
        """
        if not steps:
            steps = max_steps

        elms = {self}
        elms_diff = elms

        if kwargs:
            filter_element = filter_factory(kwargs)

        for i in range(steps):
            elms_ext = {elm_ext for elm in elms_diff for grid in elm.grids for
                        elm_ext in grid.elems if not kwargs or filter_element(elm_ext)}
            elms_diff = elms_ext - elms

            if elms_diff:
                elms |= elms_diff
            else:
                break

        return elms

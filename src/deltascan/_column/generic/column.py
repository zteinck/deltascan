from ._base import ColumnBase
from ._alias import Alias


class Column(ColumnBase):

    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    #╭-------------------------------------------------------------------------╮
    #| Properties                                                              |
    #╰-------------------------------------------------------------------------╯

    @property
    def alias(self):
        '''
        Description
        ------------
        Returns an aliased instance if the column belongs to a dataset.
        Otherwise, an error is raised.

        Returns
        ------------
        alias : Alias
            Aliased instance of the column.
        '''

        if self._dataset is None:
            raise AttributeError(
                f'Column {self.name!r} cannot be aliased because it does not '
                'belong to a dataset.'
                )

        return Alias(self)
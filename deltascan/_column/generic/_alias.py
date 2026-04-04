from ._base import ColumnBase


class Alias(ColumnBase):
    '''
    Description
    --------------------
    Wraps a column instance associated with a specific dataset.

    Class Attributes
    --------------------
    ...

    Instance Attributes
    --------------------
    _base : Column
        The original column instance.
    '''

    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, base):
        name = base._dataset.apply_alias(base.name)

        super().__init__(
            name=name,
            expr=base._expr,
            dataset=base._dataset
            )

        self._base = base


    #╭-------------------------------------------------------------------------╮
    #| Properties                                                              |
    #╰-------------------------------------------------------------------------╯

    @property
    def base(self):
        return self._base
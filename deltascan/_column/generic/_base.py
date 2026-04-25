import oddments as odd
import polars as pl


class ColumnBase(odd.ReprMixin):
    '''
    Description
    --------------------
    Column base class.

    Class Attributes
    --------------------
    ...

    Instance Attributes
    --------------------
    _name : str
        Column name.
    _expr : pl.Expr | None
        Expression used to derive the column.
    _dataset : Dataset | None
        Dataset instance to which the column belongs.
    '''

    #╭-------------------------------------------------------------------------╮
    #| Class Attributes                                                        |
    #╰-------------------------------------------------------------------------╯

    _repr_attrs = [
        'name',
        'is_derived',
        '_dataset',
        ]


    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, name, expr=None, dataset=None):
        self._name = name
        self._expr = expr
        self._dataset = dataset


    #╭-------------------------------------------------------------------------╮
    #| Magic Methods                                                           |
    #╰-------------------------------------------------------------------------╯

    def __str__(self):
        return self.name


    #╭-------------------------------------------------------------------------╮
    #| Properties                                                              |
    #╰-------------------------------------------------------------------------╯

    @property
    def name(self):
        return self._name


    @property
    def col(self):
        return pl.col(self.name)


    @property
    def is_derived(self):
        ''' True if the column is derived via expression '''
        return self._expr is not None


    @property
    def expr(self):
        if self.is_derived:
            return self._expr.alias(self.name)

        raise AttributeError(
            "Unexpected attempt to access 'expr' property on column "
            f"{self.name!r}. This attribute is only available for columns "
            "derived from expressions."
            )


    @property
    def is_join_key(self):
        if self._dataset is not None:
            return self.name in self._dataset._parent._join_keys

        raise AttributeError(
            "Unexpected attempt to access 'is_join_key' property on column "
            f"{self.name!r}. This attribute is only available for columns "
            "that belong to a dataset."
            )
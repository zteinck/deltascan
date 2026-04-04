import polars as pl

from ..._column import Column


class SharedColumn(object):
    '''
    Description
    --------------------
    Contains the comparison logic used to evaluate values in a column that
    exists in both datasets.

    Class Attributes
    --------------------
    delta_enabled : bool
        If True, an expression calculating the difference between the left and
        right column is:
            • Available for use when defining the "unequal" expression.
            • Included in the list of expressions selected when materializing
              the column differences.

    Instance Attributes
    --------------------
    _parent : DeltaScan
        Parent instance.
    _name : str
        Name of the column shared between the left and right datasets.
    _left : Alias
        Alias instance that represents the column in the left dataset.
    _right : Alias
        Alias instance that represents the column in the right dataset.
    _delta : Column | None
        Column instance used to compute the difference between the left and
        right column. Is None when 'delta_enabled=False'.
    _unequal : Column
        Column instance used to to determine whether the left and right column
        values are unequal. Specifically, the expression evaluates to True if
        values are unequal and False if they are equal.
    '''

    #╭-------------------------------------------------------------------------╮
    #| Class Attributes                                                        |
    #╰-------------------------------------------------------------------------╯

    delta_enabled = False


    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, parent, name):
        self._parent = parent
        self._name = name
        self._left, self._right = self._init_side_columns()
        self._delta = self._init_delta_column()
        self._unequal = self._init_unequal_column()


    #╭-------------------------------------------------------------------------╮
    #| Properties                                                              |
    #╰-------------------------------------------------------------------------╯

    @property
    def name(self):
        return self._name


    @property
    def left(self):
        return self._left


    @property
    def right(self):
        return self._right


    @property
    def sides(self):
        return self.left, self.right


    @property
    def delta(self):
        if self.delta_enabled:
            return self._delta
        else:
            raise ValueError(
                "'delta' attribute is not accessible when "
                "'delta_enabled=False'."
                )


    @property
    def unequal(self):
        return self._unequal


    @property
    def select_exprs(self):
        '''
        Description
        ------------
        Generates a list of expressions to select when materializing
        differences for the shared column.

        Returns
        ------------
        exprs : list
            List of expressions to select.
        '''

        cols = [
            *self._parent.join_on,
            *self._parent._context[self.name],
            *[side.name for side in self.sides],
            ]

        exprs = [pl.col(col) for col in cols]

        if self.delta_enabled:
            exprs.append(self.delta.expr)

        return exprs


    #╭-------------------------------------------------------------------------╮
    #| Instance Methods                                                        |
    #╰-------------------------------------------------------------------------╯

    def _make_column(self, **kwargs):
        ''' initialize a Column instance '''
        return Column(**kwargs)


    def _init_side_columns(self):
        sides = []

        for ds in self._parent._datasets:
            col = self._make_column(
                name=self.name,
                dataset=ds
                )
            side = col.alias
            side._expr = self._build_side_expr(side)
            sides.append(side)

        return tuple(sides)


    def _build_side_expr(self, side):
        return side.col


    def _init_delta_column(self):
        if not self.delta_enabled:
            return None

        name = self._parent.column_template.format(
            alias=self._parent.delta_alias,
            column=self.name
            )

        expr = self.left.expr - self.right.expr

        col = self._make_column(
            name=name,
            expr=expr
            )

        return col


    def _init_unequal_column(self):
        name = f'_is_unequal_{self.name}'

        expr = pl.all_horizontal([
            (self._parent._in_both_mask),
            (self._build_unequal_expr())
            ])

        col = self._make_column(
            name=name,
            expr=expr
            )

        return col


    def _build_unequal_expr(self):
        ''' default not-equal expression where None == None '''
        return self.left.expr.ne_missing(self.right.expr)
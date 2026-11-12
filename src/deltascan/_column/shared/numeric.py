from ._base import SharedColumn


class NumericColumn(SharedColumn):

    #╭-------------------------------------------------------------------------╮
    #| Class Attributes                                                        |
    #╰-------------------------------------------------------------------------╯

    delta_enabled = True


    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    #╭-------------------------------------------------------------------------╮
    #| Instance Methods                                                        |
    #╰-------------------------------------------------------------------------╯

    def _build_unequal_expr(self):
        if self._parent.tolerance == 0:
            return super()._build_unequal_expr()

        null_mismatch = (
            (self.left.col.is_null())
            ^ (self.right.col.is_null())
            )

        exceeds_tolerance = (
            (self.delta.expr.abs())
            > (self._parent.tolerance)
            )

        return (null_mismatch) | (exceeds_tolerance)
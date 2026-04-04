from ._base import SharedColumn


class StringColumn(SharedColumn):

    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    #╭-------------------------------------------------------------------------╮
    #| Instance Methods                                                        |
    #╰-------------------------------------------------------------------------╯

    def _build_side_expr(self, side):
        expr = side.col

        if self._parent.ignore_whitespace:
            expr = expr.str.strip_chars().replace('', None)

        if self._parent.ignore_case:
            expr = expr.str.to_lowercase()

        return expr
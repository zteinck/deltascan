from ._base import SharedColumn


class BooleanColumn(SharedColumn):

    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
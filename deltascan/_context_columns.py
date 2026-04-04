import oddments as odd


class ContextColumns(odd.ReprMixin):
    '''
    Description
    --------------------
    Administers columns included in comparison results for context.

    Class Attributes
    --------------------
    ...

    Instance Attributes
    --------------------
    _parent : DeltaScan | Dataset
        Instance to which the context columns belong.
    _ordered : list[Column]
        Ordered list of all context columns.
    _universal : set[str]
        Set of aliased column names included universally.
    _targeted : dict[str: list[str]]
        Dictionary mapping shared column names to specific columns to include
        in that column's comparison result.
    '''

    #╭-------------------------------------------------------------------------╮
    #| Class Attributes                                                        |
    #╰-------------------------------------------------------------------------╯

    _repr_attrs = [
        'ordered',
        'universal',
        'targeted',
        ]


    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, parent, ordered, universal, targeted):
        self._parent = parent

        for name, value, kind in (
            ('ordered', ordered, list),
            ('universal', universal, set),
            ('targeted', targeted, dict),
            ):
            odd.validate_value(
                name=name,
                value=value,
                types=kind,
                )
            setattr(self, '_' + name, value)


    #╭-------------------------------------------------------------------------╮
    #| Properties                                                              |
    #╰-------------------------------------------------------------------------╯

    @property
    def ordered(self):
        return self._ordered[:]


    @property
    def universal(self):
        return self._universal.copy()


    @property
    def targeted(self):
        return self._targeted.copy()


    @property
    def empty(self):
        return not self.ordered


    #╭-------------------------------------------------------------------------╮
    #| Magic Methods                                                           |
    #╰-------------------------------------------------------------------------╯

    def __getitem__(self, key):
        '''
        Description
        ------------
        Generates an ordered list of columns to include alongside a given
        shared column for context.

        Parameters
        ------------
        key : str
            Shared column name.

        Returns
        ------------
        out : list
            Ordered list of column names to include for context.
        '''

        if key not in self._parent._schema:
            raise KeyError(
                f'Column name not found in schema: {key!r}'
                )

        out = []

        if self.empty:
            return out

        extras = self.targeted.get(key, set())
        include = self.universal.union(extras)

        for col in self.ordered:
            if col.name == key:
                continue

            alias = col.alias.name

            if alias in include:
                out.append(alias)

        return out
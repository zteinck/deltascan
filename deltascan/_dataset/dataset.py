from functools import cached_property

from clockwork import print_duration
import oddments as odd
import polars as pl

from .._column import Column
from .._context_columns import ContextColumns
from ._schema import Schema


class Dataset(odd.ReprMixin):
    '''
    Description
    --------------------
    Dataset on one side of the comparison.

    Class Attributes
    --------------------
    ...

    Instance Attributes
    --------------------
    _parent : DeltaScan
        Parent instance to which the dataset belongs.
    _side : str
        Indicates the dataset's position in the comparison: 'left' or 'right'.
    _alias : str
        See parent class documentation.
    _lf : pl.LazyFrame
        Lazy representation of the data.
    _schema : Schema
        Schema of the LazyFrame. Excludes the side flag column.
    _context : ContextColumns
        See parent class documentation.
    '''

    #╭-------------------------------------------------------------------------╮
    #| Class Attributes                                                        |
    #╰-------------------------------------------------------------------------╯

    _repr_attrs = [
        'side',
        'alias',
        ]


    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, parent, side, data, alias, context):
        self._parent = parent
        self._side = side
        self._alias = self._init_alias(alias)
        self._lf, self._schema = self._init_data(data)
        self._context = self._init_context(context)


    #╭-------------------------------------------------------------------------╮
    #| Properties                                                              |
    #╰-------------------------------------------------------------------------╯

    @property
    def side(self):
        return self._side


    @property
    def alias(self):
        return self._alias


    @property
    def lf(self):
        return self._lf


    @property
    def schema(self):
        return self._schema


    @property
    def context(self):
        return self._context


    @property
    def join_on(self):
        return self._parent.join_on


    @property
    def verbose(self):
        return self._parent.verbose


    @property
    def other(self):
        ''' the opposing dataset instance '''
        if self is self._parent._left_data:
            return self._parent._right_data
        else:
            return self._parent._left_data


    @property
    def column_count(self):
        ''' total number of columns '''
        return len(self.schema)


    @property
    def value_count(self):
        ''' total number of values '''
        return self.row_count * self.column_count


    @property
    def shape(self):
        ''' shape of the dataset '''
        return self.row_count, self.column_count


    @property
    def _data_param_name(self):
        ''' name of the data argument passed to the owning class constructor;
            used in error messages '''
        return f'{self.side}_data'


    #╭-------------------------------------------------------------------------╮
    #| Cached Properties                                                       |
    #╰-------------------------------------------------------------------------╯

    @cached_property
    def side_flag(self):
        ''' boolean column indicating the origin of each row '''
        return Column(f'_{self.side}_flag')


    @cached_property
    def row_count(self):
        ''' total number of rows '''
        return self._parent._count_rows(self.lf)


    #╭-------------------------------------------------------------------------╮
    #| Instance Methods                                                        |
    #╰-------------------------------------------------------------------------╯

    def apply_alias(self, column):
        ''' apply alias to a column name '''
        return self._parent.column_template.format(
            alias=self.alias,
            column=column
            )


    def _add_side_flag(self, obj):
        ''' adds the side flag column to a Polars object '''
        expr = pl.lit(True).alias(self.side_flag.name)
        return obj.with_columns(expr)


    def _to_not_in_description(self, dim):
        ''' generates the comparison label for items found on one side but not
            the other '''
        dim = 'cols' if dim == 'columns' else dim
        return f'{self.alias} {dim} not in {self.other.alias}'


    def _init_alias(self, value):
        '''
        Description
        ------------
        Ensures the alias is a non-empty string and unique across both
        datasets.

        Parameters
        ------------
        value : str
            Dataset alias.

        Returns
        ------------
        value : str
            Validated alias.
        '''

        odd.validate_value(
            value=value,
            name=f'{self.side}_alias',
            types=str,
            empty_ok=False
            )

        if (
            self.side == 'right'
            and self._parent._left_data.alias == value
            ):
            raise ValueError(
                "'left_alias' and 'right_alias' must be "
                f"different, got {value!r} for both."
                )

        return value


    @print_duration()
    def _assert_unique_join_keys(self, lf):
        odd.assert_unique(
            lf,
            subset=self.join_on,
            name=self._data_param_name,
            null_policy='error'
            )


    def _init_data(self, data):
        ''' resolves the data argument into LazyFrame and Schema instances '''

        # convert data to LazyFrame
        lf = odd.to_polars_frame(
            obj=data,
            name=self._data_param_name,
            lazy=True
            )

        # add row index column if joining on index
        if self._parent._join_on_index:
            lf = lf.with_row_index()

        # collect schema
        schema = Schema(dataset=self, lf=lf)

        # verify there are no missing join keys
        missing_join_keys = [
            col for col in self.join_on
            if col not in schema
            ]

        if missing_join_keys:
            raise ValueError(
                f"{self._data_param_name!r} is missing join keys specified "
                f"in 'join_on': {missing_join_keys}"
                )

        # drop duplicate rows
        lf = lf.unique()

        # check for join key duplicates
        if not (
            self._parent.allow_duplicates
            or self._parent._join_on_index
            ):
            if self.verbose:
                print(self.alias + ':')

            self._assert_unique_join_keys(lf)

        # tag column names with dataset's alias
        lf = lf.rename({
            col: self.apply_alias(col)
            for col in schema
            if col not in self.join_on
            })

        # add side-flag column
        lf = self._add_side_flag(lf)

        return lf, schema


    def _init_context(self, value):
        args = self._trifurcate_context(value)
        return ContextColumns(self, *args)


    def _trifurcate_context(self, value):
        ''' resolves the context argument into ContextColumns parameters '''

        def to_aliased_set(column_list):
            column_set = set(column_list)

            aliased_set = {
                self.apply_alias(col)
                for col in column_set
                }

            return aliased_set


        # context parameter name
        param_name = f'{self.side}_context'

        # validate parameter type
        odd.validate_value(
            value=value,
            name=param_name,
            types=(str, list, dict),
            none_ok=True
            )

        # set up default values
        ordered = list()
        universal = set()
        targeted = dict()

        # return early if parameter is None
        if value is None:
            return ordered, universal, targeted

        # if parameter is str or dict, convert to list
        value = odd.ensure_list(value)

        # parse parameter
        for i, x in enumerate(value):
            odd.validate_value(
                value=x,
                name=f'{param_name!r} element {i}',
                types=(str, dict),
                empty_ok=False
                )

            if isinstance(x, str):
                ordered.append(x)
                continue

            for k in x.keys():
                v = odd.sanitize_subset(x[k])
                targeted.setdefault(k, []).extend(v)

        # update columns to be applied universally
        universal.update(
            to_aliased_set(ordered)
            )

        # for each targeted list, update column order then drop duplicates and
        # apply alias
        for k in list(targeted.keys()):
            v = targeted[k]
            ordered.extend(v)
            targeted[k] = to_aliased_set(v)

        # validate context columns against schema columns
        ordered = odd.sanitize_subset(
            subset=ordered + list(targeted.keys()),
            superset=self.schema.columns,
            subset_name=repr(param_name),
            superset_name='schema'
            )

        # verify parameter excludes join keys
        join_keys = [
            col for col in ordered
            if col in self.join_on
            ]

        if join_keys:
            raise ValueError(
                f"{param_name!r} cannot include join "
                f"keys from 'join_on': {join_keys}"
                )

        # convert to list of column instances
        ordered = [
            Column(name=col, dataset=self)
            for col in ordered
            ]

        return ordered, universal, targeted
from functools import cached_property

from clockwork import print_duration
import oddments as odd
import polars as pl

from ._column import Column


class UnifiedSchema(object):
    '''
    Description
    --------------------
    Generates a lazy plan to unify the left and right dataset schemas and
    provides methods to:
        • Identify columns present in one dataset but not the other.
        • Detect data type mismatches between shared columns.

    Class Attributes
    --------------------
    uid : Column
        Column instance representing the unified schema's primary key.
    is_compatible : Column
        Column instance that indicates whether columns in the left and right
        dataset have compatible data types.
    type_columns : list[str]
        The schema's data type column names.

    Instance Attributes
    --------------------
    _parent : DeltaScan
        Parent instance to which the unified schema belongs.
    lf : pl.LazyFrame
        Lazy full join of the left and right dataset schemas.

        columns:
            'column': str
            '{left_alias}.kind' : str
            '{right_alias}.kind' : str
            '{left_alias}.dtype' : str
            '{right_alias}.dtype' : str
            'is_compatible' : bool
            '_side_indicator' : cat

        row order:
            'column' ascending
    '''

    #╭-------------------------------------------------------------------------╮
    #| Class Attributes                                                        |
    #╰-------------------------------------------------------------------------╯

    uid = Column('column')
    is_compatible = Column('is_compatible')
    type_columns = ['kind','dtype']
    dimension = 'columns'


    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, parent):
        self._parent = parent
        self.lf = self._full_join_schemas()


    #╭-------------------------------------------------------------------------╮
    #| Magic Methods                                                           |
    #╰-------------------------------------------------------------------------╯

    def __contains__(self, item):
        ''' check whether a column exists in the unified schema '''
        for ds in self._datasets:
            if item in ds.schema:
                return True

        return False


    #╭-------------------------------------------------------------------------╮
    #| Properties                                                              |
    #╰-------------------------------------------------------------------------╯

    @property
    def _in_both_mask(self):
        return self._parent._in_both_mask


    @property
    def _datasets(self):
        return self._parent._datasets


    @property
    def verbose(self):
        return self._parent.verbose


    #╭-------------------------------------------------------------------------╮
    #| Cached Properties                                                       |
    #╰-------------------------------------------------------------------------╯

    @cached_property
    def unique_column_count(self):
        ''' total number of unique columns accross both datasets '''
        return self._parent._count_rows(self.lf)


    @cached_property
    def shared_column_count(self):
        ''' total number of columns shared between the datasets '''
        lf = self.lf.filter(self._in_both_mask)
        return self._parent._count_rows(lf)


    @cached_property
    def type_columns_order(self):
        ''' ordered list of aliased type columns '''
        return [
            ds.apply_alias(col)
            for col in self.type_columns
            for ds in self._datasets
            ]


    @cached_property
    def _in_join_on_mask(self):
        ''' boolean mask expression for filtering rows that do not represent
            join keys '''
        return self.uid.col.is_in(self._parent.join_on)


    @cached_property
    def kind_exprs(self):
        ''' tuple of aliased type column expressions '''
        return tuple(
            ds.schema.kind.alias.col
            for ds in self._datasets
            )


    @cached_property
    def dtype_exprs(self):
        ''' tuple of aliased type column expressions '''
        return tuple(
            ds.schema.dtype.alias.col
            for ds in self._datasets
            )


    @cached_property
    def dtype_unequal_mask(self):
        ''' boolean mask expression for filtering rows where dtype is not
            equal '''
        a, b = self.dtype_exprs
        return a != b


    #╭-------------------------------------------------------------------------╮
    #| Instance Methods                                                        |
    #╰-------------------------------------------------------------------------╯

    def _get_aliased_schema(self, ds):
        '''
        Description
        ------------
        Retrieves the schema DataFrame for a dataset, converts it to a
        LazyFrame, filters ignored column rows, and aliases the type columns.

        Parameters
        ------------
        ds : Dataset
            Dataset instance.

        Returns
        ------------
        lf : pl.LazyFrame
            Aliased dataset schema.
        '''

        df = ds.schema.df

        expected_cols = [
            self.uid.name,
            *self.type_columns,
            ds.side_flag.name,
            ]

        if expected_cols != df.columns:
            raise ValueError(
                'Schema column mismatch: expected '
                f'{expected_cols}, got {df.columns}'
                )

        lf = df.lazy()

        ignore_columns = self._parent.ignore_columns

        if ignore_columns:
            lf = lf.filter((
                ~self.uid.col.is_in(ignore_columns)
                ))

        lf = lf.rename({
            col: ds.apply_alias(col)
            for col in self.type_columns
            })

        return lf


    def _full_join_schemas(self):
        '''
        Description
        ------------
        Performs a full join on the left and right schemas, adds
        'is_compatible' column, sorts rows by column name, then reorders
        column names.

        Returns
        ------------
        lf : pl.LazyFrame
            Plan to unify schemas.
        '''

        left_schema, right_schema = (
            self._get_aliased_schema(ds)
            for ds in self._datasets
            )

        lf = self._parent._full_join(
            left_data=left_schema,
            right_data=right_schema,
            join_on=self.uid.name
            )

        left_expr, right_expr = (
            self.dtype_exprs
            if self._parent.dtype_strict
            else self.kind_exprs
            )

        lf = (
            lf
            .with_columns(
                (left_expr == right_expr)
                .alias(self.is_compatible.name)
                )
            .sort(self.uid.name)
            .select([
                self.uid.name,
                *self.type_columns_order,
                self.is_compatible.name,
                self._parent._row_origin.name
                ])
            )

        return lf


    def _validate_join_key_compatibility(self):
        ''' raise on mismatched join key data types '''

        df = (
            self.lf
            .filter(
                pl.all_horizontal([
                    (self._in_both_mask),
                    (self.dtype_unequal_mask),
                    (self._in_join_on_mask),
                    ])
                )
            .select([
                self.uid.col,
                *self.dtype_exprs
                ])
            .collect()
            )

        if not df.is_empty():
            raise TypeError(
                f'Join keys must share the same data type:\n\n{df}'
                )


    @print_duration()
    def _compare_column_names(self):
        ''' detects columns present in one dataset but not the other '''

        plans = []

        for ds in self._datasets:
            plan = (
                self.lf
                .filter((
                    self._parent._matches_origin(ds.side)
                    ))
                .rename({
                    ds.apply_alias(col): col
                    for col in self.type_columns
                    })
                .select([
                    self.uid.name,
                    *self.type_columns
                    ])
                )

            plans.append(plan)

        for ds, df in self._parent._iter_zip_collect(
            items=self._datasets,
            plans=plans
            ):

            if df.is_empty():
                continue

            self._parent._add_difference(
                description=ds._to_not_in_description(self.dimension),
                dimension=self.dimension,
                df=df,
                total_count=self.unique_column_count
                )


    @print_duration()
    def _compare_column_types(self):
        ''' detects data type mismatches among columns present in both
            datasets '''

        df = (
            self.lf
            .filter(
                pl.all_horizontal([
                    (self._in_both_mask),
                    (self.dtype_unequal_mask),
                    ])
                )
            .select(
                pl.col([
                    self.uid.name,
                    *self.type_columns_order
                    ])
                )
            .collect()
            )

        if df.is_empty():
            return

        self._parent._add_difference(
            description='data types',
            dimension=self.dimension,
            df=df,
            total_count=self.shared_column_count
            )


    def _get_shared_columns(self, compatible=None):
        '''
        Description
        ------------
        Filters unified schema for columns present in both left and right
        schemas, excluding the join keys, and optionally for compatibility.

        Parameters
        ------------
        compatible : bool | None
            Optional compatibility filter:
                • None → Compatibility filter is omitted.
                • True → Include only shared columns whose data types are
                    compatible.
                • False → Include only shared columns whose data types are
                    incompatible.

        Returns
        ------------
        df : pl.DataFrame
            Shared columns within unified schema.
        '''

        odd.validate_value(
            value=compatible,
            name='compatible',
            types=bool,
            none_ok=True
            )

        exprs = [
            (self._in_both_mask),
            (~self._in_join_on_mask),
            ]

        if compatible is not None:
            exprs.append((
                self.is_compatible.col == compatible
                ))

        df = (
            self.lf
            .filter(
                pl.all_horizontal(exprs)
                )
            .collect()
            )

        return df
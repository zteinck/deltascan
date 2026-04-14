from functools import cached_property
from numbers import Real

from clockwork import print_duration
from pathpilot import file_factory
import oddments as odd
import polars as pl

from ._column import *
from ._dataset import Dataset
from ._context_columns import ContextColumns
from ._unified_schema import UnifiedSchema


class DeltaScan(odd.ReprMixin):
    '''
    Description
    --------------------
    Finds and summarizes the differences between two datasets.

    Class Attributes
    --------------------
    ...

    Instance Attributes
    --------------------
    _left_data : Dataset
        Dataset designated as the left-side in the comparison.
    _right_data : Dataset
        Dataset designated as the right-side in the comparison.
    _summary_rows : list
        Buffer for summary statistics.
    _differences : dict
        Registry for the results of the comparison.

    Note:
    --------------------
    Refer to the '__init__()' documentation below for additional instance
    attributes.
    '''

    #╭-------------------------------------------------------------------------╮
    #| Class Attributes                                                        |
    #╰-------------------------------------------------------------------------╯

    _repr_attrs = [
        '_left_data',
        '_right_data',
        'join_on',
        'delta_alias',
        'column_template',
        '_context',
        'dimensions',
        'ignore_columns',
        'tolerance',
        'ignore_whitespace',
        'ignore_case',
        'allow_duplicates',
        'dtype_strict',
        'verbose',
        ]


    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(
        self,
        left_data,
        right_data,
        join_on=None,
        left_alias='left',
        right_alias='right',
        delta_alias='delta',
        column_template='{alias}.{column}',
        summary_name='summary',
        left_context=None,
        right_context=None,
        dimensions=None,
        ignore_columns=None,
        tolerance=0,
        ignore_whitespace=False,
        ignore_case=False,
        allow_duplicates=False,
        dtype_strict=False,
        verbose=False,
        ):
        '''
        Parameters
        ------------
        left_data : LazyFrame | DataFrame | Series
            Data to compare to 'right_data'. Supports Polars LazyFrame,
            DataFrame, and Series. Also supports Pandas DataFrame and Series.
        right_data : ↑ same
            Data to compare to 'left_data'.
        join_on : str | list | tuple | set
            Column name(s) on which to join 'left_data' and 'right_data'. For
            Pandas, supplying index names is also supported.
        left_alias : str
            Alias used to denote columns from the "left" dataset (e.g.
            'after').
        right_alias : str
            Alias used to denote columns from the "right" dataset (e.g.
            'before').
        delta_alias : str
            Alias used to denote delta columns, representing differences
            between the same column in the left and right datasets. (e.g.
            'delta', 'diff', 'Δ', etc.).
        column_template : str
            Template for aliasing column names by dataset (e.g.
            '{alias}.{name}', '{name} ({alias})', etc.).
        summary_name : str
            Name used to identify the summary DataFrame. Serves as the
            dictionary key in 'results' and as the Excel worksheet name when
            exported.
        left_context : str | list[str] | dict[str: list[str]] | None
            Specifies which columns from 'left_data' to include in the
            comparison results for context. Strings are interpreted as column
            names to be included universally across all relevant comparisons.
            Dictionaries can be used to target specific comparisons, where
            each key is a shared column name and its value is a list of
            additional column names to include for that comparison.
        right_context : ↑ same
            Same as 'left_context', but applied to 'right_data'.
        dimensions : set[str] | None
            Specifies the dimensions along which the data will be compared:
                • 'rows' → identifies rows that exist in one dataset but not
                    the other, using 'join_on' for alignment.
                • 'columns' → highlights differences in column presence and
                    data types between datasets.
                • 'values → detects mismatched values in corresponding cells.
                • None → performs a full comparison across all dimensions.
        ignore_columns : set | str
            Columns to exclude from the comparison.
        tolerance : float
            Numeric tolerance for comparing values. Differences that exceed
            this margin are counted as differences. For example, with
            'tolerance=0.01', values 1.01 and 1.02 are considered equal.
        ignore_whitespace : bool
            If True, leading and trailing whitespace in string column values
            is ignored during comparison.
        ignore_case : bool
            If True, string column comparisons will ignore letter casing.
        allow_duplicates : bool
            If True, duplicate values in the 'join_on' column(s) are allowed;
            otherwise, an error is raised.
        dtype_strict : bool
            If True, data types must match exactly for two values to be
                considered equal. For example, if a column has type Int64 and
                its counterpart has type Float64, the values in all matching
                rows will be considered different.
            If False, values with different data types may be considered equal
                provided they belong to the same data type family. For example
                1 (Int64) and 1.0 (Float64) would be considered equal despite
                having different data types because they are both numeric.
        verbose : bool
            If True, status information is printed during the comparison.
        '''

        # initialize miscellaneous attributes from user parameters
        self._join_on = odd.sanitize_subset(join_on)
        self._delta_alias = self._init_delta_alias(delta_alias)
        self._column_template = self._init_column_template(column_template)
        self._summary_name = self._init_summary_name(summary_name)
        self._dimensions = self._init_dimensions(dimensions)
        self._ignore_columns = self._init_ignore_columns(ignore_columns)
        self._tolerance = self._init_tolerance(tolerance)

        # initialize boolean attributes from user parameters
        for name, value in {
            'allow_duplicates': allow_duplicates,
            'ignore_whitespace': ignore_whitespace,
            'ignore_case': ignore_case,
            'dtype_strict': dtype_strict,
            'verbose': verbose,
            }.items():
            odd.validate_value(
                name=name,
                value=value,
                types=bool
                )
            setattr(self, '_' + name, value)

        # initialize left & right datasets
        self._left_data = Dataset(
            parent=self,
            side='left',
            data=left_data,
            alias=left_alias,
            context=left_context,
            )

        self._right_data = Dataset(
            parent=self,
            side='right',
            data=right_data,
            alias=right_alias,
            context=right_context,
            )

        # initialize runtime attributes
        self._summary_rows = []
        self._differences = {}

        # execute the comparison
        self._perform_comparison()


    #╭-------------------------------------------------------------------------╮
    #| Cached Properties                                                       |
    #╰-------------------------------------------------------------------------╯

    @cached_property
    def _schema(self):
        ''' refer to unified schema documentation '''
        return UnifiedSchema(self)


    @cached_property
    def _context(self):
        '''
        Description
        ------------
        Merges the left and right datasets' context columns.

        Returns
        ------------
        out : ContextColumns
            Merged instance.
        '''

        ordered = self._merge_ordered_context_columns()
        universal = self._merge_universal_context_columns()
        targeted = self._merge_targeted_context_columns()

        out = ContextColumns(
            parent=self,
            ordered=ordered,
            universal=universal,
            targeted=targeted
            )

        return out


    @cached_property
    def _row_origin(self):
        ''' categorical column indicating the origin of each row ('left',
            'right', or 'both') '''
        return Column('_row_origin')


    @cached_property
    def _lf(self):
        ''' full join left and right datasets '''
        self._schema._validate_join_key_compatibility()

        lf = self._full_join(
            left_data=self._left_data.lf,
            right_data=self._right_data.lf,
            join_on=self.join_on
            )

        return lf


    @cached_property
    def _matching_row_count(self):
        ''' total number of matching rows '''
        lf = self._lf.filter(self._in_both_mask)
        return self._count_rows(lf)


    @cached_property
    def _summary(self):
        return self._summarize_differences()


    #╭-------------------------------------------------------------------------╮
    #| Properties                                                              |
    #╰-------------------------------------------------------------------------╯

    @property
    def join_on(self):
        ''' returns the join key(s) '''
        if self._join_on_index:
            return ['index']

        return self._join_on[:]


    @property
    def delta_alias(self):
        return self._delta_alias


    @property
    def column_template(self):
        return self._column_template


    @property
    def dimensions(self):
        return self._dimensions.copy()


    @property
    def ignore_columns(self):
        return self._ignore_columns.copy()


    @property
    def tolerance(self):
        return self._tolerance


    @property
    def allow_duplicates(self):
        return self._allow_duplicates


    @property
    def ignore_whitespace(self):
        return self._ignore_whitespace


    @property
    def ignore_case(self):
        return self._ignore_case


    @property
    def dtype_strict(self):
        return self._dtype_strict


    @property
    def verbose(self):
        return self._verbose


    @property
    def summary_name(self):
        return self._summary_name


    @property
    def summary(self):
        return self._summary.clone()


    @property
    def differences(self):
        return {
            k: v.clone()
            for k, v in self._differences.items()
            }


    @property
    def results(self):
        ''' dictionary containing both the summary and differences '''
        key = self.summary_name

        if key in self.differences:
            raise AssertionError(
                f'Unexpected key in differences: {key!r}'
                )

        return {key: self.summary} | self.differences


    @property
    def _rows_in_scope(self):
        return 'rows' in self.dimensions


    @property
    def _cols_in_scope(self):
        return 'columns' in self.dimensions


    @property
    def _vals_in_scope(self):
        return 'values' in self.dimensions


    @property
    def _datasets(self):
        return self._left_data, self._right_data


    @property
    def _in_both_mask(self):
        ''' boolean mask expression for filtering rows that are not present
            in both datasets '''
        return self._matches_origin('both')


    @property
    def _join_on_index(self):
        ''' True if joining on row index; otherwise False '''
        return self._join_on is None


    @property
    def _vs_label(self):
        ''' left vs right dataset aliases '''
        return ' vs '.join(ds.alias for ds in self._datasets)


    #╭-------------------------------------------------------------------------╮
    #| Static Methods                                                          |
    #╰-------------------------------------------------------------------------╯

    @staticmethod
    def _count_rows(lf):
        return lf.select(pl.len()).collect().item()


    #╭-------------------------------------------------------------------------╮
    #| Instance Methods                                                        |
    #╰-------------------------------------------------------------------------╯

    def to_excel(self, path=None, **kwargs):
        '''
        Description
        ------------
        Exports comparison results to an Excel file.

        Parameters
        ------------
        path : None | str | pp.File
            Excel file path. If None, a default file name is generated using
            the dataset aliases.
        summary_name : str
            Name of the summary worksheet.
        kwargs : dict
            Keyword arguments passed to the writer.

        Returns
        ------------
        file : pp.ExcelFile
            Excel file object.
        '''

        ext = 'xlsx'

        if path is None:
            path = '_'.join([
                *['delta','scan'],
                *self._vs_label.split()
                ]) + '.' + ext

        file = file_factory(str(path))

        if file.ext != ext:
            raise ValueError(
                "Invalid file extension for 'path'. "
                f"Expected {ext!r}, got: {file.ext!r}"
                )

        file.save(self.results, df_backend='polars', **kwargs)

        return file


    def _init_delta_alias(self, value):
        odd.validate_value(
            value=value,
            name='delta_alias',
            types=str,
            empty_ok=False
            )

        return value


    def _init_column_template(self, value):
        name = 'column_template'

        odd.validate_value(
            value=value,
            name=name,
            types=str,
            empty_ok=False
            )

        for placeholder in ['{alias}','{column}']:
            if placeholder not in value:
                raise ValueError(
                    f'{name!r} parameter must include a {placeholder!r} '
                    'placeholder.'
                    )

        if not value.isprintable():
            raise ValueError(
                f'{name!r} must be printable.'
                )

        return value


    def _init_summary_name(self, value):
        odd.validate_value(
            value=value,
            name='summary_name',
            types=str,
            empty_ok=False,
            )

        return value


    def _init_dimensions(self, value):
        out = odd.sanitize_subset(
            subset=value,
            superset=['rows','columns','values'],
            subset_name='dimensions',
            )

        return set(out)


    def _init_ignore_columns(self, value):
        out = odd.sanitize_subset(value)
        return set(out or [])


    def _init_tolerance(self, value):
        odd.validate_value(
            value=value,
            name='tolerance',
            types=Real,
            finite=True,
            min_value=0,
            min_inclusive=True,
            )

        return value


    def _merge_ordered_context_columns(self):
        '''
        Description
        ------------
        Merges the left and right datasets' ordered list of context columns.

        Returns
        ------------
        out : list
            Merged list of ordered context columns.
        '''

        left, right = (
            ds.context
            for ds in self._datasets
            )

        if left.empty:
            return right.ordered

        if right.empty:
            return left.ordered

        right_map = {
            col.name: col
            for col in right.ordered
            }

        out = []

        for col in left.ordered:
            out.append(col)

            if col.name in right_map:
                out.append(right_map.pop(col.name))

        out.extend(list(right_map.values()))

        return out


    def _merge_universal_context_columns(self):
        '''
        Description
        ------------
        Merges the left and right datasets' universal set of context columns.

        Returns
        ------------
        out : set
            Merged set of universal context columns.
        '''

        a, b = (
            ds.context.universal.copy()
            for ds in self._datasets
            )

        return a.union(b)


    def _merge_targeted_context_columns(self):
        '''
        Description
        ------------
        Merges the left and right datasets' targeted lists of context columns.

        Returns
        ------------
        a : dict
            Merged dictionary of targeted context columns.
        '''

        a, b = (
            ds.context.targeted.copy()
            for ds in self._datasets
            )

        for k, v in b.items():
            a[k] = a[k].union(v) if k in a else v

        return a


    def _iter_zip_collect(self, items, plans):
        '''
        Description
        ------------
        Materializes plans then yields them paired with their corresponding
        items.

        Parameters
        ------------
        items : iterable
            Items to pair with each result.
        plans : list
            Polars lazy plans to be executed in parallel.

        Yields
        ------------
        item : any
            Corresponding item.
        df : pl.DataFrame
            The materialized result of the lazy plan.
        '''

        results = pl.collect_all(plans)

        for item, df in zip(items, results, strict=True):
            yield item, df


    def _matches_origin(self, value):
        '''
        Description
        ------------
        Creates a boolean mask expression for filtering rows based on their
        origin category.

        Parameters
        ------------
        value : str
            Row origin category to filter by. Must be 'both', 'left', or
            'right'.

        Returns
        ------------
        expr : pl.Expr
            Boolean mask expression.
        '''

        odd.validate_value(
            value=value,
            types=str,
            whitelist=['both','left','right']
            )

        return self._row_origin.col == value


    def _full_join(self, left_data, right_data, join_on):
        '''
        Description
        ------------
        Performs a full join on the left and right datasets (data or metadata)
        and adds a '_row_origin' categorical column indicating the source of
        each row: 'both', 'left', or 'right'.

        Parameters
        ------------
        left_data : pl.LazyFrame | pl.DataFrame
            Data or metadata from the left dataset.
        right_data : ↑ same
            Data or metadata from the left dataset.
        join_on : str | list[str]
            Column name(s) on which to join 'left_data' and 'right_data'.

        Returns
        ------------
        obj : pl.LazyFrame | pl.DataFrame
            Amended query plan or join result including a new '_row_origin'
            categorical column.
        '''

        # full join left and right data
        out = left_data.join(
            right_data,
            on=join_on,
            how='full',
            coalesce=True,
            )

        # add categorical column indicating the origin of each row
        left_flag, right_flag = (
            ds.side_flag.col
            for ds in self._datasets
            )

        out = (
            out
            .with_columns(
                pl.when((left_flag) & (right_flag))
                .then(pl.lit('both'))
                .when((left_flag) & (right_flag.is_null()))
                .then(pl.lit('left'))
                .when((left_flag.is_null()) & (right_flag))
                .then(pl.lit('right'))
                .cast(pl.Categorical)
                .alias(self._row_origin.name)
                )
            .drop([left_flag, right_flag])
            )

        return out


    def _get_value_comparison_specs(self):
        '''
        Description
        ------------
        Maps shared columns with comparable data types to complementary
        SharedColumn subclass instances.

        Returns
        ------------
        spec_map : dict[str: SharedColumn] | None
            Refer to description.
        '''

        # filter schema for comparable shared columns
        df = self._schema._get_shared_columns(
            compatible=True
            )

        if df.is_empty():
            return

        # get aliased broad data type category column
        kind = self._left_data.schema.kind.alias

        # filter shared columns where both are entirely null
        df = (
            df
            .filter((
                kind.col != 'Null'
                ))
            .select([
                self._schema.uid.name,
                kind.col.alias(kind.base.name)
                ])
            )

        if df.is_empty():
            return

        # map broad data type categories to SharedColumn subclasses
        cls_map = {
            'Numeric': NumericColumn,
            'Temporal': TemporalColumn,
            'String': StringColumn,
            'Boolean': BooleanColumn,
            }

        # map shared column names to new shared column instances
        spec_map = {}

        for name, kind in df.iter_rows():
            spec_map[name] = cls_map[kind](
                parent=self,
                name=name
                )

        return spec_map


    def _log_unmatched_row_differences(self, df):
        '''
        Description
        ------------
        Records rows from each dataset that have no counterpart in the other.

        Parameters
        ------------
        df : pl.DataFrame
            Rows that exist in one dataset but not the other.

        Returns
        ------------
        None
        '''

        dimension = 'rows'

        # log each dataset's unmatched rows
        lf = df.lazy()
        plans = []

        for ds in self._datasets:

            # include all default context columns in order
            context_columns = [
                col.alias.name
                for col in ds.context.ordered
                if col.alias.name in ds.context.universal
                ]

            plan = (
                lf
                .filter((
                    self._matches_origin(ds.side)
                    ))
                .select([
                    *self.join_on,
                    *context_columns
                    ])
                )

            plans.append(plan)

        for ds, unmatched in self._iter_zip_collect(
            items=self._datasets,
            plans=plans
            ):

            if unmatched.is_empty():
                continue

            description = ds._to_not_in_description(dimension)

            self._add_difference(
                description=description,
                dimension=dimension,
                df=unmatched,
                total_count=ds.row_count
                )


    def _log_type_mismatch_value_differences(self):
        '''
        Description
        ------------
        For shared columns with incompatible data types, all corresponding
        matching rows are classified as value differences and recorded in the
        summary.

        Returns
        ------------
        None
        '''

        # filter schema for non-comparable shared columns
        df = self._schema._get_shared_columns(
            compatible=False
            )

        if df.is_empty():
            return

        # log all matching rows as a value difference
        total_count = self._matching_row_count

        if total_count == 0:
            return

        for description in df['column']:
            self._add_summary_row(
                description=description,
                dimension='values',
                delta_count=total_count,
                total_count=total_count
                )


    def _compare_columns(self):
        ''' detect missing columns and data type differences '''
        self._schema._compare_column_names()
        self._schema._compare_column_types()


    @print_duration()
    def _compare_rows_and_values(self):
        ''' detect unmatched rows and value differences '''

        # For shared columns, If data types are:
        #   • compatible → retrieve the value comparison specifications.
        #   • incompatible → log the differences due to type mismatch.
        if self._vals_in_scope:
            self._log_type_mismatch_value_differences()
            spec_map = self._get_value_comparison_specs()

        # Compile boolean expressions to flag rows that are either unmatched
        # or contain at least one value difference.
        with_exprs = []
        diff_exprs = []

        # add boolean column indicating unmatched rows
        if self._rows_in_scope:
            row_diff = Column(
                name='_row_difference',
                expr=(~self._in_both_mask)
                )
            with_exprs.append(row_diff.expr)
            diff_exprs.append(row_diff.col)

        # add struct column comprised of all "unequal" boolean expressions
        if self._vals_in_scope and spec_map:
            val_diffs = Column(
                name='_value_differences',
                expr=pl.struct([
                    spec.unequal.expr
                    for spec in spec_map.values()
                    ])
                )

            with_exprs.append(val_diffs.expr)
            struct = val_diffs.col.struct
            diff_exprs.append(struct.field('*'))

        # return early if no difference expressions
        if not diff_exprs:
            return

        # materialize DataFrame
        df = (
            self._lf
            .with_columns(with_exprs)
            .filter(
                pl.any_horizontal(diff_exprs)
                )
            .collect()
            )

        # return early if no differences were identified
        if df.is_empty():
            return

        # log unmatched rows then drop them from the DataFrame
        if self._rows_in_scope:
            unmatched = df.filter(row_diff.col)

            if not unmatched.is_empty():
                self._log_unmatched_row_differences(unmatched)
                df = df.filter(~row_diff.col)

            df = df.drop(row_diff.name)

        # return early if there are no matched rows or values are out-of-scope
        if df.is_empty() or not self._vals_in_scope:
            return

        # Drop entirely False struct fields (i.e. no differences identified)
        # and their associated columns, unless designated for context.

        # list of struct fields to retain
        keep_fields = []

        # list of column names to drop
        drop_columns = []

        any_diffs = df.select(
            struct.field('*').any()
            )

        context_set = set(
            col.alias.name
            for col in self._context.ordered
            )

        for col in list(spec_map.keys()):
            spec = spec_map[col]
            field = spec.unequal.name

            if any_diffs[field].item():
                keep_fields.append(field)
                continue

            del spec_map[col]

            drop_columns.extend([
                side.name
                for side in spec.sides
                if side.name not in context_set
                ])

        if not keep_fields:
            raise AssertionError(
                f'Expected to retain at least one struct field.'
                )

        df = (
            df
            .drop(drop_columns)
            .with_columns(
                pl.struct((
                    struct.field(keep_fields)
                    ))
            .alias(val_diffs.name))
            )

        # log value differences
        lf = df.lazy()
        plans = []

        for spec in spec_map.values():
            plan = (
                lf
                .filter(struct.field(spec.unequal.name))
                .select(spec.select_exprs)
                )
            plans.append(plan)

        for col, diffs in self._iter_zip_collect(
            items=list(spec_map.keys()),
            plans=plans
            ):

            if diffs.is_empty():
                raise AssertionError(
                    f'Expected value differences for column: {col!r}'
                    )

            self._add_difference(
                description=col,
                dimension='values',
                df=diffs,
                total_count=self._matching_row_count
                )


    def _perform_comparison(self):
        '''
        Description
        ------------
        Performs the comparison across all specified dimensions while
        concurrently logging summary statistics and storing detailed results.

        Returns
        ------------
        None
        '''

        if self.verbose:
            print(self._vs_label + ':')

        if self._cols_in_scope:
            self._compare_columns()

        if self._rows_in_scope or self._vals_in_scope:
            self._compare_rows_and_values()


    def _add_summary_row(
        self,
        description,
        dimension,
        delta_count,
        total_count
        ):
        '''
        Description
        ------------
        Appends a new summary row to self._summary_rows.

        Parameters
        ------------
        description : str
            Difference description.
        dimension : str
            Difference dimension: 'rows','columns', or 'values'
        delta_count : int
            Number of differences identified.
        total_count : int
            Total number of differences that could potentially occur.

        Returns
        ------------
        None
        '''

        row = {
            'Comparison': description,
            'Dimension': dimension,
            'Differences': delta_count,
            'Total': total_count,
            }

        self._summary_rows.append(row)


    def _add_difference(
        self,
        description,
        dimension,
        df,
        total_count
        ):
        '''
        Description
        ------------
        Stores newly identified differences in self._differences and adds a
        corresponding summary row to self._summary_rows.

        Parameters
        ------------
        df : pl.DataFrame
            DataFrame containing differences.

        Note:
        For others see '_add_summary_row()' documentation.

        Returns
        ------------
        None
        '''

        self._differences[description] = df

        self._add_summary_row(
            description=description,
            dimension=dimension,
            delta_count=df.height,
            total_count=total_count
            )


    @print_duration()
    def _summarize_differences(self):
        '''
        Description
        ------------
        Aggregates summary statistics collected during the comparison into a
        single summary DataFrame.

        Returns
        ------------
        df : pl.DataFrame
            Summary DataFrame.
        '''

        schema = {
            'Comparison': pl.Utf8,
            'Dimension': pl.Utf8,
            'Differences': pl.Int64,
            'Matches': pl.Int64,
            'Total': pl.Int64,
            'Match Rate %': pl.Float64,
            }

        # return an empty DataFrame if no differences were detected
        if not self._summary_rows:
            df = pl.DataFrame(schema=schema)
            return df

        # build summary DataFrame from accumulated summary rows
        df = pl.DataFrame(data=self._summary_rows)

        # clear the buffer
        self._summary_rows.clear()

        # derive 'Matches' & 'Match Rate %' columns, then enforce schema types
        tot_col = pl.col('Total')

        exprs = [
            (tot_col - pl.col('Differences')).alias('Matches'),
            (pl.col('Matches') / tot_col).alias('Match Rate %'),
            [pl.col(k).cast(v) for k, v in schema.items()],
            ]

        for expr in exprs:
            df = df.with_columns(expr)

        # naturally sort value difference rows by shared column name
        col_names = (
            df
            .filter(
                (pl.col('Dimension') == 'values')
                )
            .select('Comparison')
            .to_series()
            .to_list()
            )

        col_names = odd.natural_sort(col_names)
        col_rank = {v: i for i, v in enumerate(col_names)}

        df = (
            df
            .with_columns((
                pl.when(pl.col('Dimension') == 'values')
                .then(pl.col('Comparison').replace(col_rank))
                .alias('Rank')
                ))
            .sort(by=['Dimension','Rank'])
            .select(list(schema.keys()))
            )

        return df
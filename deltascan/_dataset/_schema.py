from functools import cached_property

import polars as pl

from .._column import Column


class Schema(object):
    '''
    Description
    --------------------
    Dataset schema

    Instance Attributes
    --------------------
    _dataset : Dataset
        Dataset instance to which the schema belongs.
    _dict : OrderedDict
        Dictionary returned by 'lf.collect_schema()'.
    df : pl.DataFrame
        See '_init_frame()' documentation below.
    '''

    #╭-------------------------------------------------------------------------╮
    #| Initialize Instance                                                     |
    #╰-------------------------------------------------------------------------╯

    def __init__(self, dataset, lf):
        '''
        Parameters
        ------------
        dataset : Dataset
            See class documentation.
        lf : pl.LazyFrame
            Lazy frame from which to collect the schema.
        '''
        self._dataset = dataset
        self._dict = lf.collect_schema()
        self._validate_summary_name()
        self.df = self._init_frame()


    #╭-------------------------------------------------------------------------╮
    #| Magic Methods                                                           |
    #╰-------------------------------------------------------------------------╯

    def __iter__(self):
        ''' iterate over schema column names '''
        return iter(self._dict.keys())


    def __contains__(self, item):
        ''' check if a column name is in the schema '''
        return item in self._dict


    def __len__(self):
        ''' number of columns in the schema '''
        return len(self._dict)


    #╭-------------------------------------------------------------------------╮
    #| Cached Properties                                                       |
    #╰-------------------------------------------------------------------------╯

    @cached_property
    def uid(self):
        ''' unique identifier column '''
        return self._make_column('column')


    @cached_property
    def kind(self):
        ''' broad data type category column '''
        return self._make_column('kind')


    @cached_property
    def dtype(self):
        ''' specific data type column '''
        return self._make_column('dtype')


    #╭-------------------------------------------------------------------------╮
    #| Properties                                                              |
    #╰-------------------------------------------------------------------------╯

    @property
    def columns(self):
        ''' list of column names '''
        return list(self._dict.keys())


    #╭-------------------------------------------------------------------------╮
    #| Instance Methods                                                        |
    #╰-------------------------------------------------------------------------╯

    def _validate_summary_name(self):
        ''' validates that no column name conflicts with the summary name '''
        column = self._dataset._parent.summary_name

        if column in self:
            raise ValueError(
                f"{self._dataset._data_param_name!r} includes a column name "
                f"that conflicts with 'summary_name': {column!r}. Please "
                "choose a different summary name."
                )


    def _make_column(self, name):
        '''
        Description
        ------------
        Initialize a Column instance for a given column name.

        Parameters
        ------------
        name : str
            Schema column name.

        Returns
        ------------
        col : Column
            Column instance
        '''
        return Column(name=name, dataset=self._dataset)


    def _init_frame(self):
        '''
        Description
        ------------
        Converts schema dictionary into a DataFrame.

        Returns
        ------------
        df : pl.DataFrame
            'column' : str
                Column name
            'kind' : str
                Data type category name
            'dtype' : str
                Polars data type name
            '_{side}_flag' : bool
                Side indicator
        '''

        rows = []
        renames = {'String': 'Utf8'}

        for k, v in self._dict.items():
            kind = self._identify_kind(k)
            dtype = str(v)
            dtype = renames.get(dtype.title(), dtype)
            rows.append([k, kind, dtype])

        schema = {
            col: pl.String for col in
            ('column','kind','dtype')
            }

        df = pl.DataFrame(
            data=rows,
            schema=schema,
            orient='row'
            )

        df = self._dataset._add_side_flag(df)

        return df


    def _identify_kind(self, column):
        '''
        Description
        ------------
        Identifies the broad data type category to which a column belongs.

        Parameters
        ------------
        column : str
            Schema column name.

        Returns
        ------------
        kind : str
            Broad data type category to which the column belongs.
        '''

        dtype = self._dict[column]

        if dtype.is_numeric():
            return 'Numeric'
        elif dtype.is_temporal():
            return 'Temporal'
        elif dtype.is_(pl.Boolean):
            return 'Boolean'
        elif dtype.is_(pl.Null):
            return 'Null'
        elif any(
            dtype.is_(x) for x in (
                pl.String,
                pl.Categorical,
                pl.Enum,
                )
            ):
            return 'String'

        raise NotImplementedError(
            f'Failed to categorize data type {dtype!r} for '
            f'column {column!r} into a known type category.'
            )
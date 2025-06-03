"""
Handles the logic of building the columnar output of the system.
"""

import inspect
import types
import typing

import pyarrow

from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    FundamentalData,
    FundamentalDataRow,
    MarketData,
    MarketDataDailyRow,
    SplitData,
)
from kaxanuk.data_curator.exceptions import (
    ColumnBuilderCircularDependenciesError,
    ColumnBuilderCustomFunctionNotFoundError,
    ColumnBuilderNoDatesToInfillError,
    ColumnBuilderUnavailableEntityFieldError,
    InjectedDependencyError,
)
# can't import directly from kaxanuk.data_curator.DataColumn because of circular import error:
from kaxanuk.data_curator.modules.data_column import DataColumn
from kaxanuk.data_curator.services.entity_helper import DataclassProtocol


# Type for a list of modules with calculation functions
type CalculationModules = list[types.ModuleType]
# Type for the identifiers of the columns, e.g. c_eps
type ColumnIdentifier = str
# Type for the container of the columns that have been completely calculated
type CompletedColumns = dict[ColumnIdentifier, DataColumn | Configuration]
# Type for the indexed data rows we'll use to build some columns
type DataRows = dict[str, DataclassProtocol | None]
# Type for factors expanded by date (example: dividends, splits)
type ExpandedDatedFactors = dict[str, dict[str, typing.Any] | None]
# Type for the container of the columns with unresolved dependencies
type PostponedColumns = dict[ColumnIdentifier, list[ColumnIdentifier]]


class ColumnBuilder:
    """
    Class for building the columns we need, based on the data entities provided.

    Initiate it with the data entities, and then run process_columns().
    """

    # the names of fields generated from combining columns, like in dividends and splits, will go here:
    _COMBINED_COLUMN_FIELD_NAMES : typing.ClassVar = {}

    # @todo: these should probably be in dividend_data instead of here:
    _DIVIDEND_DATE_FIELDS = (
        'declaration_date',
        'ex_dividend_date',
        'record_date',
        'payment_date',
    )
    _DIVIDEND_FACTOR_FIELDS = (
        'dividend',
        'dividend_split_adjusted',
    )
    _SPLIT_DATE_FIELDS = (
        'split_date',
    )
    _SPLIT_FACTOR_FIELDS = (
        'numerator',
        'denominator',
    )

    def __init__(
        self,
        *,
        calculation_modules: CalculationModules,
        configuration: Configuration,
        dividend_data: DividendData,
        fundamental_data: FundamentalData,
        market_data: MarketData,
        split_data: SplitData,
    ):
        self.calculation_modules = calculation_modules

        if not isinstance(configuration, Configuration):
            raise InjectedDependencyError("Incorrect configuration passed to ColumnBuilder")

        self.configuration = configuration

        if not isinstance(market_data, MarketData):
            raise InjectedDependencyError("Incorrect market_data passed to ColumnBuilder")

        self.market_data = market_data

        if not isinstance(fundamental_data, FundamentalData):
            raise InjectedDependencyError("Incorrect fundamental_data passed to ColumnBuilder")

        self.infilled_fundamental_data_rows = self._infill_data(
            iter(market_data.daily_rows.keys()),
            fundamental_data.rows
        )

        if not isinstance(dividend_data, DividendData):
            raise InjectedDependencyError("Incorrect dividend_data passed to ColumnBuilder")

        self.expanded_dividend_data_rows = self._expand_dated_factors(
            iter(market_data.daily_rows.keys()),
            self._DIVIDEND_DATE_FIELDS,
            self._DIVIDEND_FACTOR_FIELDS,
            dividend_data.rows
        )

        if not isinstance(split_data, SplitData):
            raise InjectedDependencyError("Incorrect split_data passed to ColumnBuilder")

        self.expanded_split_data_rows = self._expand_dated_factors(
            iter(market_data.daily_rows.keys()),
            self._SPLIT_DATE_FIELDS,
            self._SPLIT_FACTOR_FIELDS,
            split_data.rows
        )

    def process_columns(
        self,
        columns: tuple[ColumnIdentifier, ...]
    ) -> pyarrow.Table:
        """
        Create the required columns by using the data entities and running the calculation functions.

        Parameters
        ----------
        columns
            Tuple containing the columns to process and output

        Returns
        -------
        A pyarrow.Table containing all the output columns

        Raises
        ------
        ColumnBuilderCircularDependenciesError
        """
        # here we will save the columns that we've finished obtaining:
        completed_columns: CompletedColumns = {}
        # here we will save the column names of the calculated columns that we can't yet calculate, as the
        # columns they require as parameters haven't been calculated yet. Each key is a column name, and
        # its value is a list containing the names of its parameters that haven't been yet calculated.
        postponed_columns: PostponedColumns = {}

        completed_columns['configuration'] = self.configuration
        self._process_columns_with_available_dependencies(
            set(columns),
            completed_columns,
            postponed_columns,
            calculation_modules=self.calculation_modules,
            expanded_dividend_data_rows=self.expanded_dividend_data_rows,
            expanded_split_data_rows=self.expanded_split_data_rows,
            infilled_fundamental_data_rows=self.infilled_fundamental_data_rows,
            market_data_rows=self.market_data.daily_rows,
        )
        while len(postponed_columns) > 0:
            initial_completed_count = len(completed_columns)
            initial_postponed_count = len(postponed_columns)
            extended_columns = set(columns)
            for column in postponed_columns:
                extended_columns |= set(postponed_columns[column])

            self._process_columns_with_available_dependencies(
                extended_columns,
                completed_columns,
                postponed_columns,
                calculation_modules=self.calculation_modules,
                expanded_dividend_data_rows=self.expanded_dividend_data_rows,
                expanded_split_data_rows=self.expanded_split_data_rows,
                infilled_fundamental_data_rows=self.infilled_fundamental_data_rows,
                market_data_rows=self.market_data.daily_rows,
            )

            if (
                len(postponed_columns) > 0
                and len(postponed_columns) == initial_postponed_count
                and len(completed_columns) == initial_completed_count
            ):
                # we've run into a fixed point, which means 2 or more calculation functions are calling each other
                msg = " ".join([
                    "Circular dependency detected, the following column calculation functions seem to be calling",
                    "each other in a loop:",
                    ", ".join([
                        *postponed_columns.keys()
                    ])
                ])
                raise ColumnBuilderCircularDependenciesError(msg)

        return pyarrow.Table.from_pydict(
            {
                column: completed_columns[column]
                for column in columns
            }
        )

    @staticmethod
    def _add_column_dependency(
        postponed_columns: PostponedColumns,
        column: ColumnIdentifier,
        dependency: ColumnIdentifier
    ) -> None:
        """
        Register that `column` depends on `dependency` by adding it to `postponed_columns`.

        This allows us to keep track which dependencies need to be resolved before we can calculate the values
        of `column`

        Parameters
        ----------
        postponed_columns
            The PostponedColumns containing the dependencies. Passed and mutated by reference
        column
            The column whose dependency will be added
        dependency
            The dependency of `column` that will be registered
        """
        if column not in postponed_columns:
            postponed_columns[column] = [dependency]
        elif dependency not in postponed_columns[column]:
            postponed_columns[column].append(dependency)

    @staticmethod
    def _expand_dated_factors(
        dates: typing.Iterator,
        date_fields: tuple[str, ...],
        factor_fields: tuple[str, ...],
        data_rows: DataRows
    ) -> ExpandedDatedFactors:
        """
        Create an outer product between date_fields and factor_fields, with value factor_field for each date_field.

        An example use is dividends, where there are 4 dates and 2 amounts associated with each dividend, and we want
        1 column for each date and amount combination, with the amount as value for that specific date, and None
        otherwise.

        Parameters
        ----------
        dates
            An iterator with dates that we will use as indices for all row data associations

        date_fields
            The date field names

        factor_fields
            The factor field names

        data_rows
            The original data rows containing entities with a field per each `date_fields` and each `factor_fields`

        Returns
        -------
        A dict with date strings as keys, and dicts with date_fields * factor_fields columns as values
        """
        if len(data_rows) < 1:
            return dict.fromkeys(dates)

        # expand the factor_fields data for each date_fields
        expanded_dates = {
            key: {}
            for key in date_fields
        }
        for row in data_rows.values():
            for date_field in date_fields:
                if getattr(row, date_field) is not None:
                    cur_date = str(getattr(row, date_field))
                    expanded_dates[date_field][cur_date] = {}
                    for factor_field in factor_fields:
                        expanded_dates[date_field][cur_date][factor_field] = getattr(row, factor_field)

        output_data = {}
        for date in dates:
            output_data[date] = {}
            for expanded_date_field, expanded_data in expanded_dates.items():
                if date in expanded_data:
                    for factor_field, factor_data in expanded_data[date].items():
                        factor_field_identifier = f'{expanded_date_field}_{factor_field}'
                        output_data[date][factor_field_identifier] = factor_data

        return output_data

    @classmethod
    def _generate_column(
        cls,
        data_entity_rows: dict[str, object],
        field: str,
        subfield: str | None = None
    ) -> DataColumn:
        """
        Return a DataColumn containing the column data for the chosen field.subfield of the data entity rows.

        Parameters
        ----------
        data_entity_rows
            The rows on the data entity from which to extract the fields and subfields
        field
            The name of the field
        subfield
            The name of the subfield

        Returns
        -------
        The generated DataColumn
        """
        return DataColumn.load(
            [
                cls._get_field_from_row(row, field, subfield)
                for row in data_entity_rows.values()
            ],
        )

    @staticmethod
    def _get_calculation_function(
        function_name: str,
        calculation_modules: CalculationModules
    ) -> typing.Callable:
        """
        Look for a calculation function among the custom_module (if exists) or the calculations_module, and return it.

        Parameters
        ----------
        function_name
            The name of the function we're looking for
        calculation_modules
            The list of modules where we're searching for the function.

        Returns
        -------
        The function itself, from the first module in the list that defines it

        Raises
        ------
        ColumnBuilderCustomFunctionNotFoundError
        """
        for calculation_module in calculation_modules:
            if hasattr(calculation_module, function_name):
                return getattr(calculation_module, function_name)
        else:   # noqa: PLW0120
            msg = f"Custom column calculation function not found: {function_name}"
            raise ColumnBuilderCustomFunctionNotFoundError(msg)

    @staticmethod
    def _get_class_of_first_non_empty_row(
        data_rows: dict[str, object],
        subentity_name: str | None = None
    ) -> type | None:
        """
        Determine the class type of the first row value entity in data_rows.

        Parameters
        ----------
        data_rows
            Data rows where the keys are strings and the values are entity objects.
        subentity_name
            The name of the field of the subentity inside the entuty whose class we want to determine, if any

        Returns
        -------
        The class type of the first row value entity, or None if empty.

        Raises
        ------
        AttributeError if the subentity_name is not found in the entity
        """
        if len(data_rows) < 1:
            return None

        for row in data_rows.values():
            if (
                row is not None
                and subentity_name is None
            ):
                return type(
                    row
                )

            if (
                row is not None
                and subentity_name is not None
                and getattr(row, subentity_name, None) is not None
            ):
                return type(
                    getattr(row, subentity_name)
                )

        return None

    @classmethod
    def _get_combined_field_column_names(
        cls,
        field_source: str,
        date_fields: tuple[str, ...],
        factor_fields: tuple[str, ...],
    ) -> list[str, ...]:
        """
        For columns generated from date/factor field combinations, return the combined column names for field_source.

        Specific field sources like dividends and splits output single events for each date and factor combination,
        for example dividend.ex_dividend_date and dividend.dividend combine to output the value of dividend.dividend
        for the dividend.ex_dividend_date, with the output column named ex_dividend_date_dividend.

        Multiple dates and multiple factors create one column per each possible date/factor combination.

        Parameters
        ----------
        field_source
            The source data of the fields we're combining, e.g. 'dividend', 'split', etc.
        date_fields
            The entity date fields denoting the dates of the events
        factor_fields
            The entity factor fields denoting the values that will be ascribed to each date

        Returns
        -------
        The names of the columns generated from all date/factor field combinations
        """
        # memoize the shit out of this
        if field_source not in cls._COMBINED_COLUMN_FIELD_NAMES:
            cls._COMBINED_COLUMN_FIELD_NAMES[field_source] = [
                f'{date}_{factor}'
                for date in date_fields
                for factor in factor_fields
            ]

        return cls._COMBINED_COLUMN_FIELD_NAMES[field_source]

    @staticmethod
    def _get_field_from_row(
        row: dict | object,
        field: str,
        subfield: str | None = None
    ) -> typing.Any:
        """
        Extract a data entity field or subfield from a row, returning None if not exists.

        Parameters
        ----------
        row
            The rows on the data entity from which to extract the fields and subfields
        field
            The name of the field
        subfield
            The name of the subfield

        Returns
        -------
        The requested field or subfield, or None if not exists
        """
        if isinstance(row, dict):
            field_value = row.get(field, None)
            if subfield is not None:
                if isinstance(field_value, dict):
                    return field_value.get(subfield, None)
                else:
                    return None
            else:
                return field_value

        if (
            row is None
            or not hasattr(row, field)
        ):
            return None
        elif subfield is None:
            return getattr(row, field)
        else:
            parent_field = getattr(row, field)
            if not hasattr(parent_field, subfield):
                return None
            else:
                return getattr(
                    parent_field,
                    subfield
                )

    @staticmethod
    def _get_function_params(callable_function: typing.Callable) -> list[str]:
        """
        Return the names of the callable function's parameters.

        Parameters
        ----------
        callable_function
            The callable whose parameter names we want to extract

        Returns
        -------
        list of the callable's parameter names

        Raises
        ------
        TypeError
        """
        if not callable(callable_function):
            msg = "ColumnBuilder._get_function_params callable_function parameter is not callable"
            raise TypeError(msg)

        return list(
            inspect.signature(callable_function).parameters.keys()
        )

    @staticmethod
    def _infill_data(
        dates: typing.Iterator,
        data_rows: DataRows
    ) -> DataRows:
        """
        Infill the data rows, duplicating the previous row for each date in `dates` not present in `data_rows`.

        Parameters
        ----------
        dates
            The dates that will serve as the indices of the data that we will be infilling
        data_rows
            The data that will be used to infill the rows for each date in `dates`

        Returns
        -------
        The infilled  data rows for all `dates`

        Raises
        ------
        ColumnBuilderNoDatesToInfillError
        """
        if len(data_rows) < 1:
            return dict.fromkeys(dates)

        infilled_data = {}
        data_row_dates = iter(data_rows.keys())
        previous_data_row_date = None
        current_data_row_date = next(data_row_dates)

        try:
            first_date = next(dates)
        except StopIteration as err:
            raise ColumnBuilderNoDatesToInfillError from err

        # Find the data_row right before the first date in dates
        if first_date > current_data_row_date:
            while first_date >= current_data_row_date:
                try:
                    previous_data_row_date = current_data_row_date
                    current_data_row_date = next(data_row_dates)
                    continue
                except StopIteration:
                    break

        # need to fill in the first element as we already advanced the dates internal cursor
        infilled_data[first_date] = data_rows.get(previous_data_row_date)

        for date in dates:
            if date < current_data_row_date:
                infilled_data[date] = data_rows.get(previous_data_row_date)
            else:
                try:
                    previous_data_row_date = current_data_row_date
                    current_data_row_date = next(data_row_dates)
                except StopIteration:
                    pass

                infilled_data[date] = data_rows.get(previous_data_row_date)

        return infilled_data

    @classmethod
    def _process_columns_with_available_dependencies(
        cls,
        columns: set[ColumnIdentifier],
        completed_columns: CompletedColumns,
        postponed_columns: PostponedColumns,
        *,
        calculation_modules: CalculationModules,
        expanded_dividend_data_rows: ExpandedDatedFactors,
        expanded_split_data_rows: ExpandedDatedFactors,
        infilled_fundamental_data_rows: DataRows,
        market_data_rows: dict[str, MarketDataDailyRow],
    ) -> None:
        """
        Calculate the columns that have all their dependencies already resolved.

        Parameters
        ----------
        columns
            The set of all columns the system must output
        completed_columns
            The CompletedColumns containing the completely calculated columns. Passed and mutated by reference
        postponed_columns
            The PostponedColumns containing the columns with unresolved dependencies. Passed and mutated by reference

        Raises
        ------
        ColumnBuilderUnavailableEntityFieldError
        """
        for column in columns:
            if column in completed_columns:
                continue

            [column_type, column_name] = column.split('_', 1)
            match column_type:
                case 'c':       # computed functions
                    calculation_function = cls._get_calculation_function(
                        column,
                        calculation_modules
                    )
                    params = cls._get_function_params(calculation_function)
                    for param in params:
                        if param in completed_columns:
                            continue

                        [param_type, _] = param.split('_', 1)
                        if param_type == 'c':
                            cls._add_column_dependency(
                                postponed_columns,
                                column,
                                param
                            )
                        else:
                            cls._process_columns_with_available_dependencies(
                                {param},
                                completed_columns,
                                {},
                                calculation_modules=calculation_modules,
                                expanded_dividend_data_rows=expanded_dividend_data_rows,
                                expanded_split_data_rows=expanded_split_data_rows,
                                infilled_fundamental_data_rows=infilled_fundamental_data_rows,
                                market_data_rows=market_data_rows,
                            )
                    if (
                        column not in postponed_columns
                        or len(postponed_columns[column]) < 1
                    ):
                        param_columns = (completed_columns.get(p) for p in params)
                        # Execute the calculation function and wrap it in DataColumn (in case of Pandas.Series, etc.)
                        completed_columns[column] = DataColumn.load(
                            calculation_function(*param_columns)
                        )
                        if column in postponed_columns:
                            postponed_columns.pop(column)
                        cls._remove_column_dependency(postponed_columns, column)
                case 'd':       # dividends
                    if column_name not in cls._get_combined_field_column_names(
                        'dividend',
                        cls._DIVIDEND_DATE_FIELDS,
                        cls._DIVIDEND_FACTOR_FIELDS
                    ):
                        msg = f"Column not available in dividend data: {column}"
                        raise ColumnBuilderUnavailableEntityFieldError(msg)
                    else:
                        completed_columns[column] = cls._generate_column(
                            expanded_dividend_data_rows,
                            column_name
                        )
                case 'f':       # fundamental data
                    if not cls._property_exists_in_class(FundamentalDataRow, column_name):
                        msg = f"Column not available in fundamental data: {column}"
                        raise ColumnBuilderUnavailableEntityFieldError(msg)
                    else:
                        completed_columns[column] = cls._generate_column(
                            infilled_fundamental_data_rows,
                            column_name
                        )
                case 'fbs':     # fundamental data balance sheet
                    row_entity_class = cls._get_class_of_first_non_empty_row(
                        infilled_fundamental_data_rows,
                        'balance_sheet'
                    )
                    if (
                        row_entity_class is not None
                        and not cls._property_exists_in_class(row_entity_class, column_name)
                    ):
                        msg = f"Column not available in fundamental data balance_sheet: {column}"
                        raise ColumnBuilderUnavailableEntityFieldError(msg)
                    else:
                        completed_columns[column] = cls._generate_column(
                            infilled_fundamental_data_rows,
                            'balance_sheet',
                            column_name
                        )
                case 'fcf':     # fundamental data cash flow
                    row_entity_class = cls._get_class_of_first_non_empty_row(
                        infilled_fundamental_data_rows,
                        'cash_flow'
                    )
                    if (
                        row_entity_class is not None
                        and not cls._property_exists_in_class(row_entity_class, column_name)
                    ):
                        msg = f"Column not available in fundamental data cash_flow: {column}"
                        raise ColumnBuilderUnavailableEntityFieldError(msg)
                    else:
                        completed_columns[column] = cls._generate_column(
                            infilled_fundamental_data_rows,
                            'cash_flow',
                            column_name
                        )
                case 'fis':      # fundamental data income
                    row_entity_class = cls._get_class_of_first_non_empty_row(
                        infilled_fundamental_data_rows,
                        'income_statement'
                    )
                    if (
                        row_entity_class is not None
                        and not cls._property_exists_in_class(row_entity_class, column_name)
                    ):
                        msg = f"Column not available in fundamental data income_statement: {column}"
                        raise ColumnBuilderUnavailableEntityFieldError(msg)
                    else:
                        completed_columns[column] = cls._generate_column(
                            infilled_fundamental_data_rows,
                            'income_statement',
                            column_name
                        )
                case 'm':       # market data
                    row_entity_class = cls._get_class_of_first_non_empty_row(market_data_rows)
                    if not cls._property_exists_in_class(row_entity_class, column_name):
                        msg = f"Column not available in market data: {column}"
                        raise ColumnBuilderUnavailableEntityFieldError(msg)
                    else:
                        completed_columns[column] = cls._generate_column(
                            market_data_rows,
                            column_name
                        )
                case 's':       # splits
                    if column_name not in cls._get_combined_field_column_names(
                        'split',
                        cls._SPLIT_DATE_FIELDS,
                        cls._SPLIT_FACTOR_FIELDS
                    ):
                        msg = f"Column not available in split data: {column}"
                        raise ColumnBuilderUnavailableEntityFieldError(msg)
                    else:
                        completed_columns[column] = cls._generate_column(
                            expanded_split_data_rows,
                            column_name
                        )
                case _:
                    msg = f"Column {column_name} with unknown prefix: {column_type}"
                    raise ColumnBuilderUnavailableEntityFieldError(msg)

    @staticmethod
    def _property_exists_in_class(
        uninstantiated_class: type,
        property_name: str
    ) -> bool:
        """
        Check if a property with a given name exists in an uninstantiated class.

        Useful for checking if a column name corresponds to an entity property

        Parameters
        ----------
        uninstantiated_class
            The class whose properties we'll check
        property_name
            The name of the property we're looking for

        Returns
        -------
        Whether the property name exists in the uninstantiated class
        """
        properties = [
            item[0]
            for item in inspect.getmembers(uninstantiated_class)
        ]
        return property_name in properties

    @staticmethod
    def _remove_column_dependency(
        postponed_columns: PostponedColumns,
        dependency: ColumnIdentifier
    ) -> None:
        """
        Remove the dependency from its respective columns in postponed_columns.

        This allows us to keep track of just the unresolved dependencies, and calculate all columns which have had
        all its dependencies resolved

        Parameters
        ----------
        postponed_columns
            The PostponedColumns containing the dependencies. Passed and mutated by reference
        dependency
            The dependency that will be removed from all columns that registered it
        """
        for column in postponed_columns:
            if dependency in postponed_columns[column]:
                postponed_columns[column].remove(dependency)

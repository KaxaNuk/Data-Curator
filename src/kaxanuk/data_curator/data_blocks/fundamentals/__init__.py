from kaxanuk.data_curator.data_blocks.base_data_block import (
    BaseDataBlock,
    ConsolidatedFieldsTable,
    FieldValueToEntityMap,
)
from kaxanuk.data_curator.entities import (
    FundamentalData,
    FundamentalDataRow,
    FundamentalDataRowBalanceSheet,
    FundamentalDataRowCashFlow,
    FundamentalDataRowIncomeStatement,
    # MarketInstrumentIdentifier,
)
from kaxanuk.data_curator.exceptions import (
    DataBlockEmptyError,
    DataBlockEntityPackingError,
    EntityProcessingError,
    EntityValueError,
    FundamentalDataUnsortedRowDatesError,
)


class FundamentalsDataBlock(BaseDataBlock):
    clock_sync_field = FundamentalDataRow.filing_date   # @todo check if accepted_date should be used instead
    # groups by identifier type; only one identifier type per configuration is supported:
    grouping_identifier_field = FundamentalData.main_identifier
    main_entity = FundamentalData
    # main_prefix = 'f'
    prefix_to_entity_map = {
        'f': FundamentalDataRow,
        'fbs': FundamentalDataRowBalanceSheet,
        'fcf': FundamentalDataRowCashFlow,
        'fis': FundamentalDataRowIncomeStatement,
    }

    @classmethod
    def assemble_entities_from_consolidated_table(
        cls,
        *,
        # entity_tables: EntityBuildingTables # @todo receive consolidated table instead
        consolidated_table: ConsolidatedFieldsTable,
        common_field_data: FieldValueToEntityMap,
    ) -> FundamentalData:
        # @todo throw error if not sorted by date asc

        common_market_fields = common_field_data[FundamentalData]
        identifier = common_market_fields[FundamentalData.main_identifier]

        try:
            period_rows = cls.pack_rows_entities_from_consolidated_table(
                consolidated_table
            )
        except DataBlockEntityPackingError as error:
            raise EntityProcessingError("Fundametal data processing error") from error

        try:
            if not period_rows:
                msg = f"No rows could be processed by the {cls.__name__} data block for {identifier.identifier}"

                raise DataBlockEmptyError(msg)

            data_entity = FundamentalData(
                main_identifier=common_market_fields[FundamentalData.main_identifier],
                rows=period_rows,
            )
        except (
            DataBlockEmptyError,
            EntityValueError,
            FundamentalDataUnsortedRowDatesError,
        ) as error:
            msg = f"Fundamental data processing error for {identifier.identifier}"

            raise EntityProcessingError(msg) from error

        return data_entity



__all__ = [
    'FundamentalsDataBlock',
]

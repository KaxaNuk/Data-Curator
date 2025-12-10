import datetime

import pyarrow


# Define period end dates as keys and filing dates as values
filings = {
    "2022-03-31": "2023-05-15", # ammendment
    "2022-06-30": "2022-08-12",
    "2022-09-30": "2023-11-08", # ammendment
    "2022-12-31": "2024-02-14", # ammendment

    "2023-03-31": "2023-05-10",
    "2023-06-30": "2024-08-09", # late filing
    "2023-09-30": "2024-07-07", # late filing (on same date as acceptable one)
    "2023-12-31": "2024-07-07", # late but acceptable filing (on same date as late one)

    "2024-03-31": "2024-08-14", # late filing
    "2024-06-30": "2024-08-08",
    "2024-09-30": "2025-11-06", # ammendment
    "2024-12-31": "2025-02-12",
}
irregular_row_mask = [
    True,
    False,
    True,
    True,

    False,
    True,
    True,
    False,

    True,
    False,
    True,
    False,
]
# Net income values
net_income = [
    15000000,
    22000000,
    18500000,
    31000000,
    19500000,
    25000000,
    21000000,
    35000000,
    23000000,
    28000000,
    24500000,
    42000000,
]

# Extract filing dates and period end dates from the dict
period_end_dates = list(filings.keys())
filing_dates = list(filings.values())

# Create PyArrow arrays with date type
period_end_date_array = pyarrow.array(
    [datetime.date.fromisoformat(d) for d in period_end_dates],
    type=pyarrow.date32()
)
filing_date_array = pyarrow.array(
    [datetime.date.fromisoformat(d) for d in filing_dates],
    type=pyarrow.date32()
)
net_income_array = pyarrow.array(net_income)

filings_with_none = {
    "2023-05-10": "2023-03-31",
    "2024-05-31": None,     # none should not appear in mask
    "2024-08-08": None,     # none should not appear in mask
    "2025-11-06": "2024-09-30", # ammendment
    "2025-02-12": "2024-12-31",
}
net_income_with_none = [
    15000000,
    None,
    18500000,
    31000000,
    19500000,
]
irregular_row_with_none_mask = [
    False,
    False,
    False,
    True,
    False
]
period_end_dates_with_none = list(filings_with_none.values())
filing_dates_with_none = list(filings_with_none.keys())
period_end_date_with_none_array = pyarrow.array(
    [
        datetime.date.fromisoformat(d) if d is not None else None
        for d in period_end_dates_with_none
    ],
    type=pyarrow.date32()
)
filing_date_with_none_array = pyarrow.array(
    [
        datetime.date.fromisoformat(d) if d is not None else None
        for d in filing_dates_with_none
    ],
    type=pyarrow.date32()
)
net_income_with_none_array = pyarrow.array(net_income_with_none)

IRREGULAR_FILINGS_TABLE = pyarrow.table({
    'period_end_date': period_end_date_array,
    'filing_date': filing_date_array,
    'net_income': net_income_array
})
IRREGULAR_FILINGS_MASK = pyarrow.array(irregular_row_mask)

IRREGULAR_FILINGS_WITH_NONE_TABLE = pyarrow.table({
    'period_end_date': period_end_date_with_none_array,
    'filing_date': filing_date_with_none_array,
    'net_income': net_income_with_none_array
})
IRREGULAR_FILINGS_WITH_NONE_MASK = pyarrow.array(irregular_row_with_none_mask)

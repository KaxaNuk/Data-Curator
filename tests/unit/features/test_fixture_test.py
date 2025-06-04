import pytest
import csv
import pathlib
from io import StringIO

def fake_open(self, *args, **kwargs):
    # Simula un archivo vacío.
    return StringIO("")

def fake_dict_reader(f, skipinitialspace=None):
    if skipinitialspace is None:
        skipinitialspace = True
    return type("FakeReader", (), {
        "fieldnames": [],
        "__iter__": lambda self: iter([])
    })()

def test_empty_log_returns_and_annualized_volatility(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in log_returns_annualized_volatility.csv"):
        request.getfixturevalue("example_log_returns_and_annualized_volatility")

def test_empty_sales_to_price(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in sales_to_price.csv"):
        request.getfixturevalue("example_sales_to_price")

def test_empty_adjusted_price_ratio(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in adjusted_price_ratio.csv"):
        request.getfixturevalue("example_adjusted_price_ratio")

def test_empty_moving_average(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in moving_average.csv"):
        request.getfixturevalue("example_moving_average")

def test_empty_average_daily_traded_value(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in average_daily_traded_value.csv"):
        request.getfixturevalue("example_average_daily_traded_value")

def test_empty_logarithmic_difference_high_low(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in logarithmic_difference_high_low.csv"):
        request.getfixturevalue("example_logarithmic_difference_high_low")

def test_empty_market_cap(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in market_cap.csv"):
        request.getfixturevalue("example_market_cap")

def test_empty_book_to_price(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in book_to_price.csv"):
        request.getfixturevalue("example_book_to_price")

def test_empty_last_twelve_months_revenue_per_share(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in last_twelve_months_revenue_per_share.csv"):
        request.getfixturevalue("example_last_twelve_months_revenue_per_share")

def test_empty_earnings_to_price(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in earnings_to_price.csv"):
        request.getfixturevalue("example_earnings_to_price")

def test_empty_moving_average_convergence_divergence(monkeypatch, request):
    monkeypatch.setattr(pathlib.Path, "open", fake_open)
    monkeypatch.setattr(csv, "DictReader", fake_dict_reader)
    with pytest.raises(RuntimeError, match="No data in moving_average_convergence_divergence.csv"):
        request.getfixturevalue("example_moving_average_convergence_divergence")


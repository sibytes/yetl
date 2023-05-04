from pydantic import ValidationError
from dbxconfig import Timeslice, TimesliceNow, TimesliceUtcNow
from datetime import datetime


def test_timeslice_all():
    format = "%Y%m%d"
    timeslice = Timeslice(day="*", month="*", year="*")
    actual = timeslice.strftime(format)
    expected = "***"
    assert actual == expected


def test_timeslice_year():
    format = "%Y/%m/%d"
    timeslice = Timeslice(day="*", month="*", year=2023)
    actual = timeslice.strftime(format)
    expected = "2023/*/*"
    assert actual == expected


def test_timeslice_month():
    format = "%Y-%m-%d"
    timeslice = Timeslice(day="*", month=1, year=2023)
    actual = timeslice.strftime(format)
    expected = "2023-01-*"
    assert actual == expected


def test_timeslice_day():
    format = "%Y\\%m\\%d"
    timeslice = Timeslice(day=1, month=1, year=2023)
    actual = timeslice.strftime(format)
    expected = "2023\\01\\01"
    assert actual == expected


def test_timeslice_invalid():
    actual = None
    try:
        timeslice = Timeslice(day="s", month=1, year=2023)  # noqa F841
    except ValidationError as e:
        actual = e

    assert isinstance(actual, ValidationError)


def test_timeslice_invalid_date():
    actual = None
    format = "%Y/%m/%d"
    try:
        timeslice = Timeslice(day=500, month=1, year=2023)
        actual = timeslice.strftime(format)
    except ValueError as e:
        actual = e

    assert isinstance(actual, ValueError)


def test_timeslice_now():
    now = datetime.now()
    timeslice = TimesliceNow()

    assert (
        timeslice.day == now.day
        and timeslice.month == now.month
        and timeslice.year == now.year
        and timeslice.hour == now.hour
        and timeslice.minute == now.minute
    )


def test_timeslice_utcnow():
    now = datetime.utcnow()
    timeslice = TimesliceUtcNow()

    assert (
        timeslice.day == now.day
        and timeslice.month == now.month
        and timeslice.year == now.year
        and timeslice.hour == now.hour
        and timeslice.minute == now.minute
    )


def test_timeslice_invalid_format_code():
    format = "%Y-%m-%d-%c"
    actual = ""
    try:
        timeslice = Timeslice(day="*", month=1, year=2023)
        actual = timeslice.strftime(format)
    except Exception as e:
        actual = str(e)

    expected = "The format contains the following unsupported format codes: %c"

    assert actual == expected


def test_timeslice_str():
    timeslice = Timeslice(day=1, month=1, year=2023)
    actual = str(timeslice)
    expected = "2023-01-01 00:00:00.000000"
    assert actual == expected

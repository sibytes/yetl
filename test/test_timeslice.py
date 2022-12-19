from yetl.flow._timeslice import Timeslice


def test_timeslice_date():

    timeslice = Timeslice(year="2000", month="01", day="01")

    assert timeslice.strftime("%Y_%m_%d") == "2000_01_01"


def test_timeslice_wildcard_daymonth():

    timeslice = Timeslice(year="2000", month="*", day="*")

    assert timeslice.strftime("%Y-%m-%d") == "2000-*-*"


def test_timeslice_wildcard_year():

    timeslice = Timeslice(year="2000")

    assert timeslice.strftime("%Y/%m/%d") == "2000/*/*"


def test_timeslice_wildcard():

    timeslice = Timeslice(year="*")

    assert timeslice.strftime("%Y\%m\%d") == "*\*\*"

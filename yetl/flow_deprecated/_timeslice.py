from datetime import datetime
from typing import Literal, Union


_WILDCARD = "*"
Wildcard = Literal["*"]

_UNSUPPORTED_FORMAT_CODES = [
    "%c",
    "%x",
    "%X",
    "%G",
    "%u",
    "%V",
    "%z",
    "%Z",
    "%I",
    "%p",
    "%b",
    "%B",
    "%a",
    "%A",
    "%w",
]


class Timeslice:
    def __init__(
        self,
        year: Union[int, Wildcard],
        month: Union[int, Wildcard] = None,
        day: Union[int, Wildcard] = None,
        hour: Union[int, Wildcard] = 0,
        minute: Union[int, Wildcard] = 0,
        second: Union[int, Wildcard] = 0,
        microsecond: Union[int, Wildcard] = 0,
    ) -> None:

        self.year = self._wildcard_check(year)
        self.month = self._wildcard_check(month)
        self.day = self._wildcard_check(day)
        self.hour = self._wildcard_check(hour)
        self.minutue = self._wildcard_check(minute)
        self.second = self._wildcard_check(second)
        self.microsecond = self._wildcard_check(microsecond)

    def _wildcard_check(self, value: Union[int, Wildcard]):
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value == _WILDCARD:
            return value
        else:
            raise Exception(f"Timeslice parameters must be an int or '{_WILDCARD}'")

    def strftime(self, format: str):
        """This will format and return the timeslice using python format codes. Only a subset of format codes are suppoered by design

        %d - Day of the month as a zero-padded decimal number.
        %m - Month as a zero-padded decimal number.
        %y - Year without century as a zero-padded decimal number.
        %Y - Year with century as a decimal number.
        %H - Hour (24-hour clock) as a zero-padded decimal number.
        %M - Minute as a zero-padded decimal number.
        %S - Second as a zero-padded decimal number.
        %f - Microsecond as a decimal number, zero-padded to 6 digits.
        %% - A literal '%' character.
        %j - Day of the year as a zero-padded decimal number.
        %U - Week number of the year (Sunday as the first day of the week) as a zero-padded decimal number. All days in a new year preceding the first Sunday are considered to be in week 0.
        %W - Week number of the year (Monday as the first day of the week) as a zero-padded decimal number. All days in a new year preceding the first Monday are considered to be in week 0.

        NOT SUPPORTED - %c - Locale’s appropriate date and time representation.
        NOT SUPPORTED - %x - Locale’s appropriate date representation.
        NOT SUPPORTED - %X - Locale’s appropriate time representation.
        NOT SUPPORTED - %G - ISO 8601 year with century representing the year that contains the greater part of the ISO week (%V).
        NOT SUPPORTED - %u - ISO 8601 weekday as a decimal number where 1 is Monday.
        NOT SUPPORTED - %V - ISO 8601 week as a decimal number with Monday as the first day of the week. Week 01 is the week containing Jan 4.
        NOT SUPPORTED - %z - UTC offset in the form ±HHMM[SS[.ffffff]] (empty string if the object is naive).
        NOT SUPPORTED - %Z - Time zone name (empty string if the object is naive).
        NOT SUPPORTED - %I - Hour (12-hour clock) as a zero-padded decimal number.
        NOT SUPPORTED - %p - Locale’s equivalent of either AM or PM.
        NOT SUPPORTED - %b - Month as locale’s abbreviated name.
        NOT SUPPORTED - %B - Month as locale’s full name.
        NOT SUPPORTED - %a - Weekday as locale’s abbreviated name.
        NOT SUPPORTED - %A - Weekday as locale’s full name.
        NOT SUPPORTED - %w - Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
        """
        # format = "%Y%m%d"
        unsupported_codes = []
        for c in _UNSUPPORTED_FORMAT_CODES:
            if c in format:
                unsupported_codes.append(c)

        if unsupported_codes:
            unsupported_codes = ",".join(unsupported_codes)
            raise Exception(
                f"The format contains the following unsupported format codes: {unsupported_codes}"
            )

        if not self.day and any(fmt_code in format for fmt_code in ["%d", "%j"]):
            raise Exception(
                f"Timeslice does not contain day (day=None) failed to format timeslice with FormatCode = [%d,%j]"
            )

        if not self.month and "%m" in format:
            raise Exception(
                f"Timeslice does not contain month (month=None) failed to format timeslice with FormatCode = %m"
            )

        format, year = self._format_wildcard(format, self.year, ["%y", "%Y"], 1900)
        format, month = self._format_wildcard(format, self.month, "%m", 1)
        format, day = self._format_wildcard(format, self.day, "%d", 1)

        format, hour = self._format_wildcard(format, self.hour, "%H")
        format, minutue = self._format_wildcard(format, self.minutue, "%M")
        format, second = self._format_wildcard(format, self.second, "%S")
        format, microsecond = self._format_wildcard(format, self.microsecond, "%f")

        timeslice = datetime(year, month, day, hour, minutue, second, microsecond)

        formatted = timeslice.strftime(format)
        return formatted

    def _format_wildcard(
        self,
        format: str,
        datepart: Union[int, Wildcard],
        format_code: Union[list, str],
        default=0,
    ):

        if datepart == _WILDCARD:
            if isinstance(format_code, str):
                format = format.replace(format_code, f"{_WILDCARD}")
            elif isinstance(format_code, list):
                for f in format_code:
                    format = format.replace(f, f"{_WILDCARD}")
            datepart = default

        return format, datepart

    def __str__(self) -> str:
        return self.strftime("%Y-%m-%d %H:%M:%S.%f")


class TimesliceNow(Timeslice):
    def __init__(self) -> None:
        now = datetime.now()
        super().__init__(
            now.year,
            now.month,
            now.day,
            now.hour,
            now.minute,
            now.second,
            now.microsecond,
        )


class TimesliceUtcNow(Timeslice):
    def __init__(self) -> None:
        now = datetime.utcnow()
        super().__init__(
            now.year,
            now.month,
            now.day,
            now.hour,
            now.minute,
            now.second,
            now.microsecond,
        )

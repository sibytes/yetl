from datetime import datetime
from typing import Literal, Union

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


class Timestamp:
    def __init__(
        self,
        year: Union[int, Wildcard, None] = None,
        month: Union[int, Wildcard, None] = None,
        day: Union[int, Wildcard, None] = None,
        hour: Union[int, Wildcard, None] = None,
        minutue: Union[int, Wildcard, None] = None,
        second: Union[int, Wildcard, None] = None,
        microsecond: Union[int, Wildcard, None] = None,
    ) -> None:

        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minutue = minutue
        self.second = second
        self.microsecond = microsecond

    def strftime(self, format: str):
        """
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

        if not self.year and any(fmt_code in format for fmt_code in ["%y", "%y"]):
            raise Exception(
                f"Timeslice does not contain year (year=None) failed to format timeslice with FormatCode in [%y, %Y]"
            )

        if not self.hour and "%H" in format:
            raise Exception(
                f"Timeslice does not contain hour (hour=None) failed to format timeslice with FormatCode = %H"
            )

        if not self.minutue and "%M" in format:
            raise Exception(
                f"Timeslice does not contain minute (minute=None) failed to format timeslice with FormatCode = %M"
            )

        if not self.second and "%S" in format:
            raise Exception(
                f"Timeslice does not contain second (second=None) failed to format timeslice with FormatCode = %S"
            )

        if not self.microsecond and "%f" in format:
            raise Exception(
                f"Timeslice does not contain microsecond (microsecond=None) failed to format timeslice with FormatCode = %f"
            )

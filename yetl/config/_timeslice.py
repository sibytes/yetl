from datetime import datetime
from typing import Literal, Union
from pydantic import BaseModel, Field
from typing import Any
import re

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


class Timeslice(BaseModel):
    def __init__(__pydantic_self__, **data: Any) -> None:
        super().__init__(**data)

    year: Union[int, Wildcard] = Field(...)
    month: Union[int, Wildcard] = Field(default=_WILDCARD)
    day: Union[int, Wildcard] = Field(default=_WILDCARD)
    hour: Union[int, Wildcard] = Field(default=0)
    minute: Union[int, Wildcard] = Field(default=0)
    second: Union[int, Wildcard] = Field(default=0)
    microsecond: Union[int, Wildcard] = Field(default=0)

    @classmethod
    def parse_iso_date(cls, iso_date: str):
        if iso_date == "*":
            iso_date = "*-*-*"
        pattern = "^(([12]\d{3}|[*])-(0[1-9]|1[0-2]|[*])-(0[1-9]|[12]\d|3[01]|[*]))$"  # noqa W605
        result = re.match(pattern, iso_date)

        if result:
            parts = iso_date.split("-")
            args = {"year": parts[0], "month": parts[1], "day": parts[2]}
            return cls(**args)
        else:
            raise Exception(
                f"{iso_date} is an invalid iso date string. Must be the format YYYY-mm-dd"
            )

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

        unsupported_codes = [c for c in _UNSUPPORTED_FORMAT_CODES if c in format]

        if unsupported_codes:
            unsupported_codes = ",".join(unsupported_codes)
            raise Exception(
                f"The format contains the following unsupported format codes: {unsupported_codes}"
            )

        format, _year = self._format_wildcard(format, self.year, ["%y", "%Y"], 1900)
        format, _month = self._format_wildcard(format, self.month, "%m", 1)
        format, _day = self._format_wildcard(format, self.day, "%d", 1)

        format, _hour = self._format_wildcard(format, self.hour, "%H")
        format, _minutue = self._format_wildcard(format, self.minute, "%M")
        format, _second = self._format_wildcard(format, self.second, "%S")
        format, _microsecond = self._format_wildcard(format, self.microsecond, "%f")

        timeslice = datetime(
            _year, _month, _day, _hour, _minutue, _second, _microsecond
        )

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
        args = {
            "year": now.year,
            "month": now.month,
            "day": now.day,
            "hour": now.hour,
            "minute": now.minute,
            "second": now.second,
            "microsecond": now.microsecond,
        }
        super().__init__(**args)


class TimesliceUtcNow(Timeslice):
    def __init__(self) -> None:
        now = datetime.utcnow()
        args = {
            "year": now.year,
            "month": now.month,
            "day": now.day,
            "hour": now.hour,
            "minute": now.minute,
            "second": now.second,
            "microsecond": now.microsecond,
        }
        super().__init__(**args)

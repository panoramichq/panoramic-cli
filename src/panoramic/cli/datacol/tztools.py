"""
Copyright 2017 Daniel Dotsenko

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE
"""
# ^ MIT license

from datetime import date, datetime, timedelta, timezone


def dt_to_other_timezone(dt, destination_timezone_name, origin_timezone_name='UTC'):
    """
    The only, safest, way I know to convert datetime object from one timezone to
    another while accounting for things like Daylight Saving Time
    (also known as "Summer Time") and "leap" stuff.

    Tried many many other ways and anything that work with pure offsets is plain bad.
    Must work with proper tx names and pytz is the best way on Python.

    Offsets plainly don't work because DST-vs-no-DST varies based on specific locale.

    For example, US state of Arizona, while being part of `US/Mountain` time zone
    does NOT observe Daylight Saving Time, like rest of that time zone. As a result,
    it's effectively on US Mountain time zone in the winter and in US Pacific (right?)
    for most of rest of the year.

    Then, add to that the fact that Summer Time starts and ends on different dates
    depending on a country and, as a result, noon in San Diego, California is not
    guaranteed to be noon in Tijuana - a city only 30 kilometers *South*

    As a result of all the above, learned to specify timezone names as specifically
    as possible. Say, "America/Los_Angeles" vs "US/Pacific" and work only with
    time-zone-aware datetimes and only with timezones that are timezone-name aware
    and support something like Olson timezone DB (https://en.wikipedia.org/wiki/Tz_database)

    :param datetime.datetime dt:
        Some datetime object. May be timezone-naive, in which case origin timezone
        name is required and is used to localize the incoming dt before tz conversion.
    :param str destination_timezone_name:
        'UTC' or some standard tz name string like "America/Los_Angeles"
    :param str origin_timezone_name:
        'UTC' (default) or some standard tz name string like "Europe/Paris"
    :return:
    """

    from pytz import UTC
    from pytz import timezone as Timezone
    from pytz.tzinfo import DstTzInfo

    if dt.tzinfo is None:
        assert origin_timezone_name
        origin_tz = Timezone(origin_timezone_name)
        # this step properly bakes together origin tz and dt
        tz_local_dt = origin_tz.localize(dt)
    elif dt.tzinfo == UTC or isinstance(dt.tzinfo, DstTzInfo):
        # this is an easy way out. These TZs properly react to
        # .normalize() method so no need to do anything with dt
        tz_local_dt = dt
    else:
        # We are here if tzinfo is set on dt,
        # but we don't know what the implementation is
        # (possibly some offset-based thing)
        # and, thus don't trust it to do the right thing
        # Hence, flipping it to UTC-based safe intermediate state
        # which does not have daylight saving time issues.
        tz_local_dt = dt.astimezone(UTC)

    destination_tz = Timezone(destination_timezone_name)

    # this step properly (with account for Daylight saving time) moves
    # dt to other timezone.
    return destination_tz.normalize(tz_local_dt)


def now_in_tz(timezone_name):
    # type: (str) -> datetime
    """
    :param str timezone_name:
    :return" a Timezone-aware datetime instance representing "now" in that tz
    :rtype: datetime
    """
    from datetime import datetime

    return dt_to_other_timezone(datetime.utcnow(), timezone_name)


def now():
    """
    Here only for one reason. To homogenize the derivation of "now" DT
    to be one what is always timezone-aware per UTC

    :rtype: datetime
    """
    return datetime.now(timezone.utc)


def dt_to_timestamp(dt):

    # found on
    # http://stackoverflow.com/questions/2775864/python-datetime-to-unix-timestamp#comment20415069_2775982
    # Does the closest right thing when it comes to daylight saving time

    assert dt.tzinfo is not None  # don't do any guessing. Demand tz-aware dt.

    import calendar

    return calendar.timegm(dt.utctimetuple())


def date_range(dt_start, dt_end):
    """
    Note that we iterate over dates, not datetimes
    With datetimes there is a need to adjust the clock when range crosses over
    Summer Time start / end. Here we don't want to care about that.

    :param date dt_start: Start date of the range. Will be served first.
    :param date dt_end: *Inclusive* end date (must be served last (if different from start))
    :rtype: Generator[date]
    """
    # making end date *exclusive* for ease of looping
    dt_end = dt_end + timedelta(days=1)
    dt = dt_start
    while dt_end > dt:
        yield dt
        dt += timedelta(days=1)


def to_date_string_if_set(v):
    if isinstance(v, (date, datetime)):
        return v.strftime('%Y-%m-%d')
    else:
        return v

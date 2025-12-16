from datetime import datetime, timezone

import pytz

UTC = timezone.utc
EUROPE_BERLIN = pytz.timezone("Europe/Berlin")


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")
    return dt.astimezone(UTC)


def to_berlin(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")
    return dt.astimezone(EUROPE_BERLIN)

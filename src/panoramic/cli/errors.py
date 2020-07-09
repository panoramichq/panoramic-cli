class TimeoutException(Exception):

    """Thrown when a remote operation times out."""


class SourceNotFoundException(Exception):

    """Thrown when a source cannot be found."""


class ScanException(Exception):

    """Generic scanning error."""


class RefreshException(Exception):

    """Generic refresh error."""


class UnexpectedTablesException(Exception):

    """Generic unexpected table error."""


class MissingSchemaException(Exception):

    """Generic missing schema error."""

class TimeoutException(Exception):

    """Thrown when a remote operation times out."""


class SourceNotFoundException(Exception):

    """Thrown when a source cannot be found."""


class ScanException(Exception):

    """Generic scanning error."""


class RefreshException(Exception):

    """Generic refresh error."""


class ParserException(Exception):

    """Generic parser error."""

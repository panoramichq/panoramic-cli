from panoramic.cli.__version__ import __version__


class VersionedClient:

    """Adds version headers to requests session."""

    def __init__(self):
        assert hasattr(self, 'session'), 'Client needs to have requests.Session'
        self.session.headers['User-Agent'] = f'pano-cli/{__version__}'  # type: ignore

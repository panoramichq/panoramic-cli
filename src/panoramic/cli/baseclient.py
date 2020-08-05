from requests.sessions import Session

from panoramic.cli.__version__ import __version__


class VersionedClient:

    session: Session

    def __init__(self):
        self.session.headers['User-Agent'] = f'pano-cli/{__version__}'

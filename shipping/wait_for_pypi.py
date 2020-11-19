#!/usr/bin/env python

"""
Ported from homebrew-pypi-poet project
"""

from __future__ import print_function

import argparse
import codecs
import json
import logging
import os
import sys
import time
import warnings
from contextlib import closing
from hashlib import sha256

import pkg_resources

TIMEOUT_LOOP = 60

try:
    # Python 2.x
    from urllib2 import urlopen
except ImportError:
    # Python 3.x
    from urllib.request import urlopen

# Show warnings and greater by default
logging.basicConfig(level=int(os.environ.get("POET_DEBUG", 30)))


class PackageVersionNotFoundWarning(UserWarning):
    pass


def research_package(name, version=None):
    with closing(urlopen("https://pypi.io/pypi/{}/json".format(name))) as f:
        reader = codecs.getreader("utf-8")
        pkg_data = json.load(reader(f))
    d = {}
    d['name'] = pkg_data['info']['name']
    d['homepage'] = pkg_data['info'].get('home_page', '')
    artefact = None
    if version:
        for pypi_version in pkg_data['releases']:
            if pkg_resources.safe_version(pypi_version) == version:
                for version_artefact in pkg_data['releases'][pypi_version]:
                    if version_artefact['packagetype'] == 'sdist':
                        artefact = version_artefact
                        break
        if artefact is None:
            raise PackageVersionNotFoundWarning(
                "Could not find an exact version match for " "{} version {}".format(name, version)
            )

    if artefact is None:  # no version given or exact match not found
        for url in pkg_data['urls']:
            if url['packagetype'] == 'sdist':
                artefact = url
                break

    if artefact:
        d['url'] = artefact['url']
        if 'digests' in artefact and 'sha256' in artefact['digests']:
            logging.debug("Using provided checksum for %s", name)
            d['checksum'] = artefact['digests']['sha256']
        else:
            logging.debug("Fetching sdist to compute checksum for %s", name)
            with closing(urlopen(artefact['url'])) as f:
                d['checksum'] = sha256(f.read()).hexdigest()
            logging.debug("Done fetching %s", name)
    else:  # no sdist found
        d['url'] = ''
        d['checksum'] = ''
        warnings.warn("No sdist found for %s" % name)
    d['checksum_type'] = 'sha256'
    return d


def wait_for(package, version=None):
    """
    Continue to check PyPI for package version to be published
    """
    i = 0
    data = None
    while i < TIMEOUT_LOOP:
        try:
            data = research_package(package, version)
            break
        except Exception as e:
            print(e)
            time.sleep(2)
        i = i + 1

    if data:
        print(data)
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description='Wait for PyPI to confirm package exists.')
    parser.add_argument('package')
    parser.add_argument('version', nargs='?')
    args = parser.parse_args()

    package = args.package
    version = args.version
    if not package:
        parser.print_usage(sys.stderr)
        return 1

    pkg_data = wait_for(package, version)
    if not pkg_data:
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())

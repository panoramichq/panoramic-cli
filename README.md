# Panoramic CLI

[![Build Status](https://github.com/panoramichq/panoramic-cli/workflows/CI/badge.svg)](https://github.com/panoramichq/panoramic-cli/actions)
[![Last Commit](https://img.shields.io/github/last-commit/panoramichq/panoramic-cli)](https://github.com/panoramichq/panoramic-cli/commits)
[![Latest Release](https://img.shields.io/github/v/release/panoramichq/panoramic-cli)](https://github.com/panoramichq/panoramic-cli/releases)
[![License](https://img.shields.io/pypi/l/panoramic-cli.svg)](https://github.com/panoramichq/panoramic-cli/blob/master/LICENSE)
[![PyPI Download](https://img.shields.io/pypi/pyversions/panoramic-cli.svg)](https://pypi.org/project/panoramic-cli/)

This repository contains the Panoramic Command Line tool. This tool allows you to create & maintain your Panoramic data models. It is built with Python 3.6+ and can be installed via PyPI or other similar tools.

## Installation

To install the CLI, use [`pip`](https://pip.pypa.io/en/stable/quickstart/) or [`pipenv`](https://docs.pipenv.org):

```console
$ pip install -U panoramic-cli
```

## Usage

Once you install the CLI tool, you can call it using `pano` on the command line. For more information, run `pano` with no commands to see the help information:

```
Usage: pano [OPTIONS] COMMAND [ARGS]...

Options:
  --debug     Enables debug mode
  --version   Show the version and exit.
  -h, --help  Show this message and exit.

Commands:
  configure         Configure pano CLI options
  detect-joins      Detect joins under a dataset
  field             Commands on local field files.
  init              Initialize metadata repository
  list-companies    List available companies
  list-connections  List available data connections
  pull              Pull models from remote
  push              Push models to remote
  scan              Scan models from source
  validate          Validate local files
```

## Release process

To release a new version of the library, follow these steps:

- In your PR, update version in [\_\_version\_\_.py](src/panoramic/cli/__version__.py) and add entry to [CHANGELOG.md](CHANGELOG.md)
- After merge, tag the commit with version number from setup.py. For example `git tag v0.1.1`.
- Once the tag is pushed, it will trigger a build with GitHub Actions, which will publish the new version on PyPI and create a release on GitHub.

## Development

### Virtual Environment Using venv

Add python virtual environment using python venv (adds `.venv` inside current directory):

```
> python3 -m venv .venv
```

Then, you can switch to it from command-line using following command:

```
> source .venv/bin/activate
```

### Virtual Environment Using pyenv

Alternatively if you use pyenv and pyenv-virtualenv, you can create virtual environment using:

```
> pyenv virtualenv pano-cli
```

And use the created virtual environment:

```
> pyenv local pano-cli
```

### Build and Run

Use following command to install dependencies (make sure you have correct python environment active):

```
> make install
```

Now you should have `pano` package available. First create config. You need to ask friendly SRE Team Member for OAuth credentials for yourself.

```
> pano configure
```

And finally you are ready to use `pano`. You can find all commands in help:

```
> pano -h
```

## Tests

Use following command to run all tests:

```
> make test
```

Use following command to run all other checks:

```
> make lint
```

## Pre commit hooks

You can install pre-commit. It is useful to avoid commiting code that doesn't pass the linter. It installs git hooks that run pre-commit.

```
> make pre-commit-install
```

## VSCode

You can use following debug config to run pano cli using VSCode debugger:

```
{
    "name": "Python: Pano CLI Scan",
    "type": "python",
    "request": "launch",
    "module": "panoramic.cli",
    "args": ["scan", "testsource"]
}
```

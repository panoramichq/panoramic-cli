# Panoramic CLI

## Release process

To release a new version of the library, follow these steps:

* In your PR, update version in [setup.py](setup.py) and add entry to [CHANGELOG.md](CHANGELOG.md)
* After merge, tag the commit with version number from setup.py. For example `git tag v0.1.1`. You can also do this by creating a new [release](https://github.com/panoramichq/panoctl/releases).
* This triggers a Jenkins pipeline which runs tests, linters and uploads the package to Artifactory

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
> pyenv virtualenv panoctl
```

And use the created virtual environment:

```
> pyenv local panoctl
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

You can install pre-commit. It is useful to avoid commiting code that doesn't pass the linter. Id installs git hooks that run pre-commit.

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

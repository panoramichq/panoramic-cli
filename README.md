# Panoramic CLI

## Release process

To release a new version of the library, follow these steps:

* In your PR, update version in [setup.py](setup.py) and add entry to [CHANGELOG.md](CHANGELOG.md)
* After merge, tag the commit with version number from setup.py. For example `git tag v0.1.1`. You can also do this by creating a new [release](https://github.com/panoramichq/panoramic-auth/releases).
* This triggers a Jenkins pipeline which runs tests, linters and uploads the package to Artifactory

## Development

This repository does not have a dedicated docker image. At the moment, we create python virtual environment using command (in directory `.venv` inside current directory):

```
> python3 -m venv .venv
```

If you use pyenv and pyenv-virtualenv, you can create it using:

```
> pyenv virtualenv panoramic-auth
```

Then, you can switch to it from command-line using following command:

```
> source .venv/bin/activate
```

Or if using pyenv-virtualenv:

```
pyenv local panoramic-auth
```

Lastly, use following command to install dependencies (make sure you have correct python environment active):

```
> make install
```

Install pre-commit - useful to avoid commiting code that doesn't pass the linter:

```
> make pre-commit-install
```

This installs git hooks that run pre-commit.

## Tests

Use following command to run all tests:

```
> make tests
```

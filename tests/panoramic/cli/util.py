import os

from contextlib import contextmanager


@contextmanager
def changedir(new_directory_path: str):
    current_directory_path = os.getcwd()

    os.chdir(new_directory_path)

    try:
        yield
    finally:
        os.chdir(current_directory_path)


@contextmanager
def overwrite_env(name: str, value: str):
    current_value = os.environ.get(name)

    os.environ[name] = value

    try:
        yield
    finally:
        if current_value is not None:
            os.environ[name] = current_value
        else:
            del os.environ[name]

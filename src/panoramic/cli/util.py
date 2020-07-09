import random
import string

from typing import Set

import pydash

from panoramic.cli.errors import UniqueSlugException


def random_alphabet_string(size: int) -> str:
    """
    Returns random ascii alphabet uppercase string of given length
    """
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(size))


def slug_string(input_str: str) -> str:
    """
    Returns lowercase slug variant of given string
    """
    return pydash.slugify(input_str, separator="_").lower()


def generate_unique_slug(input_str: str, existing_slugs: Set[str]) -> str:
    """
    Get unique slug for given input str  within given set
    """
    slug = slug_string(input_str)

    if slug not in existing_slugs:
        return slug

    for _ in range(10):
        candidate = f'{slug}{random_alphabet_string(5)}'.lower()

        if candidate not in existing_slugs:
            return candidate

    raise UniqueSlugException(f'Unable to find unique slug for input string {input_str}')

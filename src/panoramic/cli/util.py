import pydash


def slug_string(input_str: str) -> str:
    """
    Returns lowercase slug variant of given string
    """
    return pydash.slugify(input_str, separator="_").lower()

import re
import unicodedata


def slug_string(input_str: str) -> str:
    """
    Returns lowercase slug variant of given string
    """
    normalized = unicodedata.normalize('NFKD', input_str).encode('ascii', 'ignore').decode('utf8')
    return re.sub('[^a-zA-Z0-9_.]', '', normalized).lower()

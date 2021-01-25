import os

WRITE_EXPECTATIONS = bool(os.getenv('APP_WRITE_EXPECTATIONS', False))
"""Controls whether tests update expectations or not"""

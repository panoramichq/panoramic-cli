class EnumHelper:
    """
    Note: Mypy does not understand extending Enum class
    https://github.com/python/mypy/issues/6037
    Thus using this helper
    """

    @staticmethod
    def list_all_values(enum_class):
        """
        Returns all values on the enum.
        """
        return list(map(lambda c: c.value, enum_class))

    @staticmethod
    def from_value_safe(enum_class, value, default=None):
        """
        Tries to deserialize the value into enum instance. Returns None if fails.
        """
        try:
            return enum_class(value)
        except ValueError:
            return default

    @staticmethod
    def from_value(enum_class, value):
        """
        Tries to deserialize the value into enum instance. Throws ValueError if value is not valid.
        """
        return enum_class(value)

    @staticmethod
    def list_to_value(values):
        """
        Converts list of enum values to list with their actual values
        """
        return [val.value for val in values]

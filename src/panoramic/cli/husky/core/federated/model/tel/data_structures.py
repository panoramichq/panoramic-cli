from typing import List, Set


class AttributeValidationTelVisitorResult:
    def __init__(self):
        self.used_taxon_slugs: Set[str] = set()
        """
        Taxon slugs used during rendering of the transformation
        """

        self.used_column_names: Set[str] = set()
        """
        Column names used during rendering of the transformation
        """

        self._result_expressions: List[str] = []
        """
        List of SQL part of the transaction
        """

    @property
    def result_expression(self) -> str:
        """
        Final expression
        """
        return ''.join(self._result_expressions)

    def reset_result_expression(self):
        """
        Reset the expression
        """
        self._result_expressions = []

    def add_to_expressions(self, exp: str):
        """
        Adds partial expression to the list of expressions
        """
        self._result_expressions.append(exp)

    def append_with(self, other: 'AttributeValidationTelVisitorResult'):
        """
        Appends the other visitor result to the current one
        """

        self._result_expressions.extend(other._result_expressions)
        self.used_taxon_slugs |= other.used_taxon_slugs
        self.used_column_names |= other.used_column_names


class AttributeValidationTelVisitorParams:
    """
    Class holding all parameters for AttributeValidationTelVisitor
    """

    def __init__(self, source_taxon_slug: str, source_model_attributes):
        self.source_taxon_slug: str = source_taxon_slug
        """
        Identification of the source taxon
        (used for detection of cyclic dependencies)
        """

        self.source_model_attributes = source_model_attributes
        """
        List of attributes in the source model
        No type hints on purpose because of cyclic imports
        """

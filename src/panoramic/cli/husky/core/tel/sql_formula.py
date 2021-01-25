from string import Template
from typing import Optional, Set

from panoramic.cli.husky.core.sql_alchemy_util import safe_identifier


class SqlTemplate(Template):
    # Added | and @ to be able to use slug for ns and comparison taxons.
    idpattern = r'([_a-zA-Z][|@:_a-zA-Z0-9]*)'

    def __repr__(self):
        return f"SqlTemplate('''{self.template}''')"

    def __eq__(self, other):
        return self.template == other.templates


class SqlFormulaTemplate:
    """
    Class with sql formula template. Used in cases where at the moment of TEL evaluation we dont know what columns to
    use (because they are found in step later). This class contains sql template, which should be rendered with proper
    columns when building a query.
    """

    template: SqlTemplate
    """
    SQL template, variables in ${xxx} or $xxx format
    """

    label: str
    """
    Temporary column name, to be able to reference the query from post formula
    """

    data_source: Optional[str]
    """
    Name of data source this formula is for.
    """

    used_taxons: Set[str]
    """
    Used taxons that should be rendered in the sql formula template
    """

    def __init__(self, template: SqlTemplate, label: str, data_source: str, used_taxons: Set[str]):
        self.template = template
        self.label = safe_identifier(label)
        self.data_source = data_source
        self.used_taxons = used_taxons

    def render_formula(self, **kwargs):
        return self.template.substitute(**kwargs)

    def __repr__(self):
        return f"SqlFormulaTemplate({repr(self.template)},'''{self.label}''', {repr(self.data_source)},{repr(self.used_taxons)})"

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return all(
                [
                    self.template.template == other.template.template,
                    self.data_source == other.data_source,
                    self.label == other.label,
                    self.used_taxons == other.used_taxons,
                ]
            )
        return False

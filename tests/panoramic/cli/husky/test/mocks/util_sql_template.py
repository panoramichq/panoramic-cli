from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate, SqlTemplate


def create_sql_formula_template_raw_taxon(slug, data_source) -> SqlFormulaTemplate:
    """
    Helper fn to create sql formula template for given slug and ds.
    Useful for calling Single Husky directly, without TelPlanner.
    """
    return SqlFormulaTemplate(SqlTemplate(f'${{{slug}}}'), slug, data_source, {slug})

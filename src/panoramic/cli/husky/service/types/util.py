def get_all_filter_clauses():
    """
    This function is needed to avoid circular dependency. Note that clauses are imported inside the function.

    Also, type annotation for return type is omitted on purpose to avoid importing FilterClause
    """
    from panoramic.cli.husky.service.filter_builder.filter_clauses import (
        GroupFilterClause,
        TaxonArrayFilterClause,
        TaxonTaxonFilterClause,
        TaxonValueFilterClause,
    )

    return [GroupFilterClause, TaxonValueFilterClause, TaxonTaxonFilterClause, TaxonArrayFilterClause]

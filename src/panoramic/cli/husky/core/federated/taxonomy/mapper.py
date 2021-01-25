from panoramic.cli.husky.core.taxonomy.constants import (
    COMPANY_DELIMITER,
    NAMESPACE_DELIMITER,
)


def unmmap_slug(taxon_slug: str) -> str:
    """Remove prefixes from taxon slug."""
    # namespaced taxon
    if NAMESPACE_DELIMITER in taxon_slug:
        vds_slug, taxon_slug = taxon_slug.split(NAMESPACE_DELIMITER, 1)
        return NAMESPACE_DELIMITER.join([vds_slug, taxon_slug])

    # company-scoped taxon
    return taxon_slug.split(COMPANY_DELIMITER, 1)[1]

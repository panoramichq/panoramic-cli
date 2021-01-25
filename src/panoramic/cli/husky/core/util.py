from typing import Iterable, Optional, Set


def get_allowed_company_ids(company_ids: Optional[Iterable[Optional[str]]] = None) -> Set[str]:
    """
    Adds support company id for companies that have acess to global dataset.
    """
    company_ids = company_ids or []
    company_ids_set = {company_id for company_id in company_ids if company_id}

    return company_ids_set

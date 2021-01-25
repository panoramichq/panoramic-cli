from panoramic.cli.husky.core.federated.model.exceptions import WrongVirtualDataSource
from panoramic.cli.husky.core.taxonomy.constants import NAMESPACE_DELIMITER


def prefix_with_virtual_data_source(virtual_data_source: str, attr: str) -> str:
    """
    Adds virtual data source prefix to an attribute
    """
    if NAMESPACE_DELIMITER in attr:
        raise WrongVirtualDataSource(
            virtual_data_source, attr, 'The attribute already contains identification of virtual data source'
        )

    return virtual_data_source + NAMESPACE_DELIMITER + attr


def remove_virtual_data_source_prefix(virtual_data_source: str, attr: str) -> str:
    """
    Removes virtual data source prefix from an attribute
    """
    if not attr.startswith(virtual_data_source + NAMESPACE_DELIMITER):
        raise WrongVirtualDataSource(
            virtual_data_source, attr, 'The attribute is missing identification of virtual data source'
        )

    return attr[len(virtual_data_source + NAMESPACE_DELIMITER) :]

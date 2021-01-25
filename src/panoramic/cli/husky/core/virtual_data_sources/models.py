from panoramic.cli.husky.core.pydantic.model import PydanticModel


class VirtualDataSource(PydanticModel):
    """Representation of virtual data source"""

    slug: str
    display_name: str

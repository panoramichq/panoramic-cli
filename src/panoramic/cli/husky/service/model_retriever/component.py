import logging
from typing import List, Optional, Set

from panoramic.cli.husky.core.model.enums import ModelVisibility
from panoramic.cli.husky.core.model.models import HuskyModel
from panoramic.cli.husky.core.virtual_state.mappers import VirtualStateMapper
from panoramic.cli.husky.service.model_retriever.model_augments import ModelAugments
from panoramic.cli.husky.service.types.api_scope_types import Scope
from panoramic.cli.husky.service.utils.exceptions import ModelNotFoundException
from panoramic.cli.local import get_state

logger = logging.getLogger(__name__)

_VISIBILITY_MAP = visibility_map = {
    ModelVisibility.available: {ModelVisibility.available},
    ModelVisibility.experimental: {ModelVisibility.available, ModelVisibility.experimental},
}


def can_scope_see_model(scope: Scope, model: HuskyModel) -> bool:
    return model.visibility in _VISIBILITY_MAP.get(scope.model_visibility, set())


def does_scope_belong_to_company_project(scope: Scope, company_id: Optional[str], project_id: Optional[str]) -> bool:
    return company_id == scope.company_id and (project_id is None or project_id == scope.project_id)


def does_model_belong_to_scope(model: HuskyModel, scope: Scope) -> bool:
    return can_scope_see_model(scope, model) and does_scope_belong_to_company_project(
        scope, model.company_id, model.project_id
    )


class ModelRetriever:
    @staticmethod
    def _filter_physical_data_sources(
        models: List[HuskyModel], allowed_physical_data_sources: Set[str]
    ) -> List[HuskyModel]:
        return [model for model in models if model.physical_data_source in allowed_physical_data_sources]

    @classmethod
    def _load_all_models(cls) -> List[HuskyModel]:
        """
        Loads all available husky models from internal state
        """
        # get virtual state
        state = get_state()
        # map it to internal state
        internal_state = VirtualStateMapper.to_husky(state)

        return internal_state.models

    @classmethod
    def _load_augmented_models(cls) -> List[HuskyModel]:
        """
        Loads all available husky models from internal state
        """
        # load all models
        all_models = cls._load_all_models()

        # augment each model
        for model in all_models:
            ModelAugments.augment_model(model)

        return all_models

    @classmethod
    def load_models(
        cls,
        data_sources: Set[str],
        scope: Scope,
        specific_model_name: Optional[str] = None,
        allowed_physical_data_sources: Optional[Set[str]] = None,
    ) -> List[HuskyModel]:
        """
        Loads all models that include all data_sources and match the company/project combination from the Scope object.
        Company available models are returned within the company. If specific_model_name is given
        it ignores data sources and tries to match a model by Scope and model name.
        """
        # get all models
        models = cls._load_augmented_models()

        # filter only models for allowed physical data sources
        if allowed_physical_data_sources is None:
            relevant_models = models
        else:
            relevant_models = cls._filter_physical_data_sources(models, allowed_physical_data_sources)

        # create iterator of models belonging to the scope
        scoped_model_iterator = (model for model in relevant_models if does_model_belong_to_scope(model, scope))

        if specific_model_name:
            # now, if we want a specific model (by name), try to find it
            selected_models = [model for model in scoped_model_iterator if model.name == specific_model_name]
        else:
            # otherwise, filter models based on scope
            selected_models = [
                model for model in scoped_model_iterator if data_sources.issubset(set(model.data_sources))
            ]

        if len(selected_models) == 0:
            raise ModelNotFoundException(
                {
                    'data_sources': data_sources,
                    'specific_model_name': specific_model_name,
                    'allowed_physical_data_sources': allowed_physical_data_sources,
                }
            )

        return selected_models

    @classmethod
    def load_models_by_taxons(
        cls, taxon_slugs: Set[str], data_sources: Set[str], scope: Scope, specific_model_name: Optional[str] = None
    ) -> List[HuskyModel]:
        """
        Loads models that contain all of the specified taxons.
        -> requested taxon slugs *is subset* of model.taxons
        """

        models = cls.load_models(data_sources, scope, specific_model_name)
        return [model for model in models if taxon_slugs.issubset(model.taxons)]

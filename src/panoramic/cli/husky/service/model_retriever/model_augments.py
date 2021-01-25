from typing import Dict, List

from panoramic.cli.husky.core.federated.utils import prefix_with_virtual_data_source
from panoramic.cli.husky.core.model.enums import TimeGranularity
from panoramic.cli.husky.core.model.models import (
    AttributeNotFound,
    HuskyModel,
    ModelAttribute,
)
from panoramic.cli.husky.service.constants import TaxonSlugs


class ModelAugments:
    """
    Helper class for Model Retriever to augment the found models with required
    attributes and possibly other properties.
    """

    _model_attributes_day = [
        dict(
            taxon=TaxonSlugs.MONTH,
            tel_transformation='date_month({taxon_slug})',
        ),
        dict(
            taxon=TaxonSlugs.WEEK,
            tel_transformation='date_week({taxon_slug})',
        ),
        dict(taxon=TaxonSlugs.MONTH_OF_YEAR, tel_transformation='month_of_year({taxon_slug})'),
        dict(
            taxon=TaxonSlugs.WEEK_OF_YEAR,
            tel_transformation='week_of_year({taxon_slug})',
        ),
    ]

    # When deriving hourly taxons from date taxon.. which is not preferred long term to the future..
    _model_attributes_hour_from_date_taxon = _model_attributes_day + [
        dict(
            taxon=TaxonSlugs.DATE_HOUR,
            tel_transformation='date_hour({taxon_slug})',
        ),
        dict(
            taxon=TaxonSlugs.HOUR_OF_DAY,
            tel_transformation='hour_of_day({taxon_slug})',
        ),
    ]

    # Instead, hourly models should have date_hour taxon
    _model_atrributes_hour_from_date_hour_taxon = _model_attributes_day + [
        dict(
            taxon=TaxonSlugs.DATE,
            tel_transformation='to_date({taxon_slug}, \'YYYY-MM-DD\')',
        ),
        dict(
            taxon=TaxonSlugs.HOUR_OF_DAY,
            tel_transformation='hour_of_day({taxon_slug})',
        ),
    ]

    @classmethod
    def _prepare_derived_time_attributes(cls, taxon_slug: str, time_attrs_def: List[Dict]) -> List[ModelAttribute]:
        """
        Creates list of new time attributes derived from sql accessor.
        """
        return [
            ModelAttribute(
                dict(
                    identifier=False,
                    taxon=attr_def['taxon'],
                    tel_transformation=attr_def['tel_transformation'].format(taxon_slug=taxon_slug),
                )
            )
            for attr_def in time_attrs_def
        ]

    @classmethod
    def _model_add_time_attributes(cls, model: HuskyModel):
        try:
            time_attrs: List[ModelAttribute] = []
            date_taxon_slug = prefix_with_virtual_data_source(model.data_source, TaxonSlugs.DATE)
            date_hour_taxon_slug = prefix_with_virtual_data_source(model.data_source, TaxonSlugs.DATE_HOUR)

            if (
                model.time_granularity
                and model.time_granularity is TimeGranularity.day
                and model.has_taxon(date_taxon_slug)
            ):
                time_attrs = ModelAugments._prepare_derived_time_attributes(
                    TaxonSlugs.DATE, ModelAugments._model_attributes_day
                )
            elif (
                model.time_granularity
                and model.time_granularity == TimeGranularity.hour
                and model.has_taxon(date_hour_taxon_slug)
            ):
                # Hourly models should always have date_hour attribute. This is correct.
                time_attrs = ModelAugments._prepare_derived_time_attributes(
                    TaxonSlugs.DATE_HOUR,
                    ModelAugments._model_atrributes_hour_from_date_hour_taxon,
                )

            for time_attr in time_attrs:
                model.add_attribute(time_attr)
        except AttributeNotFound:
            pass

    @classmethod
    def _model_add_data_source_attributes(cls, model: HuskyModel):
        if (
            TaxonSlugs.DATA_SOURCE.upper() not in model.taxons  # try to find the date_source taxon
            and len(model.data_sources) == 1  # make sure that we know the correct data source name
        ):
            # we have one known data source for this model, so let's add it as attribute to the model
            # this way, we can query it
            model.add_attribute(
                ModelAttribute(
                    {
                        'column_name': None,
                        'identifier': False,
                        'taxon': TaxonSlugs.DATA_SOURCE,
                        'tel_transformation': f"'{model.data_sources[0]}'",
                    }
                )
            )

    @classmethod
    def _model_add_model_info_attributes(cls, model: HuskyModel):
        taxons_map = model.attributes_by_taxon_memoized

        # if the model is company-scoped and no company_id taxon is provided on the model, augment it as a constant
        if TaxonSlugs.COMPANY_ID not in taxons_map:
            model.add_attribute(
                ModelAttribute(
                    {
                        'column_name': None,
                        'identifier': False,
                        'taxon': TaxonSlugs.COMPANY_ID,
                        'tel_transformation': f"'{model.company_id}'",
                    }
                )
            )

        # if the model is project-scoped and no project_id taxon is provided on the model, augment it as a constant
        if model.project_id is not None and TaxonSlugs.PROJECT_ID not in taxons_map:
            model.add_attribute(
                ModelAttribute(
                    {
                        'column_name': None,
                        'identifier': False,
                        'taxon': TaxonSlugs.PROJECT_ID,
                        'tel_transformation': f"'{model.project_id}'",
                    }
                )
            )

    @classmethod
    def augment_model(cls, model: HuskyModel):
        """
        Augment model with
        - data_source attribute
        - company_id/project_id
        - time attributes (eg. QUARTER, WEEK_OF_YEAR)
        """
        ModelAugments._model_add_data_source_attributes(model)
        ModelAugments._model_add_model_info_attributes(model)
        ModelAugments._model_add_time_attributes(model)

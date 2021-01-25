import datetime
from typing import Optional

from panoramic.cli.husky.core.model.enums import ModelVisibility, TimeGranularity
from panoramic.cli.husky.core.model.models import HuskyModel, ModelJoin
from panoramic.cli.husky.service.model_retriever.model_augments import ModelAugments

MOCK_DATA_SOURCE_NAME = 'mock_data_source'
MOCK_DATA_SOURCE_NAME2 = 'mock_data_source2'


def get_mock_husky_model() -> HuskyModel:
    return HuskyModel(
        dict(
            name='test_model',
            company_id='company_id',
            fully_qualified_name_parts=['database_a', 'company_a', 'table_a'],
            attributes=dict(
                ad_id=dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
                gender=dict(tel_transformation='"gender"', taxon='gender', identifier=False),
            ),
            visibility=ModelVisibility.available,
            data_sources=[MOCK_DATA_SOURCE_NAME],
        )
    )


def get_mock_entity_model() -> HuskyModel:
    namespaced_taxon_dimension = f'{MOCK_DATA_SOURCE_NAME}|dimension'
    namespaced_taxon_metric = f'{MOCK_DATA_SOURCE_NAME}|metric'
    return HuskyModel(
        dict(
            name='mock_data_source.entity_model',
            company_id='company_id',
            fully_qualified_name_parts=['db', 'schema', 'entity_table'],
            attributes={
                'ad_id': dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
                'ad_name': dict(
                    taxon='ad_name',
                    identifier=False,
                    tel_transformation='"ad_name"',
                ),
                'account_id': dict(tel_transformation='"account_id"', taxon='account_id', identifier=False),
                namespaced_taxon_dimension: dict(
                    tel_transformation=f'"{namespaced_taxon_dimension}"',
                    taxon=namespaced_taxon_dimension,
                    identifier=False,
                ),
                namespaced_taxon_metric: dict(
                    tel_transformation=f'"{namespaced_taxon_metric}"', taxon=namespaced_taxon_metric, identifier=False
                ),
            },
            data_sources=[MOCK_DATA_SOURCE_NAME],
            visibility=ModelVisibility.available,
        )
    )


def get_mock_entity_model_one_to_many_reverse_join() -> HuskyModel:
    return HuskyModel(
        dict(
            name='mock_data_source.entity_model_reverse_many_to_one',
            company_id='company_id',
            fully_qualified_name_parts=['db', 'schema', 'entity_reverse_many_to_one_table'],
            attributes=dict(
                ad_id=dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
                ad_name=dict(tel_transformation='"ad_name"', taxon='ad_name', identifier=False),
                account_id=dict(tel_transformation='"account_id"', taxon='account_id', identifier=False),
            ),
            data_sources=[MOCK_DATA_SOURCE_NAME],
            joins=[
                ModelJoin(
                    dict(
                        join_type='right',
                        to_model='mock_data_source.metric_model',
                        taxons=['ad_id'],
                        relationship='one_to_many',
                    )
                ).to_native()
            ],
        )
    )


def get_mock_entity_model_one_to_one_reverse_join() -> HuskyModel:
    return HuskyModel(
        dict(
            name='mock_data_source.entity_model_reverse_one_to_one',
            company_id='company_id',
            fully_qualified_name_parts=['db', 'schema', 'entity_reverse_one_to_one_table'],
            attributes=dict(
                ad_id=dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
                ad_name=dict(tel_transformation='"ad_name"', taxon='ad_name', identifier=False),
                account_id=dict(tel_transformation='"account_id"', taxon='account_id', identifier=False),
            ),
            data_sources=[MOCK_DATA_SOURCE_NAME],
            joins=[
                ModelJoin(
                    dict(
                        join_type='right',
                        to_model='mock_data_source.metric_model',
                        taxons=['ad_id'],
                        relationship='one_to_one',
                    )
                ).to_native()
            ],
        )
    )


def get_mock_entity_model_one_to_one_reverse_join_outgoing_direction() -> HuskyModel:
    return HuskyModel(
        dict(
            name='mock_data_source.entity_model_reverse_one_to_one_outgoing_direction',
            company_id='company_id',
            fully_qualified_name_parts=['db', 'schema', 'entity_reverse_one_to_one_table'],
            attributes=dict(
                ad_id=dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
                ad_name=dict(tel_transformation='"ad_name"', taxon='ad_name', identifier=False),
                account_id=dict(tel_transformation='"account_id"', taxon='account_id', identifier=False),
            ),
            data_sources=[MOCK_DATA_SOURCE_NAME],
            joins=[
                ModelJoin(
                    dict(
                        join_type='right',
                        to_model='mock_data_source.metric_model',
                        taxons=['ad_id'],
                        relationship='one_to_one',
                        direction='outgoing',
                    )
                ).to_native()
            ],
        )
    )


def get_mock_metric_model(
    company_id: Optional[str] = 'company', created_at: Optional[datetime.datetime] = None
) -> HuskyModel:
    return HuskyModel(
        dict(
            name='mock_data_source.metric_model',
            fully_qualified_name_parts=['db', 'schema', 'metric_table_hourly'],
            company_id=company_id,
            created_at=created_at,
            attributes=dict(
                ad_id=dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
                account_id=dict(tel_transformation='"account_id"', taxon='account_id', identifier=False),
                range_start=dict(tel_transformation='"start_time"', taxon='range_start', identifier=True),
                objective=dict(tel_transformation='"objective"', taxon='objective', identifier=False),
                impressions=dict(tel_transformation='"impressions"', taxon='impressions', identifier=False),
                spend=dict(tel_transformation='"spend"', taxon='spend', identifier=False),
                simple_count_all=dict(tel_transformation='"ad_id"', taxon='simple_count_all', identifier=False),
                simple_count_distinct=dict(
                    tel_transformation='"ad_id"', taxon='simple_count_distinct', identifier=False
                ),
                simple_min=dict(tel_transformation='"ad_id"', taxon='simple_min', identifier=False),
                simple_max=dict(tel_transformation='"ad_id"', taxon='simple_max', identifier=False),
                simple_first_by=dict(tel_transformation='"ad_id"', taxon='simple_first_by', identifier=False),
                simple_last_by=dict(tel_transformation='"ad_id"', taxon='simple_last_by', identifier=False),
            ),
            data_sources=[MOCK_DATA_SOURCE_NAME],
            joins=[
                ModelJoin(
                    dict(
                        join_type='left',
                        to_model='mock_data_source.entity_model',
                        taxons=['ad_id'],
                        relationship='many_to_one',
                    )
                ).to_native()
            ],
            visibility=ModelVisibility.available,
        )
    )


def get_mock_metric_time_taxon_model() -> HuskyModel:
    model = HuskyModel(
        dict(
            name='mock_data_source.metric_time_model',
            company_id='company_id',
            fully_qualified_name_parts=['db', 'schema', 'metric_table_hourly'],
            attributes={
                'ad_id': dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
                'account_id': dict(tel_transformation='"account_id"', taxon='account_id', identifier=False),
                'range_start': dict(tel_transformation='"start_time"', taxon='range_start', identifier=True),
                'impressions': dict(tel_transformation='"impressions"', taxon='impressions', identifier=False),
                'spend': dict(tel_transformation='"spend"', taxon='spend', identifier=False),
                'mock_data_source|date': dict(
                    tel_transformation='"date"', taxon='mock_data_source|date', identifier=True
                ),
            },
            time_granularity=TimeGranularity.day,
            data_sources=[MOCK_DATA_SOURCE_NAME],
            joins=[
                ModelJoin(
                    dict(
                        join_type='left',
                        to_model='mock_data_source.entity_model',
                        taxons=['ad_id'],
                        relationship='many_to_one',
                    )
                ).to_native()
            ],
            visibility=ModelVisibility.available,
        )
    )
    ModelAugments.augment_model(model)
    return model


def get_mock_metric_gender_model() -> HuskyModel:
    return HuskyModel(
        dict(
            name='mock_data_source.metric_gender_model',
            company_id='company_id',
            fully_qualified_name_parts=['db', 'schema', 'metric_gender_table_hourly'],
            attributes=dict(
                ad_id=dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
                account_id=dict(tel_transformation='"account_id"', taxon='account_id', identifier=False),
                range_start=dict(tel_transformation='"start_time"', taxon='range_start', identifier=True),
                impressions=dict(tel_transformation='"impressions"', taxon='impressions', identifier=False),
                spend=dict(tel_transformation='"spend"', taxon='spend', identifier=False),
                gender=dict(tel_transformation='"gender"', taxon='gender', identifier=True),
            ),
            data_sources=[MOCK_DATA_SOURCE_NAME],
            joins=[
                ModelJoin(
                    dict(
                        join_type='left',
                        to_model='mock_data_source.entity_model',
                        taxons=['ad_id'],
                        relationship='many_to_one',
                    )
                ).to_native()
            ],
            visibility=ModelVisibility.available,
        )
    )


def get_mock_cross_datasource_metric_model() -> HuskyModel:
    return HuskyModel(
        dict(
            name='mock_data_source.cross_metric_table_hourly',
            company_id='company_id',
            fully_qualified_name_parts=['db', 'schema', 'cross_metric_table_hourly'],
            attributes=dict(
                ad_id=dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
                account_id=dict(tel_transformation='"account_id"', taxon='account_id', identifier=False),
                data_source=dict(tel_transformation='"data_source"', taxon='data_source', identifier=False),
                range_start=dict(tel_transformation='"start_time"', taxon='range_start', identifier=True),
                spend=dict(tel_transformation='"spend"', taxon='spend', identifier=False),
                gender=dict(tel_transformation='"gender"', taxon='gender', identifier=True),
                objective=dict(tel_transformation='"objective"', taxon='objective', identifier=True),
                age=dict(tel_transformation='"age"', taxon='age', identifier=True),
            ),
            data_sources=[MOCK_DATA_SOURCE_NAME],
            visibility=ModelVisibility.available,
        )
    )


def get_mock_benchmark_metric_model() -> HuskyModel:
    return HuskyModel(
        dict(
            name='mock_data_source.project_benchmark',
            company_id='company_id',
            fully_qualified_name_parts=['db', 'schema', 'project_benchmark'],
            attributes=dict(
                data_source=dict(tel_transformation='"data_source"', taxon='data_source', identifier=False),
                project_id=dict(tel_transformation='"project_id"', taxon='project_id', identifier=True),
                company_id=dict(tel_transformation='"company_id"', taxon='company_id', identifier=True),
                spend=dict(tel_transformation='"spend"', taxon='spend', identifier=False),
            ),
            data_sources=[MOCK_DATA_SOURCE_NAME],
            visibility=ModelVisibility.available,
        )
    )


def generate_husky_mock_model(**kwargs) -> HuskyModel:
    fully_qualified_name_parts = [
        kwargs.get('database_name', 'database_a'),
        kwargs.get('schema_name', 'schema_name'),
        kwargs.get('object_name', 'table_a_day'),
    ]
    for key in ['database_name', 'schema_name', 'object_name']:
        if key in kwargs:
            del kwargs[key]

    base = dict(
        name='test_model',
        company_id='company_id',
        fully_qualified_name_parts=fully_qualified_name_parts,
        attributes=dict(
            ad_id=dict(tel_transformation='"ad_id"', taxon='ad_id', identifier=True),
            gender=dict(tel_transformation='"gender"', taxon='gender', identifier=False),
            date=dict(tel_transformation='"date_column"', taxon='date', identifier=True),
            date_hour=dict(tel_transformation='"date_hour_column"', taxon='date_hour', identifier=True),
        ),
        data_sources=[MOCK_DATA_SOURCE_NAME],
    )
    base.update(kwargs)

    return HuskyModel(base)


def get_mock_physical_data_sources_model() -> HuskyModel:
    return HuskyModel(
        dict(
            name=f'{MOCK_DATA_SOURCE_NAME}.mock_schema.mock_table',
            company_id='company_id',
            attributes=dict(
                date=dict(tel_transformation='"date"', taxon='date', identifier=False),
                cpm=dict(tel_transformation='"cpm"', taxon='cpm', identifier=False),
                account_id=dict(tel_transformation='"account_id"', taxon='account_id', identifier=False),
                company_id=dict(tel_transformation='"company_id"', taxon='company_id', identifier=False),
                spend=dict(tel_transformation='"spend"', taxon='spend', identifier=False),
                impressions=dict(tel_transformation='"impressions"', taxon='impressions', identifier=False),
            ),
            data_sources=[MOCK_DATA_SOURCE_NAME],
            fully_qualified_name_parts=[MOCK_DATA_SOURCE_NAME, 'mock_schema', 'mock_table'],
            visibility=ModelVisibility.available,
        )
    )

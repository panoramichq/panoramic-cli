import logging

from panoramic.cli.husky.core.federated.model.mappers import FdqModelMapper
from panoramic.cli.husky.federated.join_detection.detect import detect_joins
from panoramic.cli.husky.service.model_retriever.component import ModelRetriever
from panoramic.cli.husky.service.types.api_scope_types import Scope

logger = logging.getLogger(__name__)


def detect_joins_task(detect_joins_job):
    try:
        logger.info(
            f'job_id={detect_joins_job.job_id} Fetching models for vds {detect_joins_job.virtual_data_source} '
            f'under company {detect_joins_job.company_id}'
        )
        husky_models = ModelRetriever.load_models(
            {detect_joins_job.virtual_data_source}, Scope(company_id=detect_joins_job.company_id)
        )
        models = [FdqModelMapper.from_internal(husky_model) for husky_model in husky_models]

        logger.info(
            f'job_id={detect_joins_job.job_id} Running join detection for {detect_joins_job.virtual_data_source} '
            f'under company {detect_joins_job.company_id}'
        )

        detected_joins = detect_joins(models=models)
        detect_joins_job.joins = detected_joins
        detect_joins_job.status = 'COMPLETED'
        logger.info(
            f'Joins for {detect_joins_job.virtual_data_source} '
            f'under company {detect_joins_job.company_id} detected sucessfully job_id={detect_joins_job.job_id} '
        )
    except Exception:
        detect_joins_job.status = 'FAILED'
        logger.error(
            f'Failed detecting joins for {detect_joins_job.virtual_data_source} '
            f'under company {detect_joins_job.company_id} job_id={detect_joins_job.job_id} '
        )
        raise  # Let the celery handler report the failure

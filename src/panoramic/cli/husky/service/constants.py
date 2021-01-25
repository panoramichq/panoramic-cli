from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr


class TaxonSlugs:
    ACCOUNT_ID: TaxonExpressionStr = TaxonExpressionStr('account_id')
    COMPANY_ID: TaxonExpressionStr = TaxonExpressionStr('company_id')
    PROJECT_ID: TaxonExpressionStr = TaxonExpressionStr('project_id')

    DATA_SOURCE: TaxonExpressionStr = TaxonExpressionStr('data_source')

    DATE_HOUR: TaxonExpressionStr = TaxonExpressionStr('date_hour')
    DATE: TaxonExpressionStr = TaxonExpressionStr('date')
    MONTH: TaxonExpressionStr = TaxonExpressionStr('month')
    WEEK: TaxonExpressionStr = TaxonExpressionStr('week')
    MONTH_OF_YEAR: TaxonExpressionStr = TaxonExpressionStr('month_of_year')
    WEEK_OF_YEAR: TaxonExpressionStr = TaxonExpressionStr('week_of_year')
    HOUR_OF_DAY: TaxonExpressionStr = TaxonExpressionStr('hour_of_day')


HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME = '__data_source'
"""
Column on final Single Husky query with respective data source.
"""

from enum import Enum


class ComponentType(Enum):
    UNKNOWN = 'unknown'
    API = 'api'
    HUSKY_API = 'husky.api'
    TAXONOMY_API = 'taxonomy.api'
    FEDERATED = 'federated'


class ExceptionGroup(Enum):
    APP = 'app'
    API = 'api'
    API_SERVER_ERROR = 'api_server_error'
    COMMON = 'common'
    MODEL = 'model'
    UNSUPPORTED = 'unsupported'
    WRONG_FORMAT = 'wrong_format'
    TAXONS = 'taxons'
    FDQ_MODEL_ERROR = 'fdq.model.error'

    FDQ_IDENTIFIERS = 'fdq.identifiers'
    FDQ_TAXONOMY = 'fdq.taxonomy'
    FDQ_TRANSFORM = 'fdq.transform'


class ExceptionSeverity(Enum):
    info = 'info'
    warning = 'warning'
    error = 'error'


class ExceptionErrorCode(Enum):
    """
    Enum with all API error codes
    """

    UNKNOWN_OPERATOR = 'unknown-operator'
    UNKNOWN = 'unknown'
    REQUEST_VALIDATION = 'request-validation'
    TAXON_ISSUE = 'taxon-issue'
    MISSING_TAXON_JOIN = 'missing-taxon-join'
    MULTIPLE_DATA_SOURCES_GRAPH = 'multiple-sources-graph-builder'
    NO_MODEL_FOUND = 'no-model-found'
    IMPOSSIBLE_TAXON_COMBINATION = 'impossible-taxon-combination'
    INVALID_TEL_EXPRESSION = 'invalid-tel-expression'
    FORBIDDEN_ACCESS = 'forbidden-access'
    ROUTE_NOT_FOUND = 'not-found'
    ERROR_PARSING_COMPARISON_RULES = 'problem-parsing-comparison-rules'
    INVALID_MODEL_TEL_EXPRESSION = 'invalid-model-tel-expression'
    CYCLIC_REFERENCE_MODEL_ATTR_TEL = 'cyclic-reference-model-attr-tel'
    FDQ_FLAG_REQUIRED = 'fdq-flag-required'
    INVALID_REQUEST_PAYLOAD = 'invalid-request-payload'
    TOO_MANY_PHYSICAL_DATA_SOURCES = 'too-many-physical-data-sources'
    UNEXPECTED_TAXONS = 'unexpected-taxons'
    INACCESSIBLE_TAXON = 'inaccessible-taxon'
    OVERRIDE_NOT_FOUND = 'override-mapping-not-found'
    OVERRIDE_GENERIC = 'override-mapping-generic-error'
    OVERRIDE_TOO_MANY = 'override-mapping-too-many-mappings'
    WRONG_VIRTUAL_DATA_SOURCE_ATTR = 'wrong-virtual-data-source-attr'
    FDQ_JOIN_DETECTION_JOB_NOT_FOUND = 'fdq-join-detection-job-not-found'
    FDQ_UNSUPPORTED_DIALECT = 'fdq-transform-unsupported-dialect'
    UNKNOWN_PHYSICAL_DATA_SOURCE = 'unknown-physical-data-source'

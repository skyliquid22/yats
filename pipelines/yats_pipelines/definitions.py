from dagster import Definitions

from yats_pipelines.jobs.canonicalize import canonicalize
from yats_pipelines.jobs.feature_pipeline import feature_pipeline
from yats_pipelines.jobs.ingest_alpaca import ingest_alpaca
from yats_pipelines.jobs.ingest_financialdatasets import ingest_financialdatasets

defs = Definitions(
    jobs=[ingest_alpaca, ingest_financialdatasets, canonicalize, feature_pipeline],
)

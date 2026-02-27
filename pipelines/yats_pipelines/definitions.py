from dagster import Definitions

from yats_pipelines.jobs.ingest_alpaca import ingest_alpaca

defs = Definitions(
    jobs=[ingest_alpaca],
)

from dagster import Definitions

from yats_pipelines.jobs.canonicalize import canonicalize
from yats_pipelines.jobs.experiment_run import experiment_run
from yats_pipelines.jobs.experiment_sweep import experiment_sweep
from yats_pipelines.jobs.feature_pipeline import feature_pipeline
from yats_pipelines.jobs.feature_pipeline_incremental import feature_pipeline_incremental
from yats_pipelines.jobs.ingest_alpaca import ingest_alpaca
from yats_pipelines.jobs.ingest_financialdatasets import ingest_financialdatasets
from yats_pipelines.jobs.promote import promote_job
from yats_pipelines.jobs.qualify import qualify
from yats_pipelines.jobs.shadow_run import shadow_run
from yats_pipelines.jobs.stream_canonical import stream_canonical

defs = Definitions(
    jobs=[
        ingest_alpaca,
        ingest_financialdatasets,
        canonicalize,
        feature_pipeline,
        feature_pipeline_incremental,
        experiment_run,
        experiment_sweep,
        shadow_run,
        qualify,
        promote_job,
        stream_canonical,
    ],
)

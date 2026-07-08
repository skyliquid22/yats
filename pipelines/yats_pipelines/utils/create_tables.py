"""QuestDB table DDL — creates all YATS tables via PG wire.

Idempotent: uses CREATE TABLE IF NOT EXISTS throughout.
Run directly: python -m yats_pipelines.utils.create_tables
"""

import psycopg2

from yats_pipelines.resources.questdb import QuestDBResource

# ---------------------------------------------------------------------------
# Raw tables (per-vendor, append-only)
# ---------------------------------------------------------------------------

RAW_ALPACA_EQUITY_OHLCV = """
CREATE TABLE IF NOT EXISTS raw_alpaca_equity_ohlcv (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    vwap DOUBLE,
    trade_count LONG,
    ingested_at TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

RAW_FD_FUNDAMENTALS = """
CREATE TABLE IF NOT EXISTS raw_fd_fundamentals (
    report_date TIMESTAMP,
    symbol SYMBOL,
    fiscal_period SYMBOL,
    period SYMBOL,
    currency SYMBOL,
    revenue DOUBLE,
    cost_of_revenue DOUBLE,
    gross_profit DOUBLE,
    operating_expense DOUBLE,
    operating_income DOUBLE,
    interest_expense DOUBLE,
    ebit DOUBLE,
    net_income DOUBLE,
    net_income_common_stock DOUBLE,
    eps DOUBLE,
    eps_diluted DOUBLE,
    weighted_average_shares DOUBLE,
    weighted_average_shares_diluted DOUBLE,
    dividends_per_share DOUBLE,
    ingested_at TIMESTAMP
) TIMESTAMP(report_date) PARTITION BY YEAR;
"""

RAW_FD_FINANCIAL_METRICS = """
CREATE TABLE IF NOT EXISTS raw_fd_financial_metrics (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    market_cap DOUBLE,
    pe_ratio DOUBLE,
    ps_ratio DOUBLE,
    pb_ratio DOUBLE,
    ev_ebitda DOUBLE,
    roe DOUBLE,
    roa DOUBLE,
    gross_margin DOUBLE,
    operating_margin DOUBLE,
    net_margin DOUBLE,
    fcf_margin DOUBLE,
    debt_to_equity DOUBLE,
    current_ratio DOUBLE,
    revenue_growth_yoy DOUBLE,
    eps_growth_yoy DOUBLE,
    dividend_yield DOUBLE,
    shares_outstanding DOUBLE,
    ingested_at TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY YEAR;
"""

RAW_FD_INSIDER_TRADES = """
CREATE TABLE IF NOT EXISTS raw_fd_insider_trades (
    filed_at TIMESTAMP,
    symbol SYMBOL,
    insider_name STRING,
    insider_title STRING,
    transaction_type SYMBOL,
    shares DOUBLE,
    price_per_share DOUBLE,
    total_value DOUBLE,
    shares_owned_after DOUBLE,
    ingested_at TIMESTAMP
) TIMESTAMP(filed_at) PARTITION BY YEAR;
"""

RAW_FD_ANALYST_ESTIMATES = """
CREATE TABLE IF NOT EXISTS raw_fd_analyst_estimates (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    period SYMBOL,
    eps_estimate_mean DOUBLE,
    eps_estimate_high DOUBLE,
    eps_estimate_low DOUBLE,
    eps_actual DOUBLE,
    revenue_estimate_mean DOUBLE,
    revenue_actual DOUBLE,
    num_analysts LONG,
    ingested_at TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY YEAR;
"""

RAW_FD_INSTITUTIONAL_HOLDINGS = """
CREATE TABLE IF NOT EXISTS raw_fd_institutional_holdings (
    filing_date TIMESTAMP,
    symbol SYMBOL,
    report_period TIMESTAMP,
    accession_number STRING,
    filer_cik SYMBOL,
    filer_name STRING,
    cusip SYMBOL,
    title_of_class SYMBOL,
    shares DOUBLE,
    value_usd DOUBLE,
    reported_price DOUBLE,
    ingested_at TIMESTAMP,
    dagster_run_id STRING
) TIMESTAMP(filing_date) PARTITION BY YEAR WAL;
"""

RAW_FD_EARNINGS = """
CREATE TABLE IF NOT EXISTS raw_fd_earnings (
    report_date TIMESTAMP,
    symbol SYMBOL,
    fiscal_period SYMBOL,
    eps_actual DOUBLE,
    eps_estimate DOUBLE,
    eps_surprise DOUBLE,
    eps_surprise_pct DOUBLE,
    revenue_actual DOUBLE,
    revenue_estimate DOUBLE,
    ingested_at TIMESTAMP
) TIMESTAMP(report_date) PARTITION BY YEAR;
"""

RAW_THETADATA_OPTIONS_CHAIN = """
CREATE TABLE IF NOT EXISTS raw_thetadata_options_chain (
    quote_ts TIMESTAMP,
    underlying SYMBOL,
    expiry TIMESTAMP,
    strike DOUBLE,
    right SYMBOL,
    bid DOUBLE,
    ask DOUBLE,
    last DOUBLE,
    iv DOUBLE,
    delta DOUBLE,
    gamma DOUBLE,
    theta DOUBLE,
    vega DOUBLE,
    rho DOUBLE,
    open_interest LONG,
    volume LONG,
    ingested_at TIMESTAMP,
    dagster_run_id STRING
) TIMESTAMP(quote_ts) PARTITION BY DAY;
"""

RAW_THETADATA_OPTIONS_EOD = """
CREATE TABLE IF NOT EXISTS raw_thetadata_options_eod (
    quote_date TIMESTAMP,
    underlying SYMBOL,
    expiry TIMESTAMP,
    strike DOUBLE,
    right SYMBOL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    trade_count LONG,
    bid DOUBLE,
    ask DOUBLE,
    iv DOUBLE,
    delta DOUBLE,
    gamma DOUBLE,
    theta DOUBLE,
    vega DOUBLE,
    rho DOUBLE,
    open_interest LONG,
    ingested_at TIMESTAMP,
    dagster_run_id STRING
) TIMESTAMP(quote_date) PARTITION BY MONTH;
"""

# ---------------------------------------------------------------------------
# Canonical tables (reconciled, downstream input)
# ---------------------------------------------------------------------------

CANONICAL_EQUITY_OHLCV = """
CREATE TABLE IF NOT EXISTS canonical_equity_ohlcv (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    vwap DOUBLE,
    trade_count LONG,
    source_vendor SYMBOL,
    reconcile_method SYMBOL,
    validation_status SYMBOL,
    canonicalized_at TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY MONTH WAL
  DEDUP UPSERT KEYS(timestamp, symbol);
"""

CANONICAL_FUNDAMENTALS = """
CREATE TABLE IF NOT EXISTS canonical_fundamentals (
    report_date TIMESTAMP,
    symbol SYMBOL,
    fiscal_period SYMBOL,
    period SYMBOL,
    revenue DOUBLE,
    cost_of_revenue DOUBLE,
    gross_profit DOUBLE,
    operating_expense DOUBLE,
    operating_income DOUBLE,
    net_income DOUBLE,
    eps DOUBLE,
    eps_diluted DOUBLE,
    shares_outstanding DOUBLE,
    shares_outstanding_diluted DOUBLE,
    free_cash_flow DOUBLE,
    total_assets DOUBLE,
    total_liabilities DOUBLE,
    shareholder_equity DOUBLE,
    source_vendor SYMBOL,
    reconcile_method SYMBOL,
    canonicalized_at TIMESTAMP
) TIMESTAMP(report_date) PARTITION BY YEAR;
"""

CANONICAL_FINANCIAL_METRICS = """
CREATE TABLE IF NOT EXISTS canonical_financial_metrics (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    market_cap DOUBLE,
    pe_ratio DOUBLE,
    ps_ratio DOUBLE,
    pb_ratio DOUBLE,
    ev_ebitda DOUBLE,
    roe DOUBLE,
    gross_margin DOUBLE,
    operating_margin DOUBLE,
    net_margin DOUBLE,
    fcf_margin DOUBLE,
    debt_to_equity DOUBLE,
    revenue_growth_yoy DOUBLE,
    eps_growth_yoy DOUBLE,
    shares_outstanding DOUBLE,
    source_vendor SYMBOL,
    canonicalized_at TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY YEAR;
"""

CANONICAL_OPTIONS_CHAIN = """
CREATE TABLE IF NOT EXISTS canonical_options_chain (
    quote_date TIMESTAMP,
    underlying SYMBOL,
    expiry TIMESTAMP,
    strike DOUBLE,
    right SYMBOL,
    bid DOUBLE,
    ask DOUBLE,
    last DOUBLE,
    iv DOUBLE,
    delta DOUBLE,
    gamma DOUBLE,
    theta DOUBLE,
    vega DOUBLE,
    rho DOUBLE,
    open_interest LONG,
    volume LONG,
    source_vendor SYMBOL,
    reconcile_method SYMBOL,
    canonicalized_at TIMESTAMP,
    dagster_run_id STRING
) TIMESTAMP(quote_date) PARTITION BY MONTH WAL
  DEDUP UPSERT KEYS(quote_date, underlying, expiry, strike, right, source_vendor);
"""

CANONICAL_INSIDER_TRADES = """
CREATE TABLE IF NOT EXISTS canonical_insider_trades (
    filing_date TIMESTAMP,
    symbol SYMBOL,
    insider_name STRING,
    insider_title STRING,
    is_board_director BOOLEAN,
    transaction_date TIMESTAMP,
    transaction_type SYMBOL,
    shares DOUBLE,
    price_per_share DOUBLE,
    total_value DOUBLE,
    shares_owned_before DOUBLE,
    shares_owned_after DOUBLE,
    security_title STRING,
    source_vendor SYMBOL,
    canonicalized_at TIMESTAMP,
    dagster_run_id STRING
) TIMESTAMP(filing_date) PARTITION BY YEAR WAL
  DEDUP UPSERT KEYS(filing_date, symbol, insider_name, transaction_date, transaction_type, shares);
"""

CANONICAL_INSTITUTIONAL_HOLDINGS = """
CREATE TABLE IF NOT EXISTS canonical_institutional_holdings (
    filing_date TIMESTAMP,
    symbol SYMBOL,
    report_period TIMESTAMP,
    accession_number STRING,
    filer_cik SYMBOL,
    filer_name STRING,
    shares DOUBLE,
    value_usd DOUBLE,
    source_vendor SYMBOL,
    canonicalized_at TIMESTAMP,
    dagster_run_id STRING
) TIMESTAMP(filing_date) PARTITION BY YEAR WAL
  DEDUP UPSERT KEYS(filing_date, symbol, filer_cik, report_period);
"""

CANONICAL_INST_OWNERSHIP = """
CREATE TABLE IF NOT EXISTS canonical_inst_ownership (
    filing_date TIMESTAMP,
    symbol SYMBOL,
    report_period TIMESTAMP,
    total_shares DOUBLE,
    total_value_usd DOUBLE,
    filer_count LONG,
    source_vendor SYMBOL,
    canonicalized_at TIMESTAMP
) TIMESTAMP(filing_date) PARTITION BY YEAR WAL
  DEDUP UPSERT KEYS(filing_date, symbol, report_period);
"""

# ---------------------------------------------------------------------------
# Feature tables
# ---------------------------------------------------------------------------

FEATURES = """
CREATE TABLE IF NOT EXISTS features (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    feature_set SYMBOL,
    feature_set_version SYMBOL,
    ret_1d DOUBLE,
    ret_5d DOUBLE,
    ret_21d DOUBLE,
    rv_21d DOUBLE,
    rv_63d DOUBLE,
    dist_20d_high DOUBLE,
    dist_20d_low DOUBLE,
    mom_3m DOUBLE,
    mom_12m_excl_1m DOUBLE,
    log_mkt_cap DOUBLE,
    size_rank DOUBLE,
    value_rank DOUBLE,
    pe_ttm DOUBLE,
    ps_ttm DOUBLE,
    pb DOUBLE,
    ev_ebitda DOUBLE,
    roe DOUBLE,
    gross_margin DOUBLE,
    operating_margin DOUBLE,
    fcf_margin DOUBLE,
    debt_equity DOUBLE,
    eps_growth_1y DOUBLE,
    revenue_growth_1y DOUBLE,
    market_vol_20d DOUBLE,
    market_trend_20d DOUBLE,
    dispersion_20d DOUBLE,
    corr_mean_20d DOUBLE,
    atm_iv DOUBLE,
    skew_25d DOUBLE,
    iv_term_slope DOUBLE,
    put_call_oi_ratio DOUBLE,
    net_gamma_exposure DOUBLE,
    insider_net_buy_90d DOUBLE,
    insider_buy_intensity_30d DOUBLE,
    insider_cluster_30d DOUBLE,
    exec_net_buy_90d DOUBLE,
    inst_ownership_pct DOUBLE,
    inst_top10_share DOUBLE,
    spy_atm_iv DOUBLE,
    spy_iv_zscore_60d DOUBLE,
    spy_vrp DOUBLE,
    spy_iv_term_slope DOUBLE,
    spy_skew_zscore_60d DOUBLE,
    spy_gex_sign DOUBLE,
    spy_gex_norm DOUBLE,
    spy_iv_delta_5d DOUBLE,
    spy_slope_delta_5d DOUBLE,
    computed_at TIMESTAMP,
    dagster_run_id STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH WAL
  DEDUP UPSERT KEYS(timestamp, symbol, feature_set, feature_set_version);
"""

FEATURE_WATERMARKS = """
CREATE TABLE IF NOT EXISTS feature_watermarks (
    updated_at TIMESTAMP,
    symbol SYMBOL,
    feature_set SYMBOL,
    last_computed_date TIMESTAMP,
    feature_set_version SYMBOL
) TIMESTAMP(updated_at);
"""

# ---------------------------------------------------------------------------
# Experiment + execution tables
# ---------------------------------------------------------------------------

EXPERIMENT_INDEX = """
CREATE TABLE IF NOT EXISTS experiment_index (
    created_at TIMESTAMP,
    experiment_id SYMBOL,
    universe STRING,
    feature_set SYMBOL,
    policy_type SYMBOL,
    reward_version SYMBOL,
    regime_feature_set SYMBOL,
    regime_labeling SYMBOL,
    sharpe DOUBLE,
    calmar DOUBLE,
    max_drawdown DOUBLE,
    total_return DOUBLE,
    annualized_return DOUBLE,
    win_rate DOUBLE,
    turnover_1d_mean DOUBLE,
    nan_inf_violations LONG,
    qualification_status SYMBOL,
    promotion_tier SYMBOL,
    stale BOOLEAN,
    canonical_hash STRING,
    spec_path STRING,
    metrics_path STRING,
    dagster_run_id STRING
) TIMESTAMP(created_at) PARTITION BY YEAR;
"""

EXECUTION_LOG = """
CREATE TABLE IF NOT EXISTS execution_log (
    timestamp TIMESTAMP,
    experiment_id SYMBOL,
    run_id SYMBOL,
    mode SYMBOL,
    step INT,
    symbol SYMBOL,
    target_weight DOUBLE,
    realized_weight DOUBLE,
    fill_price DOUBLE,
    slippage_bps DOUBLE,
    fees DOUBLE,
    rejected BOOLEAN,
    reject_reason STRING,
    portfolio_value DOUBLE,
    cash DOUBLE,
    regime_bucket SYMBOL,
    dagster_run_id STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

EXECUTION_METRICS = """
CREATE TABLE IF NOT EXISTS execution_metrics (
    timestamp TIMESTAMP,
    experiment_id SYMBOL,
    run_id SYMBOL,
    mode SYMBOL,
    fill_rate DOUBLE,
    reject_rate DOUBLE,
    avg_slippage_bps DOUBLE,
    p95_slippage_bps DOUBLE,
    total_fees DOUBLE,
    total_turnover DOUBLE,
    execution_halts LONG,
    sharpe DOUBLE,
    max_drawdown DOUBLE,
    total_return DOUBLE,
    dagster_run_id STRING
) TIMESTAMP(timestamp) PARTITION BY YEAR;
"""

POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
    timestamp TIMESTAMP,
    experiment_id SYMBOL,
    mode SYMBOL,
    symbol SYMBOL,
    quantity DOUBLE,
    avg_entry_price DOUBLE,
    notional DOUBLE,
    unrealized_pnl DOUBLE,
    realized_pnl DOUBLE
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    timestamp TIMESTAMP,
    order_id STRING,
    experiment_id SYMBOL,
    mode SYMBOL,
    symbol SYMBOL,
    side SYMBOL,
    quantity DOUBLE,
    order_type SYMBOL,
    status SYMBOL,
    fill_price DOUBLE,
    fill_quantity DOUBLE,
    slippage_bps DOUBLE,
    fees DOUBLE,
    risk_check_result SYMBOL,
    risk_check_details STRING,
    broker_order_id STRING,
    dagster_run_id STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

PORTFOLIO_STATE = """
CREATE TABLE IF NOT EXISTS portfolio_state (
    timestamp TIMESTAMP,
    experiment_id SYMBOL,
    mode SYMBOL,
    nav DOUBLE,
    cash DOUBLE,
    gross_exposure DOUBLE,
    net_exposure DOUBLE,
    leverage DOUBLE,
    num_positions INT,
    daily_pnl DOUBLE,
    peak_nav DOUBLE,
    drawdown DOUBLE,
    dagster_run_id STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

# ---------------------------------------------------------------------------
# Risk + ops tables
# ---------------------------------------------------------------------------

RISK_DECISIONS = """
CREATE TABLE IF NOT EXISTS risk_decisions (
    timestamp TIMESTAMP,
    rule_id SYMBOL,
    experiment_id SYMBOL,
    mode SYMBOL,
    input_metrics STRING,
    decision SYMBOL,
    action_taken STRING,
    original_size DOUBLE,
    reduced_size DOUBLE,
    dagster_run_id STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

KILL_SWITCHES = """
CREATE TABLE IF NOT EXISTS kill_switches (
    timestamp TIMESTAMP,
    experiment_id SYMBOL,
    mode SYMBOL,
    trigger SYMBOL,
    action SYMBOL,
    triggered_by SYMBOL,
    details STRING,
    resolved BOOLEAN,
    resolved_at TIMESTAMP,
    resolved_by SYMBOL
) TIMESTAMP(timestamp) PARTITION BY YEAR;
"""

PROMOTIONS = """
CREATE TABLE IF NOT EXISTS promotions (
    promoted_at TIMESTAMP,
    experiment_id SYMBOL,
    tier SYMBOL,
    promoted_by SYMBOL,
    qualification_passed BOOLEAN,
    sharpe DOUBLE,
    max_drawdown DOUBLE,
    dagster_run_id STRING
) TIMESTAMP(promoted_at) PARTITION BY YEAR;
"""

AUDIT_TRAIL = """
CREATE TABLE IF NOT EXISTS audit_trail (
    timestamp TIMESTAMP,
    tool_name SYMBOL,
    invoker SYMBOL,
    experiment_id SYMBOL,
    mode SYMBOL,
    parameters STRING,
    result_status SYMBOL,
    result_summary STRING,
    duration_ms LONG,
    dagster_run_id STRING,
    quanttown_molecule_id STRING,
    quanttown_bead_id STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

TRADING_HEARTBEAT = """
CREATE TABLE IF NOT EXISTS trading_heartbeat (
    timestamp TIMESTAMP,
    experiment_id SYMBOL,
    mode SYMBOL,
    loop_iteration LONG,
    orders_pending INT,
    last_bar_received TIMESTAMP,
    last_fill_received TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

RECONCILIATION_LOG = """
CREATE TABLE IF NOT EXISTS reconciliation_log (
    timestamp TIMESTAMP,
    domain SYMBOL,
    symbol SYMBOL,
    primary_vendor SYMBOL,
    fallback_used BOOLEAN,
    fallback_vendor SYMBOL,
    validation_warnings STRING,
    dagster_run_id STRING,
    reconciled_at TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

CANONICAL_PINS = """
CREATE TABLE IF NOT EXISTS canonical_pins (
    pinned_at TIMESTAMP,
    experiment_id SYMBOL,
    canonicalized_at TIMESTAMP
) TIMESTAMP(pinned_at) PARTITION BY YEAR;
"""

CANONICAL_HASHES = """
CREATE TABLE IF NOT EXISTS canonical_hashes (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    date_from TIMESTAMP,
    date_to TIMESTAMP,
    canonical_hash STRING,
    universe_date_hash STRING,
    dagster_run_id STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH;
"""

# ---------------------------------------------------------------------------
# Ops instrumentation table
# ---------------------------------------------------------------------------

JOB_RUNS = """
CREATE TABLE IF NOT EXISTS job_runs (
    started_at TIMESTAMP,
    job_name SYMBOL,
    dagster_run_id SYMBOL,
    status SYMBOL,
    finished_at TIMESTAMP,
    duration_s DOUBLE,
    rows_written LONG,
    detail STRING,
    failure_cause STRING
) TIMESTAMP(started_at) PARTITION BY MONTH WAL
  DEDUP UPSERT KEYS(started_at, job_name, dagster_run_id);
"""

# ---------------------------------------------------------------------------
# Ordered list of all DDL statements
# ---------------------------------------------------------------------------

ALL_TABLES: list[str] = [
    # Raw
    RAW_ALPACA_EQUITY_OHLCV,
    RAW_FD_FUNDAMENTALS,
    RAW_FD_FINANCIAL_METRICS,
    RAW_FD_INSIDER_TRADES,
    RAW_FD_ANALYST_ESTIMATES,
    RAW_FD_EARNINGS,
    RAW_FD_INSTITUTIONAL_HOLDINGS,
    RAW_THETADATA_OPTIONS_CHAIN,
    RAW_THETADATA_OPTIONS_EOD,
    # Canonical
    CANONICAL_EQUITY_OHLCV,
    CANONICAL_FUNDAMENTALS,
    CANONICAL_FINANCIAL_METRICS,
    CANONICAL_OPTIONS_CHAIN,
    CANONICAL_INSIDER_TRADES,
    CANONICAL_INSTITUTIONAL_HOLDINGS,
    CANONICAL_INST_OWNERSHIP,
    # Features
    FEATURES,
    FEATURE_WATERMARKS,
    # Experiment + execution
    EXPERIMENT_INDEX,
    EXECUTION_LOG,
    EXECUTION_METRICS,
    POSITIONS,
    ORDERS,
    PORTFOLIO_STATE,
    # Risk + ops
    RISK_DECISIONS,
    KILL_SWITCHES,
    PROMOTIONS,
    AUDIT_TRAIL,
    TRADING_HEARTBEAT,
    RECONCILIATION_LOG,
    CANONICAL_PINS,
    CANONICAL_HASHES,
    # Dashboard / ops instrumentation
    JOB_RUNS,
]


# ---------------------------------------------------------------------------
# Idempotent migrations for tables that already exist
# ---------------------------------------------------------------------------
# CREATE TABLE IF NOT EXISTS never alters an existing table, so schema changes
# to already-created tables must be applied as explicit ALTERs here. Each must
# be safe to run repeatedly (QuestDB re-applying identical DEDUP keys is a
# no-op). Enabling DEDUP makes canonical tables idempotent across reruns: a
# second run's rows UPSERT in place rather than append.
MIGRATIONS: list[str] = [
    # ya-n4bhm: equity cross-run idempotency
    "ALTER TABLE canonical_equity_ohlcv DEDUP ENABLE UPSERT KEYS(timestamp, symbol)",
    # ya-6e7ok: options cross-run idempotency — live+EOD rows coexist via source_vendor key.
    # QuestDB accepts DOUBLE (strike) and non-designated TIMESTAMP (expiry) as upsert keys.
    "ALTER TABLE canonical_options_chain DEDUP ENABLE UPSERT KEYS(quote_date, underlying, expiry, strike, right, source_vendor)",
    # ya-i6nvo: feature reruns upsert instead of appending — stale rows from a
    # prior run for the same (timestamp, symbol, feature_set) would otherwise be
    # read alongside fresh ones (fetch_features does not filter by computed_at).
    "ALTER TABLE features DEDUP ENABLE UPSERT KEYS(timestamp, symbol, feature_set, feature_set_version)",
    # ya-2gqv7: insider_trades — add filing_date, transaction_date, and 4 new signal columns.
    # filed_at now truly = filing date (point-in-time fix); transaction_date preserved as column.
    "ALTER TABLE raw_fd_insider_trades ADD COLUMN filing_date TIMESTAMP",
    "ALTER TABLE raw_fd_insider_trades ADD COLUMN transaction_date TIMESTAMP",
    "ALTER TABLE raw_fd_insider_trades ADD COLUMN is_board_director BOOLEAN",
    "ALTER TABLE raw_fd_insider_trades ADD COLUMN shares_owned_before DOUBLE",
    "ALTER TABLE raw_fd_insider_trades ADD COLUMN security_title STRING",
    "ALTER TABLE raw_fd_insider_trades ADD COLUMN issuer STRING",
    # ya-ayjf6: canonical insider_trades + institutional_holdings — DEDUP from day one.
    # Tables born with DEDUP in CREATE TABLE; these migrate any pre-existing table created
    # before Stage 3b landed.
    "ALTER TABLE canonical_insider_trades DEDUP ENABLE UPSERT KEYS(filing_date, symbol, insider_name, transaction_date, transaction_type, shares)",
    "ALTER TABLE canonical_institutional_holdings DEDUP ENABLE UPSERT KEYS(filing_date, symbol, filer_cik, report_period)",
    "ALTER TABLE canonical_inst_ownership DEDUP ENABLE UPSERT KEYS(filing_date, symbol, report_period)",
    # ya-vs9a1: job_runs instrumentation table — DEDUP from day one; migrate any pre-existing table.
    "ALTER TABLE job_runs DEDUP ENABLE UPSERT KEYS(started_at, job_name, dagster_run_id)",
    # ya-tvaa0: Stage 3c — insider/institutional feature columns added to features table.
    "ALTER TABLE features ADD COLUMN insider_net_buy_90d DOUBLE",
    "ALTER TABLE features ADD COLUMN insider_buy_intensity_30d DOUBLE",
    "ALTER TABLE features ADD COLUMN insider_cluster_30d DOUBLE",
    "ALTER TABLE features ADD COLUMN exec_net_buy_90d DOUBLE",
    "ALTER TABLE features ADD COLUMN inst_ownership_pct DOUBLE",
    "ALTER TABLE features ADD COLUMN inst_top10_share DOUBLE",
    # ya-3rkix: Stage 4a — regime_v2 options-implied feature columns added to features table.
    "ALTER TABLE features ADD COLUMN spy_atm_iv DOUBLE",
    "ALTER TABLE features ADD COLUMN spy_iv_zscore_60d DOUBLE",
    "ALTER TABLE features ADD COLUMN spy_vrp DOUBLE",
    "ALTER TABLE features ADD COLUMN spy_iv_term_slope DOUBLE",
    "ALTER TABLE features ADD COLUMN spy_skew_zscore_60d DOUBLE",
    "ALTER TABLE features ADD COLUMN spy_gex_sign DOUBLE",
    "ALTER TABLE features ADD COLUMN spy_gex_norm DOUBLE",
    "ALTER TABLE features ADD COLUMN spy_iv_delta_5d DOUBLE",
    "ALTER TABLE features ADD COLUMN spy_slope_delta_5d DOUBLE",
]


def create_all_tables(resource: QuestDBResource | None = None) -> None:
    """Execute all DDL statements against QuestDB via PG wire, then migrations."""
    if resource is None:
        resource = QuestDBResource()

    conn = psycopg2.connect(
        host=resource.pg_host,
        port=resource.pg_port,
        user=resource.pg_user,
        password=resource.pg_password,
        database=resource.pg_database,
    )
    conn.autocommit = True

    try:
        cur = conn.cursor()
        for ddl in ALL_TABLES:
            table_name = ddl.split("IF NOT EXISTS")[1].split("(")[0].strip()
            print(f"Creating table: {table_name}")
            cur.execute(ddl)
        for migration in MIGRATIONS:
            try:
                cur.execute(migration)
                print(f"Applied migration: {migration}")
            except psycopg2.Error as exc:
                # Idempotent: tolerate 'already enabled' / re-apply no-ops so a
                # rerun against an up-to-date DB does not fail bootstrap.
                print(f"Migration skipped ({exc.pgerror or exc}): {migration}")
        cur.close()
        print(f"\nDone — {len(ALL_TABLES)} tables created, "
              f"{len(MIGRATIONS)} migrations applied.")
    finally:
        conn.close()


if __name__ == "__main__":
    create_all_tables()

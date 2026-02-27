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
) TIMESTAMP(timestamp) PARTITION BY MONTH;
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
    computed_at TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY MONTH;
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
    # Canonical
    CANONICAL_EQUITY_OHLCV,
    CANONICAL_FUNDAMENTALS,
    CANONICAL_FINANCIAL_METRICS,
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
]


def create_all_tables(resource: QuestDBResource | None = None) -> None:
    """Execute all DDL statements against QuestDB via PG wire."""
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
        cur.close()
        print(f"\nDone — {len(ALL_TABLES)} tables created.")
    finally:
        conn.close()


if __name__ == "__main__":
    create_all_tables()

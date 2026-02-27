"""Metric computation from weight/return time series.

Computes all PRD §8.3 metrics: sharpe, calmar, sortino, total_return,
annualized_return, max_drawdown, max_drawdown_duration, turnover, holding
period, win_rate, profit_factor, and safety checks.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

ANNUALIZATION_FACTOR = 252


@dataclass(frozen=True)
class PerformanceMetrics:
    sharpe: float
    calmar: float
    sortino: float
    total_return: float
    annualized_return: float
    max_drawdown: float
    max_drawdown_duration: int  # trading days


@dataclass(frozen=True)
class TradingMetrics:
    turnover_1d_mean: float
    turnover_1d_std: float
    avg_holding_period: float  # trading days
    win_rate: float
    profit_factor: float


@dataclass(frozen=True)
class SafetyMetrics:
    nan_inf_violations: int
    constraint_violations: int


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------

def compute_sharpe(returns: pd.Series) -> float:
    """Annualized Sharpe ratio (excess return assumed = raw return)."""
    if len(returns) < 2:
        return 0.0
    std = returns.std()
    if std < 1e-15:
        return 0.0
    return float(returns.mean() / std * np.sqrt(ANNUALIZATION_FACTOR))


def compute_sortino(returns: pd.Series) -> float:
    """Annualized Sortino ratio (downside deviation)."""
    if len(returns) < 2:
        return 0.0
    downside = returns[returns < 0]
    if len(downside) == 0 or downside.std() == 0:
        return float("inf") if returns.mean() > 0 else 0.0
    return float(returns.mean() / downside.std() * np.sqrt(ANNUALIZATION_FACTOR))


def compute_max_drawdown(equity_curve: pd.Series) -> tuple[float, int]:
    """Max drawdown (negative float) and duration in trading days."""
    if len(equity_curve) < 2:
        return 0.0, 0

    running_max = equity_curve.cummax()
    drawdowns = (equity_curve - running_max) / running_max
    max_dd = float(drawdowns.min())

    # Duration: longest streak below previous peak
    is_in_dd = drawdowns < 0
    if not is_in_dd.any():
        return 0.0, 0

    # Find longest contiguous drawdown period
    groups = (~is_in_dd).cumsum()
    dd_lengths = is_in_dd.groupby(groups).sum()
    max_dd_duration = int(dd_lengths.max()) if len(dd_lengths) > 0 else 0

    return max_dd, max_dd_duration


def compute_calmar(annualized_return: float, max_drawdown: float) -> float:
    """Calmar ratio: annualized return / abs(max drawdown)."""
    if max_drawdown == 0:
        return float("inf") if annualized_return > 0 else 0.0
    return float(annualized_return / abs(max_drawdown))


def compute_total_return(equity_curve: pd.Series) -> float:
    """Total cumulative return."""
    if len(equity_curve) < 2:
        return 0.0
    return float(equity_curve.iloc[-1] / equity_curve.iloc[0] - 1.0)


def compute_annualized_return(total_return: float, n_days: int) -> float:
    """Annualize a total return over n trading days."""
    if n_days <= 0:
        return 0.0
    years = n_days / ANNUALIZATION_FACTOR
    if years <= 0:
        return 0.0
    return float((1.0 + total_return) ** (1.0 / years) - 1.0)


def compute_performance_metrics(
    portfolio_returns: pd.Series,
    equity_curve: pd.Series,
) -> PerformanceMetrics:
    """Compute all performance metrics from returns and equity curve."""
    total_ret = compute_total_return(equity_curve)
    n_days = len(portfolio_returns)
    ann_ret = compute_annualized_return(total_ret, n_days)
    max_dd, max_dd_dur = compute_max_drawdown(equity_curve)

    return PerformanceMetrics(
        sharpe=compute_sharpe(portfolio_returns),
        calmar=compute_calmar(ann_ret, max_dd),
        sortino=compute_sortino(portfolio_returns),
        total_return=total_ret,
        annualized_return=ann_ret,
        max_drawdown=max_dd,
        max_drawdown_duration=max_dd_dur,
    )


# ---------------------------------------------------------------------------
# Trading
# ---------------------------------------------------------------------------

def compute_turnover(weights: pd.DataFrame) -> pd.Series:
    """Daily turnover: sum of absolute weight changes across symbols."""
    if len(weights) < 2:
        return pd.Series(dtype=float)
    return weights.diff().abs().sum(axis=1).iloc[1:]


def compute_avg_holding_period(weights: pd.DataFrame) -> float:
    """Average holding period in trading days.

    Estimated as 1 / mean(turnover) when turnover > 0.
    """
    turnover = compute_turnover(weights)
    mean_turnover = turnover.mean()
    if mean_turnover <= 0:
        return float(len(weights))
    return float(1.0 / mean_turnover)


def compute_win_rate(daily_pnl: pd.Series) -> float:
    """Fraction of days with positive PnL."""
    if len(daily_pnl) == 0:
        return 0.0
    return float((daily_pnl > 0).sum() / len(daily_pnl))


def compute_profit_factor(daily_pnl: pd.Series) -> float:
    """Gross profit / gross loss."""
    gross_profit = daily_pnl[daily_pnl > 0].sum()
    gross_loss = abs(daily_pnl[daily_pnl < 0].sum())
    if gross_loss == 0:
        return float("inf") if gross_profit > 0 else 0.0
    return float(gross_profit / gross_loss)


def compute_trading_metrics(
    weights: pd.DataFrame,
    daily_pnl: pd.Series,
) -> TradingMetrics:
    """Compute all trading metrics from weights and PnL."""
    turnover = compute_turnover(weights)
    return TradingMetrics(
        turnover_1d_mean=float(turnover.mean()) if len(turnover) > 0 else 0.0,
        turnover_1d_std=float(turnover.std()) if len(turnover) > 1 else 0.0,
        avg_holding_period=compute_avg_holding_period(weights),
        win_rate=compute_win_rate(daily_pnl),
        profit_factor=compute_profit_factor(daily_pnl),
    )


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------

def compute_safety_metrics(
    weights: pd.DataFrame,
    returns: pd.DataFrame,
    max_gross_exposure: float = 1.0,
    max_symbol_weight: float = 1.0,
) -> SafetyMetrics:
    """Check for NaN/Inf violations and constraint breaches."""
    # NaN/Inf in weights or returns
    nan_inf_w = int(np.isinf(weights.values).sum() + np.isnan(weights.values).sum())
    nan_inf_r = int(np.isinf(returns.values).sum() + np.isnan(returns.values).sum())
    nan_inf_violations = nan_inf_w + nan_inf_r

    # Constraint violations: gross exposure or symbol weight breaches
    constraint_violations = 0
    gross_exposure = weights.abs().sum(axis=1)
    constraint_violations += int((gross_exposure > max_gross_exposure + 1e-9).sum())

    symbol_breach = (weights.abs() > max_symbol_weight + 1e-9).sum().sum()
    constraint_violations += int(symbol_breach)

    return SafetyMetrics(
        nan_inf_violations=nan_inf_violations,
        constraint_violations=constraint_violations,
    )

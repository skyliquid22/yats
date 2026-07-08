"""Structural tests for Dashboard D2 static frontend.

Guards ya-c85dk: index.html and app.js exist with expected structure.
End-to-end render verification is covered by Dashboard D3 live deployment.
"""
from __future__ import annotations

from pathlib import Path

import pytest

STATIC = Path(__file__).parent.parent / "dashboard" / "static"
INDEX  = STATIC / "index.html"
APP_JS = STATIC / "app.js"


# ---------------------------------------------------------------------------
# File presence
# ---------------------------------------------------------------------------


def test_index_html_exists():
    assert INDEX.exists(), "dashboard/static/index.html is missing"


def test_app_js_exists():
    assert APP_JS.exists(), "dashboard/static/app.js is missing"


# ---------------------------------------------------------------------------
# index.html structure
# ---------------------------------------------------------------------------


class TestIndexHtml:
    @pytest.fixture(autouse=True)
    def src(self):
        self.html = INDEX.read_text(encoding="utf-8")

    def test_has_three_tabs(self):
        assert 'data-page="system"' in self.html
        assert 'data-page="experiments"' in self.html
        assert 'data-page="trading"' in self.html

    def test_has_theme_toggle(self):
        assert 'id="theme-btn"' in self.html

    def test_has_css_tokens_light_and_dark(self):
        assert "--bg:" in self.html
        assert "prefers-color-scheme: dark" in self.html
        assert 'data-theme="dark"' in self.html

    def test_avenir_font_referenced(self):
        assert "Avenir" in self.html

    def test_sf_mono_font_referenced(self):
        assert "SF Mono" in self.html

    def test_tabular_nums(self):
        assert "tabular-nums" in self.html

    def test_reduced_motion_media_query(self):
        assert "prefers-reduced-motion" in self.html

    def test_focus_visible(self):
        assert ":focus-visible" in self.html

    def test_app_js_script_tag(self):
        assert 'src="app.js"' in self.html

    def test_health_chips_container(self):
        assert 'id="health-chips"' in self.html

    def test_coverage_cards_container(self):
        assert 'id="coverage-cards"' in self.html

    def test_equity_filter_select(self):
        assert 'id="equity-filter"' in self.html
        # All three options
        assert ">All<" in self.html
        assert ">Stocks<" in self.html
        assert ">ETFs<" in self.html

    def test_storage_table_container(self):
        assert 'id="storage-table"' in self.html

    def test_runs_feed_container(self):
        assert 'id="runs-feed"' in self.html

    def test_exp_search_input(self):
        assert 'id="exp-search"' in self.html

    def test_deflation_value_container(self):
        assert 'id="deflation-value"' in self.html

    def test_exp_inspector_container(self):
        assert 'id="exp-inspector"' in self.html

    def test_trading_containers(self):
        assert 'id="live-experiments"' in self.html
        assert 'id="portfolio-state"' in self.html
        assert 'id="heartbeat-rows"' in self.html
        assert 'id="open-orders-rows"' in self.html
        assert 'id="risk-decisions-rows"' in self.html

    def test_poll_indicator(self):
        assert 'id="poll-indicator"' in self.html

    def test_accessibility_roles(self):
        assert 'role="tablist"' in self.html
        assert 'role="tab"' in self.html
        assert 'role="tabpanel"' in self.html


# ---------------------------------------------------------------------------
# app.js structure
# ---------------------------------------------------------------------------


class TestAppJs:
    @pytest.fixture(autouse=True)
    def src(self):
        self.js = APP_JS.read_text(encoding="utf-8")

    def test_api_shape_comment_block(self):
        assert "GET /api/health" in self.js
        assert "GET /api/coverage" in self.js
        assert "GET /api/storage" in self.js
        assert "GET /api/runs" in self.js
        assert "GET /api/experiments" in self.js
        assert "GET /api/trading" in self.js

    def test_alert_thresholds_are_status_enums_comment(self):
        assert "status ENUMS" in self.js or "status enums" in self.js.lower()

    def test_poll_interval_30s(self):
        assert "30000" in self.js

    def test_visibility_state_check(self):
        assert "visibilityState" in self.js
        assert "visibilitychange" in self.js

    def test_equity_curve_function(self):
        assert "renderEquityCurve" in self.js

    def test_max_dd_segment_function(self):
        assert "_findMaxDDSegment" in self.js or "findMaxDDSegment" in self.js

    def test_svg_polyline_used(self):
        assert "polyline" in self.js
        assert "curve-dd" in self.js

    def test_svg_area_path(self):
        assert "curve-area" in self.js

    def test_render_functions_present(self):
        for fn in ("renderHealth", "renderCoverage", "renderStorage",
                   "renderRuns", "renderExpResults", "renderExpInspector",
                   "renderTrading"):
            assert fn in self.js, f"{fn} not found in app.js"

    def test_theme_toggle(self):
        assert "data-theme" in self.js
        assert "prefers-color-scheme" in self.js

    def test_experiment_search_debounced(self):
        assert "debounce" in self.js.lower() or "_searchTimer" in self.js

    def test_storage_drill_down(self):
        assert "/api/storage/" in self.js
        assert "tickers" in self.js

    def test_escape_helper(self):
        assert "function esc" in self.js

    def test_esc_used_for_html_output(self):
        # Check that user-controlled strings go through esc()
        assert "esc(r.job_name)" in self.js or "esc(r.experiment_id)" in self.js

    def test_deflation_clock_rendered(self):
        assert "deflation-value" in self.js
        assert "deflation_clock" in self.js

    def test_kill_switch_section(self):
        assert "kill_switches" in self.js

    def test_coverage_filter_applied(self):
        assert "equityFilter" in self.js
        assert "stockOnly" in self.js

    def test_tabular_nums_class_used(self):
        assert "mono" in self.js

    def test_strict_mode(self):
        assert "'use strict'" in self.js

    def test_dom_content_loaded_init(self):
        assert "DOMContentLoaded" in self.js

    def test_poll_pauses_when_hidden(self):
        assert "hidden" in self.js
        # Verify the guard is present (not just the CSS .hidden class)
        assert "visibilityState" in self.js

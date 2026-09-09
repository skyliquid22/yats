# yats — common entry points.
#
#   make demo   Offline walk-forward + Deflated Sharpe demo: generates the
#               synthetic bundle if missing, then runs a reduced 2-config
#               sweep through the real WFO/DSR machinery. No vendor keys,
#               no QuestDB, no network. ~1-2 minutes on a laptop.
#               Runs WITHOUT the `rl` extra — no torch/SB3/gymnasium needed.
#   make test   Full test suite minus live tests (installs the `rl` extra;
#               RL trainer/env tests need torch + stable-baselines3).

# Demo path is RL-free by design: plain `uv run` installs only the core
# dependencies (the rl extra is opt-in via --extra rl).
PYRUN := PYTHONPATH=.:pipelines uv run

DEMO_BUNDLE := demo/data/demo_panel.parquet

.PHONY: demo demo-data test

demo:
	@if [ ! -f $(DEMO_BUNDLE) ]; then \
		echo "[make] demo bundle missing — generating"; \
		$(PYRUN) python demo/generate_data.py; \
	fi
	$(PYRUN) python demo/run_demo.py

demo-data:
	$(PYRUN) python demo/generate_data.py --force

# OMP_NUM_THREADS=1: torch and lightgbm each bundle libomp; with >1 OpenMP
# thread the duplicate runtimes deadlock in torch's QR init on macOS.
test:
	OMP_NUM_THREADS=1 PYTHONPATH=.:pipelines uv run --extra rl --with pytest pytest -q -k "not live"

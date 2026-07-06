"""
Tests for DagsterClient.makeRunId canonical serialization (ya-jtj5b).

The TypeScript implementation uses a recursive key-sort serializer.
These tests replicate the algorithm in Python to verify algorithm properties:
  - different configs → different IDs
  - same config → same ID (idempotent)
  - nested key order irrelevant (canonical form)

The old buggy implementation used JSON.stringify(obj, keysArray) which is a
REPLACER array — it silently dropped all nested keys, making runConfig serialize
as {} for any nested object and colliding all promote-tier IDs to a single hash.
"""
import hashlib
import json


def _canonicalize(val) -> str:
    """Mirror of DagsterClient.canonicalize in TypeScript (recursive key-sort)."""
    if val is None or not isinstance(val, dict):
        return json.dumps(val, separators=(",", ":"))
    parts = [
        f"{json.dumps(k, separators=(',', ':'))}:{_canonicalize(val[k])}"
        for k in sorted(val.keys())
    ]
    return "{" + ",".join(parts) + "}"


def _make_run_id(job_name: str, run_config: dict, invoker: str) -> str:
    """Mirror of DagsterClient.makeRunId."""
    stable = _canonicalize({"invoker": invoker, "jobName": job_name, "runConfig": run_config})
    return hashlib.sha256(stable.encode()).hexdigest()[:32]


def _old_buggy_run_id(job_name: str, run_config: dict, invoker: str) -> str:
    """The broken implementation: JS array-replacer filters ALL nesting levels.

    JSON.stringify(obj, ['invoker','jobName','runConfig']) keeps only those
    keys at EVERY nesting level — so nested keys like 'ops', 'target_tier'
    are stripped, and runConfig always serializes as {}.
    """
    allowed = {"invoker", "jobName", "runConfig"}

    def apply_replacer(val):
        if isinstance(val, dict):
            return {k: apply_replacer(v) for k, v in val.items() if k in allowed}
        if isinstance(val, list):
            return [apply_replacer(item) for item in val]
        return val

    filtered = apply_replacer({"jobName": job_name, "runConfig": run_config, "invoker": invoker})
    stable = json.dumps(filtered, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(stable.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Core correctness: different configs → different IDs
# ---------------------------------------------------------------------------

def test_different_target_tiers_produce_different_ids():
    """promote to research vs candidate must not collide (the primary bug)."""
    research_config = {
        "ops": {"promote": {"config": {"experiment_id": "abc123", "target_tier": "research"}}}
    }
    candidate_config = {
        "ops": {"promote": {"config": {"experiment_id": "abc123", "target_tier": "candidate"}}}
    }
    id_research = _make_run_id("promote", research_config, "user-1")
    id_candidate = _make_run_id("promote", candidate_config, "user-1")
    assert id_research != id_candidate


def test_different_production_tier_differs_from_research():
    research_config = {
        "ops": {"promote": {"config": {"experiment_id": "abc123", "target_tier": "research"}}}
    }
    production_config = {
        "ops": {"promote": {"config": {"experiment_id": "abc123", "target_tier": "production"}}}
    }
    assert _make_run_id("promote", research_config, "user-1") != _make_run_id(
        "promote", production_config, "user-1"
    )


def test_different_experiment_ids_produce_different_run_ids():
    config_a = {"ops": {"run_experiment": {"config": {"experiment_id": "aaa", "mode": "backtest"}}}}
    config_b = {"ops": {"run_experiment": {"config": {"experiment_id": "bbb", "mode": "backtest"}}}}
    assert _make_run_id("experiment_run", config_a, "mcp") != _make_run_id(
        "experiment_run", config_b, "mcp"
    )


def test_different_invokers_produce_different_run_ids():
    config = {"ops": {"promote": {"config": {"experiment_id": "x1", "target_tier": "research"}}}}
    assert _make_run_id("promote", config, "agent-1") != _make_run_id("promote", config, "agent-2")


# ---------------------------------------------------------------------------
# Idempotency: same config → same ID
# ---------------------------------------------------------------------------

def test_same_config_produces_same_id():
    config = {
        "ops": {"promote": {"config": {"experiment_id": "abc", "target_tier": "candidate"}}}
    }
    id1 = _make_run_id("promote", config, "user-1")
    id2 = _make_run_id("promote", config, "user-1")
    assert id1 == id2


def test_id_length_is_32_hex_chars():
    run_id = _make_run_id("some_job", {"key": "val"}, "mcp")
    assert len(run_id) == 32
    assert all(c in "0123456789abcdef" for c in run_id)


# ---------------------------------------------------------------------------
# Nested key order irrelevance (canonical form)
# ---------------------------------------------------------------------------

def test_nested_key_order_does_not_affect_id():
    """Key order inside nested objects must not change the run ID."""
    config_abc = {"z": 3, "a": 1, "m": {"nested_z": 9, "nested_a": 1}}
    config_sorted = {"a": 1, "m": {"nested_a": 1, "nested_z": 9}, "z": 3}
    assert _make_run_id("job", config_abc, "mcp") == _make_run_id("job", config_sorted, "mcp")


def test_top_level_key_order_does_not_affect_id():
    """Top-level key order in runConfig must not change the run ID."""
    config_a = {"b": 2, "a": 1}
    config_b = {"a": 1, "b": 2}
    assert _make_run_id("job", config_a, "mcp") == _make_run_id("job", config_b, "mcp")


# ---------------------------------------------------------------------------
# Regression: old buggy implementation would have collided these
# ---------------------------------------------------------------------------

def test_old_bug_would_have_collided_tiers():
    """Verify that the old replacer-array bug actually collapsed nested keys."""
    research_config = {
        "ops": {"promote": {"config": {"experiment_id": "abc123", "target_tier": "research"}}}
    }
    candidate_config = {
        "ops": {"promote": {"config": {"experiment_id": "abc123", "target_tier": "candidate"}}}
    }
    # Old bug: both reduce to same hash because nested keys are stripped
    assert _old_buggy_run_id("promote", research_config, "mcp") == _old_buggy_run_id(
        "promote", candidate_config, "mcp"
    ), "regression guard: old bug should have collided these (nested keys stripped)"

    # New fix: they must NOT collide
    assert _make_run_id("promote", research_config, "mcp") != _make_run_id(
        "promote", candidate_config, "mcp"
    ), "fixed: different tiers must produce different IDs"

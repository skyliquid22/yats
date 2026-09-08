// Stability tests for the deterministic query hash (read provenance)
import { test } from "node:test";
import assert from "node:assert/strict";
import { canonicalizeQuery, queryHash } from "../bridge/query-audit.js";

// Golden constant: if canonicalization ever changes, this fails loudly —
// stored audit-trail hashes would no longer be verifiable.
const GOLDEN_SELECT_1_HASH =
  "1e73e771c14910ac99939c2416fcc70df62401ae23c470c75a8c90df2a2a2bd6";

test("queryHash matches golden constant for SELECT 1", () => {
  assert.equal(queryHash("SELECT 1", []), GOLDEN_SELECT_1_HASH);
});

test("queryHash is stable across whitespace-only formatting changes", () => {
  const a = queryHash("SELECT symbol,  close\n   FROM canonical_equity_ohlcv\n WHERE symbol = $1", ["AAPL"]);
  const b = queryHash("SELECT symbol, close FROM canonical_equity_ohlcv WHERE symbol = $1", ["AAPL"]);
  assert.equal(a, b);
});

test("queryHash is deterministic across repeated calls", () => {
  const sql = "SELECT * FROM features LIMIT 10";
  assert.equal(queryHash(sql, [1, "x"]), queryHash(sql, [1, "x"]));
});

test("queryHash changes when SQL text changes", () => {
  assert.notEqual(
    queryHash("SELECT * FROM features", []),
    queryHash("SELECT * FROM orders", []),
  );
});

test("queryHash changes when params change", () => {
  const sql = "SELECT * FROM features WHERE symbol = $1";
  assert.notEqual(queryHash(sql, ["AAPL"]), queryHash(sql, ["MSFT"]));
});

test("canonicalizeQuery normalizes whitespace and trims", () => {
  assert.equal(
    canonicalizeQuery("  SELECT\t1  ", []),
    canonicalizeQuery("SELECT 1", []),
  );
});

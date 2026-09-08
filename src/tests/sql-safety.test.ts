// Table-whitelist enforcement tests, including the quoted-identifier bypass fix
import { test } from "node:test";
import assert from "node:assert/strict";
import { checkTableAccess, extractTableNames, extractTableReferences } from "../auth/sql-safety.js";

test("extracts bare table names from FROM and JOIN", () => {
  const tables = extractTableNames(
    "SELECT * FROM features f JOIN canonical_equity_ohlcv c ON f.symbol = c.symbol",
  );
  assert.deepEqual(tables, ["features", "canonical_equity_ohlcv"]);
});

test("extracts double-quoted identifiers (whitelist bypass fix)", () => {
  assert.deepEqual(extractTableNames('SELECT * FROM "audit_trail"'), ["audit_trail"]);
  assert.deepEqual(extractTableNames('SELECT * FROM "Kill_Switches"'), ["kill_switches"]);
});

test("extracts single-quoted table names (QuestDB syntax)", () => {
  assert.deepEqual(extractTableNames("SELECT * FROM 'orders'"), ["orders"]);
});

test("extracts tables hidden behind comments", () => {
  assert.deepEqual(extractTableNames("SELECT * FROM/**/orders"), ["orders"]);
  assert.deepEqual(extractTableNames("SELECT * FROM -- c\n orders"), ["orders"]);
});

test("subqueries are scanned for inner tables", () => {
  const tables = extractTableNames("SELECT * FROM (SELECT * FROM kill_switches)");
  assert.deepEqual(tables, ["kill_switches"]);
});

test("intern is denied quoted access to non-whitelisted tables", () => {
  const check = checkTableAccess("intern", 'SELECT * FROM "kill_switches"');
  assert.equal(check.allowed, false);
  assert.deepEqual(check.denied, ["kill_switches"]);
});

test("intern is allowed canonical_* and features", () => {
  assert.equal(checkTableAccess("intern", "SELECT * FROM canonical_equity_ohlcv").allowed, true);
  assert.equal(checkTableAccess("intern", 'SELECT * FROM "features"').allowed, true);
});

test("unparseable table reference is denied, not silently allowed", () => {
  const extraction = extractTableReferences('SELECT * FROM `orders`');
  assert.equal(extraction.unparseable, true);
  const check = checkTableAccess("managing_partner", 'SELECT * FROM `orders`');
  // managing_partner has "*" but an unparseable reference is still denied
  assert.equal(check.allowed, false);
});

test("function-only queries with no FROM are allowed", () => {
  assert.equal(checkTableAccess("intern", "SELECT 1").allowed, true);
});

test("researcher can read audit_trail; intern cannot", () => {
  assert.equal(checkTableAccess("researcher", "SELECT * FROM audit_trail").allowed, true);
  assert.equal(checkTableAccess("intern", "SELECT * FROM audit_trail").allowed, false);
});

test("risk_officer raw_* prefix glob works with quoted identifiers", () => {
  assert.equal(
    checkTableAccess("risk_officer", 'SELECT * FROM "raw_alpaca_equity_ohlcv"').allowed,
    true,
  );
  assert.equal(
    checkTableAccess("researcher", 'SELECT * FROM "raw_alpaca_equity_ohlcv"').allowed,
    false,
  );
});

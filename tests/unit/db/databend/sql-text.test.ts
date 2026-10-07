import { describe, expect, test } from "bun:test";
import {
  DATABEND_FORM_FEED,
  DATABEND_HINT_SEMICOLON,
  DATABEND_HINT_TOKEN,
  DATABEND_MULTIPLE_STATEMENTS,
  DATABEND_NO_STATEMENT,
  DATABEND_STAGE_BACKSLASH,
  DATABEND_UNTERMINATED_SPAN,
  databendStatementRefusal,
} from "@/lib/db/providers/sql/databend/sql-text";
import { applyQueryLimit } from "@/lib/db/utils/query-limiter";

/**
 * The statement guard of design 5.3, which runs before any request. Each refusal is one exported sentence that names
 * the reason and never echoes the text.
 */
describe("databendStatementRefusal", () => {
  test.each<[string, string, string]>([
    ["two statements", "SELECT 1; SELECT 2", DATABEND_MULTIPLE_STATEMENTS],
    [
      "an INSERT followed by a DELETE, the escaped quote read as Databend reads it",
      "INSERT INTO t VALUES ('a\\'b'); DELETE FROM t",
      DATABEND_MULTIPLE_STATEMENTS,
    ],
    ["no statement", "-- only a note\n/* and another */", DATABEND_NO_STATEMENT],
    ["empty text", "   ", DATABEND_NO_STATEMENT],
    ["an unterminated literal", "SELECT 'abc", DATABEND_UNTERMINATED_SPAN],
    ["an unterminated block comment", "SELECT 1 /* note", DATABEND_UNTERMINATED_SPAN],
    // I13: Databend's `\\.` does not match a line feed, so the literal does not close there.
    ["a backslash before a line feed inside a literal", "SELECT 'a\\\nb'", DATABEND_UNTERMINATED_SPAN],
    // 11 #7: Databend ends a `--` comment at a form feed, Studio's reader does not.
    ["a form feed that ends a comment in Databend", "-- x\fDROP TABLE t", DATABEND_FORM_FEED],
    // I11: Databend's stage token takes `\'` into the name, so the `;` after it is code there.
    ["a stage name holding a backslash", "SELECT * FROM @s\\'; DROP TABLE t; --'", DATABEND_STAGE_BACKSLASH],
    // I12: a hint body is tokenized by Databend, so a `;` in it is code there.
    ["an optimizer hint holding a semicolon", "SELECT /*+ SET_VAR(a=1); DROP TABLE t */ 1", DATABEND_HINT_SEMICOLON],
    // D6-1: a token in the hint body that runs past Studio's first `*/` moves Databend's hint end to a later one.
    // Each text ran `SELECT 2 AS hidden` on the pinned image.
    [
      "a hint whose quote runs past the */ Studio reads as its end",
      "/*+ ' */ SELECT 1 AS shown -- ' */ SELECT 2 AS hidden",
      DATABEND_HINT_TOKEN,
    ],
    ["a hint holding a -- comment", "/*+ -- */ SELECT 1 AS shown\n*/ SELECT 2 AS hidden", DATABEND_HINT_TOKEN],
    ["a hint holding a nested block comment", "/*+ /* */ SELECT 1 AS shown */ SELECT 2 AS hidden", DATABEND_HINT_TOKEN],
  ])("refuses %s", (_, sql, sentence) => {
    expect(databendStatementRefusal(sql)).toBe(sentence);
  });

  test.each<[string, string]>([
    ["a literal holding an escaped quote", "SELECT 'it\\'s'"],
    ["one statement and its terminator", "SELECT 1;"],
    ["one statement and a trailing comment", "SELECT 1; -- note"],
    ["a $$ script", "EXECUTE IMMEDIATE $$ BEGIN LET x := 1; RETURN x; END; $$"],
    ["a stage name with no backslash", "SELECT * FROM @my_stage/data.csv"],
    ["a backslash inside a literal after a stage name", "SELECT * FROM @s WHERE a = 'x\\'y'"],
    ["an @ inside a literal", "SELECT '@s\\\\x'"],
    ["an optimizer hint with no semicolon", "SELECT /*+ SET_VAR(max_threads=1) */ 1"],
    ["an optimizer hint holding a plain quoted value", "SELECT /*+ SET_VAR(timezone='Asia/Shanghai') */ 1"],
    ["a plain comment holding a semicolon", "SELECT /* a; b */ 1"],
  ])("passes %s", (_, sql) => {
    expect(databendStatementRefusal(sql)).toBeNull();
  });

  test("no sentence echoes the text", () => {
    const sql = "SELECT 'secret_marker'; SELECT 2";

    expect(databendStatementRefusal(sql)).not.toContain("secret_marker");
  });
});

/** The shared limiter under the Databend grammar, as the provider calls it (L3). */
describe("the shared limiter under databend", () => {
  test("appends no bound to a top-level SELECT TOP n, which Databend refuses beside a LIMIT (I4)", () => {
    const sql = "SELECT TOP 3 * FROM numbers(10)";

    expect(applyQueryLimit(sql, 500, 0, {}, "databend")).toMatchObject({ sql, wasLimited: false });
    expect(applyQueryLimit(sql, 500, 500, {}, "databend")).toMatchObject({ sql, wasLimited: false });
  });

  test("places the bound before a trailing FORMAT clause (I3)", () => {
    expect(applyQueryLimit("SELECT number FROM numbers(10) FORMAT TabSeparated", 5, 0, {}, "databend").sql).toBe(
      "SELECT number FROM numbers(10) LIMIT 5 FORMAT TabSeparated",
    );
    expect(applyQueryLimit("SELECT 1 FORMAT JSON;", 5, 10, {}, "databend").sql).toBe(
      "SELECT 1 LIMIT 5 OFFSET 10 FORMAT JSON;",
    );
  });

  test("reads an existing bound before a trailing FORMAT clause as the bound it is", () => {
    const sql = "SELECT 1 LIMIT 5 FORMAT CSV";

    expect(applyQueryLimit(sql, 500, 0, {}, "databend")).toMatchObject({ sql, wasLimited: false, originalLimit: 5 });
  });
});

import { describe, test, expect } from "bun:test";
import { databendTextStrategy } from "@/lib/explain/databend-text";
import type { ExplainMode, ExplainTreeNode } from "@/lib/explain/types";

const MODES: ExplainMode[] = ["estimate", "analyze"];

/**
 * The names whose mere presence in the code declines both modes: the two binder
 * constructs that execute under plain EXPLAIN, the sequence function, the six table
 * functions that write from a SELECT shape (design 5.6 and 5.7, triage X06), and the
 * four more that a release build registers and that act when read: the two that panic
 * the query on purpose, and the two task functions that cancel task runs or enable
 * dependent tasks.
 */
const DECLINED_NAMES = [
  "MATERIALIZED",
  "PIVOT",
  "nextval",
  "fuse_amend",
  "set_cache_capacity",
  "fuse_vacuum2",
  "fuse_vacuum_temporary_table",
  "fuse_vacuum_drop_aggregating_index",
  "fuse_vacuum_drop_inverted_index",
  "sync_crash_me",
  "async_crash_me",
  "user_task_cancel_ongoing_executions",
  "task_dependents_enable",
];

// Databend v1.2.951-nightly, the local fixture, 2026-10-08, as studio_reader over
// POST /v1/query: `EXPLAIN SELECT number % 3 AS k, count(*) FROM numbers(100) WHERE
// number > 10 GROUP BY k`. One column `explain`, one row per plan line; the M24a shape
// (`AggregateFinal` over `├── output columns: ...`) with the full plan below it.
const GROUP_BY_LINES = [
  "EvalScalar",
  "├── output columns: [COUNT(*) (#2), k (#3)]",
  "├── expressions: [group_item (#1)]",
  "├── estimated rows: 16.67",
  "└── AggregateFinal",
  "    ├── output columns: [COUNT(*) (#2), k (#1)]",
  "    ├── group by: [k]",
  "    ├── aggregate functions: [count()]",
  "    ├── estimated rows: 16.67",
  "    └── AggregatePartial",
  "        ├── group by: [k]",
  "        ├── aggregate functions: [count()]",
  "        ├── estimated rows: 16.67",
  "        └── EvalScalar",
  "            ├── output columns: [k (#1)]",
  "            ├── expressions: [numbers.number (#0) % 3]",
  "            ├── estimated rows: 50.00",
  "            └── Filter",
  "                ├── output columns: [numbers.number (#0)]",
  "                ├── filters: [numbers.number (#0) > 10]",
  "                ├── estimated rows: 50.00",
  "                └── TableScan",
  "                    ├── table: default.system.numbers",
  "                    ├── scan id: 0",
  "                    ├── output columns: [number (#0)]",
  "                    ├── read rows: 100",
  "                    ├── read size: < 1 KiB",
  "                    ├── partitions total: 1",
  "                    ├── partitions scanned: 1",
  "                    ├── push downs: [filters: [numbers.number (#0) > 10], limit: NONE]",
  "                    └── estimated rows: 100.00",
];

// Same fixture and day: `EXPLAIN SELECT a.number FROM numbers(10) a JOIN numbers(5) b ON
// a.number = b.number UNION ALL SELECT 1`, cut to the join's first child. A property can
// carry lines of its own (`build join filters:`), and a node label can carry a
// parenthesised role (`TableScan(Build)`).
const JOIN_LINES = [
  "UnionAll",
  "├── output columns: [number (#3)]",
  "├── estimated rows: 51.00",
  "├── HashJoin",
  "│   ├── join type: INNER",
  "│   ├── build join filters:",
  "│   │   └── filter id:0, build key:b.number (#1), probe targets:[a.number (#0)@scan0], filter type:bloom,inlist,min_max",
  "│   ├── estimated rows: 50.00",
  "│   └── TableScan(Build)",
  "│       ├── table: default.system.numbers",
  "│       └── estimated rows: 5.00",
  "└── EvalScalar",
  "    ├── expressions: [1]",
  "    └── DummyTableScan",
];

const rowsOf = (lines: string[]) => lines.map((explain) => ({ explain }));

function renderTree(lines: string[]): { root: ExplainTreeNode; raw: unknown } {
  const model = databendTextStrategy.toRenderModel(rowsOf(lines));
  if (model?.kind !== "tree") throw new Error("expected a tree");
  return { root: model.root, raw: model.raw };
}

describe("databendTextStrategy.buildSql", () => {
  test("format id", () => {
    expect(databendTextStrategy.format).toBe("databend-text");
  });

  // C22: ANALYZE drains the pipeline, so neither mode may ask for it.
  test.each(MODES)("%s builds plain EXPLAIN for SELECT 1, never ANALYZE", (mode) => {
    const built = databendTextStrategy.buildSql("SELECT 1", mode);
    expect(built).toBe("EXPLAIN SELECT 1");
    expect(built).not.toMatch(/\bANALY[SZ]E\b/i);
  });

  test.each(MODES)("%s plans a CTE whose body sits at depth 1", (mode) => {
    const sql = "WITH a AS (SELECT 1) SELECT * FROM a";
    expect(databendTextStrategy.buildSql(sql, mode)).toBe(`EXPLAIN ${sql}`);
  });

  test.each(MODES)("%s declines a statement that is not a SELECT, and a comment on its own", (mode) => {
    expect(databendTextStrategy.buildSql("INSERT INTO t VALUES (1)", mode)).toBeNull();
    expect(databendTextStrategy.buildSql("CALL system$set_cache_capacity('x', '1')", mode)).toBeNull();
    expect(databendTextStrategy.buildSql("-- SELECT 1", mode)).toBeNull();
  });

  for (const mode of MODES) {
    test.each(DECLINED_NAMES)(`${mode} declines %s as a word, backtick-quoted and double-quoted`, (name) => {
      expect(databendTextStrategy.buildSql(`SELECT * FROM ${name}()`, mode)).toBeNull();
      expect(databendTextStrategy.buildSql(`SELECT * FROM ${name.toLowerCase()}()`, mode)).toBeNull();
      expect(databendTextStrategy.buildSql(`SELECT * FROM \`${name}\`()`, mode)).toBeNull();
      expect(databendTextStrategy.buildSql(`SELECT * FROM "${name}"()`, mode)).toBeNull();
    });
  }

  test.each(MODES)("%s declines a writer nested in a table-function argument subquery", (mode) => {
    expect(
      databendTextStrategy.buildSql("SELECT * FROM numbers((SELECT count(*) FROM fuse_vacuum2()))", mode),
    ).toBeNull();
    expect(databendTextStrategy.buildSql("SELECT * FROM numbers(( /* c */ SELECT nextval(s)))", mode)).toBeNull();
  });

  test.each(MODES)("%s declines the MATERIALIZED CTE and PIVOT in their real spellings", (mode) => {
    expect(databendTextStrategy.buildSql("WITH t AS MATERIALIZED (SELECT 1 AS a) SELECT * FROM t", mode)).toBeNull();
    expect(databendTextStrategy.buildSql("SELECT * FROM t PIVOT(sum(a) FOR b IN (SELECT b FROM u))", mode)).toBeNull();
  });

  // A subscript is the statement's own code, so a name inside one is still read.
  test.each(MODES)("%s reads a name inside an array literal", (mode) => {
    expect(databendTextStrategy.buildSql("SELECT [nextval(s)]", mode)).toBeNull();
  });

  test.each(MODES)("%s plans a declined name written inside a string or a comment", (mode) => {
    for (const sql of [
      "SELECT 'fuse_vacuum2()' AS note",
      "SELECT 1 -- nextval(s)\n",
      "SELECT /* PIVOT MATERIALIZED */ 1",
      "SELECT 'it\\'s nextval(s)' AS note",
    ]) {
      expect(databendTextStrategy.buildSql(sql, mode)).toBe(`EXPLAIN ${sql}`);
    }
  });

  test.each(MODES)("%s plans a longer word that only starts with a declined name", (mode) => {
    const sql = "SELECT nextval_count, pivoted FROM t";
    expect(databendTextStrategy.buildSql(sql, mode)).toBe(`EXPLAIN ${sql}`);
  });

  test.each(MODES)("%s declines text with an unterminated span", (mode) => {
    expect(databendTextStrategy.buildSql("SELECT 'abc", mode)).toBeNull();
    expect(databendTextStrategy.buildSql("SELECT 1 /* open", mode)).toBeNull();
    expect(databendTextStrategy.buildSql('SELECT "open', mode)).toBeNull();
  });

  // D9-1, measured on v1.2.951: `$a$` is a variable to Databend, so what sits between two
  // tags is code there, and an argument subquery between them ran under plain EXPLAIN.
  test.each(MODES)("%s declines a tagged dollar run, which Databend reads as code", (mode) => {
    expect(
      databendTextStrategy.buildSql(
        "SELECT $a$ AS c, number FROM numbers((SELECT nextval(s))) WHERE number > $a$",
        mode,
      ),
    ).toBeNull();
    expect(databendTextStrategy.buildSql("SELECT $a$ AS c FROM fuse_vacuum2() t, (SELECT $a$) u", mode)).toBeNull();
  });

  // D9-2: the three readings the provider guard refuses, where Databend sees code the
  // span reader reads as a comment or a literal, so the screen declines them itself.
  test.each(MODES)("%s declines a form feed, a stage backslash and an optimizer hint", (mode) => {
    expect(databendTextStrategy.buildSql("SELECT * FROM t --\f, numbers((SELECT nextval(s)))", mode)).toBeNull();
    expect(databendTextStrategy.buildSql("SELECT * FROM @s\\' , numbers((SELECT nextval(s))) --'", mode)).toBeNull();
    expect(
      databendTextStrategy.buildSql("SELECT /*+ ' */ 1 -- ' */ FROM numbers((SELECT nextval(s)))", mode),
    ).toBeNull();
    expect(databendTextStrategy.buildSql("SELECT $$it$$ AS a FROM @stage", mode)).toBe(
      "EXPLAIN SELECT $$it$$ AS a FROM @stage",
    );
  });

  // X06, X29: a subquery opener at depth 2 or more is an argument subquery, which binds
  // by executing, so the background estimate declines it.
  test.each([
    "SELECT * FROM numbers(( /* c */ SELECT 1))",
    "SELECT * FROM numbers((FROM t))",
    "SELECT * FROM numbers((VALUES (1)))",
    "SELECT * FROM numbers((WITH a AS (SELECT 1) SELECT * FROM a))",
    "SELECT * FROM numbers((\n-- why\nselect 1))",
  ])("estimate declines the argument subquery in %s", (sql) => {
    expect(databendTextStrategy.buildSql(sql, "estimate")).toBeNull();
  });

  // The button accepts it: the subquery runs once while planning, as it does on Run.
  test("analyze accepts an argument subquery that names no writer", () => {
    const sql = "SELECT * FROM numbers((SELECT 1))";
    expect(databendTextStrategy.buildSql(sql, "analyze")).toBe(`EXPLAIN ${sql}`);
  });

  test.each([
    "SELECT * FROM (SELECT 1) t",
    "SELECT * FROM t WHERE a IN (SELECT 1)",
    "SELECT * FROM t WHERE EXISTS (SELECT 1)",
    "SELECT * FROM numbers(3) a, (SELECT 1) b",
    "SELECT * FROM numbers((1))",
    "SELECT * FROM numbers((x))",
    "SELECT (",
  ])("estimate still plans %s", (sql) => {
    expect(databendTextStrategy.buildSql(sql, "estimate")).toBe(`EXPLAIN ${sql}`);
  });
});

describe("databendTextStrategy.extractPlan", () => {
  test("stores the rows verbatim and an empty array when there are none", () => {
    const rows = rowsOf(["EvalScalar"]);
    expect(databendTextStrategy.extractPlan({ rows })).toBe(rows);
    expect(databendTextStrategy.extractPlan({})).toEqual([]);
  });
});

describe("databendTextStrategy.toRenderModel", () => {
  test("rejects foreign shapes", () => {
    expect(databendTextStrategy.toRenderModel(null)).toBeNull();
    expect(databendTextStrategy.toRenderModel([])).toBeNull();
    expect(databendTextStrategy.toRenderModel(["EvalScalar"])).toBeNull();
    expect(databendTextStrategy.toRenderModel(rowsOf(["", "   "]))).toBeNull();
  });

  test("the measured GROUP BY plan nests nodes by indentation with properties as detail", () => {
    const { root } = renderTree(GROUP_BY_LINES);
    expect(root.label).toBe("EvalScalar");
    expect(root.detail).toBe("output columns: [COUNT(*) (#2), k (#3)]; expressions: [group_item (#1)]");
    expect(root.metrics).toEqual({ estRows: 16.67 });

    const labels: string[] = [];
    let node: ExplainTreeNode | undefined = root;
    while (node !== undefined) {
      labels.push(node.label);
      expect(node.children.length).toBeLessThanOrEqual(1);
      node = node.children[0];
    }
    expect(labels).toEqual(["EvalScalar", "AggregateFinal", "AggregatePartial", "EvalScalar", "Filter", "TableScan"]);

    const scan = root.children[0].children[0].children[0].children[0].children[0];
    expect(scan.metrics).toEqual({ estRows: 100 });
    expect(scan.detail).toContain("table: default.system.numbers");
    expect(scan.detail).toContain("push downs: [filters: [numbers.number (#0) > 10], limit: NONE]");
    expect(scan.detail).not.toContain("estimated rows");
  });

  test("the raw tab shows the plan as Databend printed it", () => {
    expect(renderTree(GROUP_BY_LINES).raw).toBe(GROUP_BY_LINES.join("\n"));
  });

  test("a property's own lines belong to its node, and a parenthesised role stays in the label", () => {
    const { root } = renderTree(JOIN_LINES);
    expect(root.label).toBe("UnionAll");
    expect(root.children.map((child) => child.label)).toEqual(["HashJoin", "EvalScalar"]);

    const join = root.children[0];
    expect(join.metrics).toEqual({ estRows: 50 });
    expect(join.detail).toContain("build join filters:");
    expect(join.detail).toContain("filter id:0, build key:b.number (#1)");
    expect(join.children.map((child) => child.label)).toEqual(["TableScan(Build)"]);
    expect(join.children[0].metrics).toEqual({ estRows: 5 });

    const scalar = root.children[1];
    expect(scalar.metrics).toBeUndefined();
    expect(scalar.children).toEqual([{ label: "DummyTableScan", children: [] }]);
  });

  test("a property line with no node above it becomes a node, so nothing printed is lost", () => {
    const { root } = renderTree(["Fragment 0:", "└── EvalScalar"]);
    expect(root.label).toBe("Fragment 0:");
    expect(root.children.map((child) => child.label)).toEqual(["EvalScalar"]);
  });

  test("several top-level nodes sit under a synthetic EXPLAIN root", () => {
    const { root } = renderTree(["EvalScalar", "Filter"]);
    expect(root.label).toBe("EXPLAIN");
    expect(root.children.map((child) => child.label)).toEqual(["EvalScalar", "Filter"]);
  });

  test("an estimate that is not a number stays as detail and fabricates no metric", () => {
    const { root } = renderTree(["EvalScalar", "├── estimated rows: unknown", "└── estimated rows: "]);
    expect(root.metrics).toBeUndefined();
    expect(root.detail).toBe("estimated rows: unknown; estimated rows:");
  });

  test("reads the first column whatever it is called, and skips blank and null cells", () => {
    const model = databendTextStrategy.toRenderModel([{ plan: "EvalScalar" }, { plan: null }, { plan: "" }]);
    expect(model).toEqual({ kind: "tree", root: { label: "EvalScalar", children: [] }, raw: "EvalScalar" });
  });
});

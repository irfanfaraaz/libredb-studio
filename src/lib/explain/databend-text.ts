import { resolveSqlGrammar } from "@/lib/sql/grammar";
import { readSqlSpan } from "@/lib/sql/spans";
import { readSqlWord } from "@/lib/sql/words";
import { classifySelectPrefix } from "./select-prefix";
import { INDENT, buildTree, cellText, isRecord, withRoot, type PlanLine } from "./text-plan";
import type { ExplainMode, ExplainPlanInput, ExplainStrategy, ExplainTreeNode } from "./types";

/**
 * This strategy's dialect, resolved once. Reached only through
 * `explainFormat: "databend-text"`, which only the Databend provider declares, so the
 * module's identity IS the dialect - the same shape as a provider passing `this.type`.
 */
const DATABEND_GRAMMAR = resolveSqlGrammar("databend");

/**
 * The prefix, and the decision behind it.
 *
 * Databend's `EXPLAIN ANALYZE` drains the pipeline, so it runs the statement and bills a
 * full run, while plain `EXPLAIN` plans it. Both modes therefore emit the plain form: the
 * Explain button always asks for "analyze" and skips the confirmation gate, so honouring
 * the mode would run SELECT-shaped writers unconfirmed. Trino, ClickHouse, SQLite,
 * Couchbase and Druid ignore the mode the same way.
 *
 * Plain `EXPLAIN` is still not free of execution: the binder runs a subquery written in a
 * table-function argument, a PIVOT value subquery and a MATERIALIZED CTE while it plans
 * (measured on v1.2.951: `EXPLAIN SELECT * FROM numbers((SELECT nextval(s)))` moved the
 * sequence, as did the comment-led and `FROM`-first spellings, and a MATERIALIZED CTE
 * moved it once a client session was present, which Studio always sends). Derived tables
 * and `IN` or `EXISTS` subqueries bind without executing. `screen` below is what keeps
 * those executions off the Explain path.
 */
const EXPLAIN_PREFIX = "EXPLAIN ";

/**
 * Names whose presence anywhere in the code declines both modes, compared upper-cased.
 *
 * `MATERIALIZED` and `PIVOT` are the binder constructs that execute under plain EXPLAIN.
 * `NEXTVAL` takes a sequence value, and the other six are the table functions that write
 * from a SELECT shape (`table_function_factory.rs`): `FUSE_VACUUM2` vacuums tables, and
 * `SET_CACHE_CAPACITY` really moved a cache's capacity under
 * plain EXPLAIN when nested in an argument subquery. A name is matched as a word and as
 * a quoted identifier, so a writer nested in an argument subquery never runs on an
 * Explain click.
 */
const DECLINED_NAMES = new Set([
  "MATERIALIZED",
  "PIVOT",
  "NEXTVAL",
  "FUSE_AMEND",
  "SET_CACHE_CAPACITY",
  "FUSE_VACUUM2",
  "FUSE_VACUUM_TEMPORARY_TABLE",
  "FUSE_VACUUM_DROP_AGGREGATING_INDEX",
  "FUSE_VACUUM_DROP_INVERTED_INDEX",
]);

/**
 * The words that open a query, and so a subquery after `(`. Databend accepts a
 * parenthesised `FROM`-first query and a parenthesised `VALUES` list besides `SELECT`
 * and `WITH` (`query.rs`), which is why a raw-text "parenthesised SELECT" check misses
 * argument subqueries.
 */
const QUERY_OPENERS = new Set(["SELECT", "WITH", "FROM", "VALUES"]);

/**
 * The parenthesis depth, counting the opener itself, from which a subquery is an
 * argument subquery: a table function's argument always sits inside the function's own
 * parentheses, while a derived table and an `IN` or `EXISTS` subquery open at depth 1.
 * A depth-1 subquery nested inside another parenthesis is declined too, which costs the
 * estimate a plan rather than running anything; the decline rate over the generated
 * queries is recorded in the design's probe results.
 */
const ARGUMENT_SUBQUERY_DEPTH = 2;

/**
 * Databend's only dollar literal. A tagged run (`$a$ ... $a$`) is a dollar string to the
 * span reader, but Databend lexes `$a$` as a variable and reads what lies between two of
 * them as code (measured on v1.2.951: an argument subquery between two tags ran under
 * plain EXPLAIN), so a tagged run declines.
 */
const DOLLAR_LITERAL_OPENER = "$$";

/**
 * Three readings where Databend sees code that the span reader takes for trivia or a
 * literal, all of which the provider's statement guard refuses on Run; an explain
 * strategy does not import a provider, so the screen declines them itself. A form feed
 * ends a `--` comment in Databend, a `/*+` block is an optimizer hint whose body
 * Databend tokenizes, and a stage token (`@name`) takes a backslash and the quote after
 * it into the name, up to the first of these ending characters.
 */
const FORM_FEED = "\f";
const HINT_OPENER = "/*+";
const STAGE_END = /[\s,`;'"()]/;

/** Whether the stage token whose `@` is at `index` holds a backslash before it ends. */
function stageHoldsBackslash(sql: string, index: number): boolean {
  for (let i = index + 1; i < sql.length && !STAGE_END.test(sql[i]); i++) {
    if (sql[i] === "\\") return true;
  }
  return false;
}

/** The spans the walk skips without reading: they are not the statement's own code. */
const TRIVIA = new Set(["whitespace", "line-comment", "block-comment"]);

/** The first code word at or after `index`, comments and whitespace skipped, or `null`. */
function nextCodeWord(sql: string, index: number): string | null {
  let i = index;
  let span = readSqlSpan(sql, i, DATABEND_GRAMMAR);
  while (span !== null && TRIVIA.has(span.kind)) {
    i = span.end;
    span = readSqlSpan(sql, i, DATABEND_GRAMMAR);
  }
  return readSqlWord(sql, i)?.text ?? null;
}

/**
 * Whether Explain may send this statement in this mode, walking code only: strings and
 * comments are skipped, a quoted identifier is read as the name it is, and an array
 * subscript is code. Text with a run that never closes declines, because what follows
 * the run cannot be read.
 */
function screen(sql: string, mode: ExplainMode): boolean {
  if (sql.includes(FORM_FEED)) return false;
  let depth = 0;
  let i = 0;

  while (i < sql.length) {
    const span = readSqlSpan(sql, i, DATABEND_GRAMMAR);
    if (span !== null && span.kind !== "subscript") {
      if (!span.terminated) return false;
      if (span.kind === "dollar-string" && !sql.startsWith(DOLLAR_LITERAL_OPENER, i)) return false;
      if (span.kind === "block-comment" && sql.startsWith(HINT_OPENER, i)) return false;
      if (span.kind === "quoted-identifier" && DECLINED_NAMES.has(sql.slice(i + 1, span.end - 1).toUpperCase())) {
        return false;
      }
      i = span.end;
      continue;
    }

    const word = readSqlWord(sql, i);
    if (word !== null) {
      if (DECLINED_NAMES.has(word.text)) return false;
      i = word.end;
      continue;
    }

    if (sql[i] === "@" && stageHoldsBackslash(sql, i)) return false;
    if (sql[i] === "(") {
      depth++;
      const opener = nextCodeWord(sql, i + 1);
      if (mode === "estimate" && depth >= ARGUMENT_SUBQUERY_DEPTH && opener !== null && QUERY_OPENERS.has(opener)) {
        return false;
      }
    } else if (sql[i] === ")") {
      depth--;
    }
    i++;
  }

  return true;
}

/** The property Databend prints its row estimate under; it becomes the node's metric. */
const EST_ROWS_KEY = "estimated rows";

/**
 * A plan node's line: an operator name, optionally with a parenthesised role
 * (`TableScan(Build)`). Every other line is a property of the node above it
 * (`output columns: [...]`, `build join filters:` and the lines under it).
 */
const NODE_LABEL = /^[A-Z]\w*(?:\(.*\))?$/;

/** A `key: value` property, split once at the first colon. */
const PROPERTY = /^([^:]+):\s*(.*)$/;

interface PendingNode {
  line: PlanLine;
  details: string[];
}

/**
 * The row estimate, or nothing. An empty or non-numeric value is an absent reading,
 * not a zero: `Number("")` is 0, and a badge nobody measured is the fabrication the
 * absence rule (#477) exists to prevent.
 */
function readEstRows(label: string): number | undefined {
  const match = PROPERTY.exec(label);
  if (match === null || match[1].trim().toLowerCase() !== EST_ROWS_KEY || match[2] === "") return undefined;
  const parsed = Number(match[2]);
  return Number.isFinite(parsed) ? parsed : undefined;
}

/**
 * Databend prints a plan as one row per line in its one `explain` column, a node's
 * properties drawn as children of the node with the same glyphs as its child nodes. So
 * a node line opens a tree node, and a property line becomes detail on the nearest node
 * above it that is less indented, its estimate a metric. A property with no node above
 * it becomes a node itself, so nothing printed is lost.
 */
function toPlanLines(texts: readonly string[]): PlanLine[] {
  const pending: PendingNode[] = [];
  const open: PendingNode[] = [];

  for (const text of texts) {
    const unindented = text.replace(INDENT, "");
    const label = unindented.trim();
    const indent = text.length - unindented.length;

    while (open.length > 0 && open[open.length - 1].line.indent >= indent) open.pop();
    const owner = open[open.length - 1];
    if (owner !== undefined && !NODE_LABEL.test(label)) {
      const estRows = readEstRows(label);
      if (estRows === undefined) owner.details.push(label);
      else owner.line.node.metrics = { estRows };
      continue;
    }

    const node: ExplainTreeNode = { label, children: [] };
    const entry: PendingNode = { line: { indent, text, node }, details: [] };
    pending.push(entry);
    open.push(entry);
  }

  return pending.map(({ line, details }) => {
    if (details.length > 0) line.node.detail = details.join("; ");
    return line;
  });
}

export const databendTextStrategy: ExplainStrategy = {
  format: "databend-text",
  buildSql(sql, mode) {
    if (classifySelectPrefix(sql, DATABEND_GRAMMAR) === null) return null;
    if (!screen(sql, mode)) return null;
    return `${EXPLAIN_PREFIX}${sql}`;
  },
  // No parsing here: the rows ARE the plan, one line per row.
  extractPlan(result) {
    return result.rows ?? [];
  },
  toRenderModel(raw): ExplainPlanInput | null {
    if (!Array.isArray(raw) || raw.length === 0 || !raw.every(isRecord)) return null;
    // Shape-driven, as mysql-text: the first column is the plan text whatever it is
    // called, and a blank line would be an empty box in the tree.
    const texts = raw.map((row) => cellText(Object.values(row)[0])).filter((text) => text.trim() !== "");
    if (texts.length === 0) return null;
    // The raw tab shows the plan as Databend printed it, every line verbatim.
    return { kind: "tree", root: withRoot(buildTree(toPlanLines(texts))), raw: texts.join("\n") };
  },
};

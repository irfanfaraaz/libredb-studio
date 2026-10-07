/**
 * The Databend statement guard (design 5.3), which runs before any request.
 *
 * Pure: it reads the text under the Databend grammar row and answers one refusal sentence, or `null` when the text is
 * one statement Studio and Databend read the same way. No sentence echoes the text.
 *
 * Databend's `/v1/query` takes one statement per request, and when the first of several is an INSERT or REPLACE it
 * drops the rest without an error, so more than one code statement is refused, and so is none. Every other refusal is
 * a place where Databend's lexer ends a construct somewhere the span reader does not, so text that Studio reads as a
 * comment or literal is code to the server, and the confirmation gate would have read a different statement than the
 * one that runs:
 *
 * - a run that never closes, which hides whatever is written inside it;
 * - a form feed, which ends a `--` comment in Databend (`--[^\n\f]*`) and not in the span reader;
 * - a code-level `@` stage token holding a backslash: `@([^\s,`;'"()]|\\\s|\\'|\\"|\\\\)+` takes `\'` into the name,
 *   so `@s\'; DROP TABLE t; --'` is a stage, a `;` and a DROP there and one statement here;
 * - a `/*+` hint holding a `;`: Databend tokenizes a hint body where the span reader sees a block comment, and no hint
 *   needs a `;`;
 * - a `/*+` hint holding a token that can run past the closing star-slash the span reader stops at: wherever the
 *   prefix stands, Databend ends the hint at the first closing star-slash TOKEN, so a quote, a comment, a `$$` string,
 *   a stage name or a `~*` that takes it in moves the end to a later one, and no `;` is needed to hide a statement.
 *
 * The checks that say Studio's reading is not Databend's come before the count, which is only meaningful once the two
 * readings agree.
 */

import { resolveSqlGrammar } from "@/lib/sql/grammar";
import { hasUnterminatedSpan, readSqlSpan } from "@/lib/sql/spans";
import { countCodeStatements } from "@/lib/sql/statement-splitter";

export const DATABEND_MULTIPLE_STATEMENTS =
  "Databend runs one statement per request, and when the first is an INSERT or REPLACE it drops the rest without an error. Run the statements one at a time, or use Run All.";

export const DATABEND_NO_STATEMENT = "There is no statement to run: the text holds only comments.";

export const DATABEND_UNTERMINATED_SPAN =
  "A quote or comment in this text never closes, so Studio cannot tell where the statement ends.";

export const DATABEND_FORM_FEED =
  "This text holds a form feed, which ends a -- comment in Databend but not in Studio's reading. Remove it and run again.";

export const DATABEND_STAGE_BACKSLASH =
  "A stage name (@...) in this text holds a backslash, which Databend reads as part of the name together with the quote after it, so Studio cannot tell where the statement ends. Remove the backslash and run again.";

export const DATABEND_HINT_SEMICOLON =
  "An optimizer hint (/*+ ... */) in this text holds a semicolon, which Databend reads as code and Studio as a comment. Remove the semicolon from the hint and run again.";

export const DATABEND_HINT_TOKEN =
  "An optimizer hint (/*+ ... */) in this text holds a character that can make Databend end the hint at a later */ than Studio does, so Studio cannot tell which statement runs. Keep the hint to names, numbers and plain quoted values, and run again.";

const GRAMMAR = resolveSqlGrammar("databend");

/** The characters that end a stage token, from the lexer's `[^\s,`;'"()]`. */
const STAGE_END = /[\s,`;'"()]/;

/** Whether the stage token whose `@` is at `index` holds a backslash before its first ending character. */
function stageHoldsBackslash(sql: string, index: number): boolean {
  for (let i = index + 1; i < sql.length && !STAGE_END.test(sql[i]); i++) {
    if (sql[i] === "\\") return true;
  }
  return false;
}

/** A quoted value with no backslash, which Databend's string token and a plain pairing of quotes end at the same place. */
const HINT_PLAIN_LITERAL = /'[^'\\]*'/g;

/**
 * What can start a token that runs past the star-slash the span reader ends a hint at, once plain quoted values are gone:
 * any other quote or backslash, `$` (a `$$` string or a name that runs into one), a stage name, `~` (`~*` and `!~*`
 * take the `*`) and the two comment forms.
 */
const HINT_RUN_ON = /['"`\\$@~]|--|\/\*/;

/** The first code-level construct Databend's lexer ends somewhere the span reader does not, as its refusal. */
function lexerDisagreement(sql: string): string | null {
  let i = 0;

  while (i < sql.length) {
    const span = readSqlSpan(sql, i, GRAMMAR);
    if (span === null) {
      if (sql[i] === "@" && stageHoldsBackslash(sql, i)) return DATABEND_STAGE_BACKSLASH;
      i++;
      continue;
    }
    if (span.kind === "block-comment" && sql.startsWith("/*+", i)) {
      if (sql.slice(i, span.end).includes(";")) return DATABEND_HINT_SEMICOLON;
      if (HINT_RUN_ON.test(sql.slice(i + 3, span.end - 2).replace(HINT_PLAIN_LITERAL, ""))) return DATABEND_HINT_TOKEN;
    }
    i = span.end;
  }

  return null;
}

/** The refusal for this statement text, or `null` when it may be sent. */
export function databendStatementRefusal(sql: string): string | null {
  if (sql.includes("\f")) return DATABEND_FORM_FEED;
  if (hasUnterminatedSpan(sql, GRAMMAR)) return DATABEND_UNTERMINATED_SPAN;

  const disagreement = lexerDisagreement(sql);
  if (disagreement !== null) return disagreement;

  const count = countCodeStatements(sql, GRAMMAR);
  if (count === 0) return DATABEND_NO_STATEMENT;
  if (count > 1) return DATABEND_MULTIPLE_STATEMENTS;
  return null;
}

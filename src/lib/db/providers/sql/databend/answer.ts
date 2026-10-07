/**
 * Reading one HTTP answer of Databend's query API (design 3.12, section 4).
 *
 * Only a 200 `application/json` body is an answer. Anything else is a refusal, read by its status, then its content
 * type: a JSON refusal names Databend's code (`{"error":{"code","message"}}`), the Cloud gateway's kind
 * (`{"kind","message"}`) or databend-go's string shape (`{"error","message"}`), and a text one (a panic is 500
 * `text/plain`, 07 M04e) keeps its body as its text. A 200 that is not JSON, a body that does not parse (a
 * `RangeError` included), a key named `__proto__` anywhere, a field of the wrong type, a cell that is not text or
 * null, or a row of another width than the schema is `protocol`.
 *
 * Nothing here classifies a refusal or scrubs its text: `errors.ts` does both, with the connection's secret forms.
 * Pure: no I/O.
 */
import type { NodeResponse } from "@/lib/db/http/node-transport";
import { DATABEND_PROTOCOL_FAULTS, protocolError } from "./errors";
import type { DatabendAffect, DatabendCell, DatabendColumn, DatabendNotice } from "./transport";

/** An answer's in-body `error`: the statement failed, over HTTP 200. */
export interface DatabendAnswerError {
  readonly code: number;
  /** Raw server text, not yet through `serverText`. */
  readonly message: string;
  readonly detail: string | null;
}

/** The session the answer echoed, which design 3.4 and 3.7 read and the ROLLBACK sends back verbatim. */
export interface DatabendSessionEcho {
  /** The echoed object as it arrived, never stored, logged or returned past the run (design 3.4). */
  readonly raw: Readonly<Record<string, unknown>>;
  readonly txnState: string | null;
  readonly needKeepAlive: boolean;
  readonly role: string | null;
  readonly settings: Readonly<Record<string, string>>;
}

/** One 200 JSON answer, typed. Links are as received: `routes.ts` decides whether one is followed (design 3.9). */
export interface DatabendAnswer {
  /** Empty in a fail-to-start answer, where nothing ran (02 5.3). */
  readonly id: string;
  readonly sessionId: string | null;
  readonly nodeId: string | null;
  /** Display only: `nextUri` alone ends the loop (design 3.8). */
  readonly state: string;
  readonly error: DatabendAnswerError | null;
  /** Raw server text, not yet through `serverText`. */
  readonly warnings: readonly string[];
  readonly hasResultSet: boolean;
  readonly schema: readonly DatabendColumn[];
  readonly data: readonly (readonly DatabendCell[])[];
  readonly nextUri: string | null;
  readonly affect: DatabendAffect | null;
  readonly session: DatabendSessionEcho | null;
}

/** An answer that is not a 200 JSON answer, for `refusalError`. `text` is raw server text. */
export interface DatabendRefusal {
  readonly status: number;
  readonly contentType: string | null;
  readonly code: number | null;
  readonly gatewayKind: string | null;
  readonly text: string;
  /**
   * When `text` is a JSON body that names no message: every key and string value in it, decoded, since the raw text
   * holds them escaped and `errors.ts` checks both against the secret forms (design 3.13).
   */
  readonly decoded?: readonly string[];
}

export type DatabendReading =
  | { readonly kind: "answer"; readonly answer: DatabendAnswer }
  | { readonly kind: "refusal"; readonly refusal: DatabendRefusal };

/** The oldest Databend that knows `http_json_result_mode` (L7, I6). */
export const RESULT_MODE_FLOOR = "v1.2.881";

/** The warning of a `result-mode` notice, for the provider doc and the test to read back (design section 4, I6). */
export const DATABEND_ANSWER_SENTENCES = Object.freeze({
  resultMode: (mode: string) =>
    mode === ""
      ? `Databend did not confirm the display result mode, which servers older than ${RESULT_MODE_FLOOR} do not have, so Studio may show some values differently from how Databend displays them. Upgrade the server to ${RESULT_MODE_FLOOR} or later.`
      : `Databend answered in the result mode "${mode}", not "display", so Studio may show some values differently from how Databend displays them.`,
});

const RESULT_MODE_SETTING = "http_json_result_mode";
const DISPLAY_MODE = "display";

/** Thrown inside the reviver so that a `__proto__` key is told apart from a body that does not parse. */
class PrototypeKey extends Error {}

type Body = Readonly<Record<string, unknown>>;

function isRecord(value: unknown): value is Body {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isJson(contentType: string | null): boolean {
  return contentType?.split(";")[0].trim().toLowerCase() === "application/json";
}

/** `JSON.parse` refusing a `__proto__` key at any depth; every failure, a `RangeError` included, is `protocol`. */
function parse(text: string, status: number): unknown {
  try {
    return JSON.parse(text, (key, value: unknown) => {
      if (key === "__proto__") throw new PrototypeKey();
      return value;
    });
  } catch (error) {
    const fault =
      error instanceof PrototypeKey ? DATABEND_PROTOCOL_FAULTS.prototypeKey : DATABEND_PROTOCOL_FAULTS.notJson;
    throw protocolError(fault, error, status);
  }
}

function wrongType(field: string): never {
  throw protocolError(DATABEND_PROTOCOL_FAULTS.field(field));
}

/** A string or absent (null); anything else is the field's protocol failure. */
function text(body: Body, key: string, field = key): string | null {
  const value = body[key];
  if (value === undefined || value === null) return null;
  return typeof value === "string" ? value : wrongType(field);
}

function required(body: Body, key: string, field = key): string {
  return text(body, key, field) ?? wrongType(field);
}

function flag(body: Body, key: string, field: string): boolean | null {
  const value = body[key];
  if (value === undefined || value === null) return null;
  return typeof value === "boolean" ? value : wrongType(field);
}

function list(body: Body, key: string, field = key): readonly unknown[] {
  const value = body[key];
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : wrongType(field);
}

function textList(values: readonly unknown[], field: string): string[] {
  return values.map((value) => (typeof value === "string" ? value : wrongType(field)));
}

function record(body: Body, key: string, field: string): Body | null {
  const value = body[key];
  if (value === undefined || value === null) return null;
  return isRecord(value) ? value : wrongType(field);
}

function readColumn(value: unknown): DatabendColumn {
  if (!isRecord(value)) return wrongType("schema");
  return { name: required(value, "name", "schema"), type: required(value, "type", "schema") };
}

function readError(body: Body): DatabendAnswerError | null {
  const error = record(body, "error", "error");
  if (error === null) return null;
  const code = error.code;
  if (typeof code !== "number") return wrongType("error");
  return { code, message: required(error, "message", "error"), detail: text(error, "detail", "error") };
}

function readSession(body: Body): DatabendSessionEcho | null {
  const session = record(body, "session", "session");
  if (session === null) return null;
  const settings = record(session, "settings", "session") ?? {};
  return {
    raw: session,
    txnState: text(session, "txn_state", "session"),
    needKeepAlive: flag(session, "need_keep_alive", "session") ?? false,
    role: text(session, "role", "session"),
    settings: Object.fromEntries(Object.entries(settings).map(([key, value]) => [key, textValue(value)])),
  };
}

function textValue(value: unknown): string {
  return typeof value === "string" ? value : wrongType("session");
}

/** USE and SET only (07 M09); an affect of any other type is not carried. */
function readAffect(body: Body): DatabendAffect | null {
  const affect = record(body, "affect", "affect");
  if (affect === null) return null;
  switch (affect.type) {
    case "UseDB":
    case "UseCatalog":
      return { type: affect.type, name: required(affect, "name", "affect") };
    case "ChangeSettings":
      return {
        type: "ChangeSettings",
        keys: textList(list(affect, "keys", "affect"), "affect"),
        values: textList(list(affect, "values", "affect"), "affect"),
        isGlobals: list(affect, "is_globals", "affect").map((value) =>
          typeof value === "boolean" ? value : wrongType("affect"),
        ),
      };
    default:
      return null;
  }
}

function readRows(body: Body, width: number): DatabendCell[][] {
  return list(body, "data").map((row) => {
    if (!Array.isArray(row)) throw protocolError(DATABEND_PROTOCOL_FAULTS.cell);
    if (row.length !== width) throw protocolError(DATABEND_PROTOCOL_FAULTS.width(row.length, width));
    return row.map((cell: unknown) => {
      if (cell === null || typeof cell === "string") return cell;
      throw protocolError(DATABEND_PROTOCOL_FAULTS.cell);
    });
  });
}

function readBody(body: Body): DatabendAnswer {
  const schema = list(body, "schema").map(readColumn);
  return {
    id: required(body, "id"),
    sessionId: text(body, "session_id"),
    nodeId: text(body, "node_id"),
    state: required(body, "state"),
    error: readError(body),
    warnings: textList(list(body, "warnings"), "warnings"),
    hasResultSet: flag(body, "has_result_set", "has_result_set") ?? schema.length > 0,
    schema,
    data: readRows(body, schema.length),
    nextUri: text(body, "next_uri"),
    affect: readAffect(body),
    session: readSession(body),
  };
}

/** Every key and string value of a parsed JSON document, walked without recursion so depth cannot overflow. */
function decodedStrings(root: unknown): string[] {
  const found: string[] = [];
  const pending = [root];
  while (pending.length > 0) {
    const value = pending.pop();
    if (typeof value === "string") found.push(value);
    else if (typeof value === "object" && value !== null) {
      for (const [key, item] of Object.entries(value)) {
        found.push(key);
        pending.push(item);
      }
    }
  }
  return found;
}

/** A refusal's code, gateway kind and message from a JSON body, else the raw text. */
function refusalOf(response: NodeResponse): DatabendRefusal {
  let parsed: unknown;
  if (isJson(response.contentType)) {
    try {
      parsed = JSON.parse(response.text);
    } catch {
      // A refusal whose JSON does not parse is read by its status alone, with its raw text.
    }
  }
  const body = isRecord(parsed) ? parsed : null;
  const error = isRecord(body?.error) ? body.error : null;
  const message = [error?.message, body?.message, body?.error].find((value) => typeof value === "string");
  return {
    status: response.status,
    contentType: response.contentType,
    code: typeof error?.code === "number" ? error.code : null,
    gatewayKind: typeof body?.kind === "string" ? body.kind : null,
    text: typeof message === "string" ? message : response.text,
    decoded: typeof message === "string" || parsed === undefined ? undefined : decodedStrings(parsed),
  };
}

/** One HTTP answer as an answer or a refusal; throws a `protocol` `DatabendError` for a malformed 200. */
export function readAnswer(response: NodeResponse): DatabendReading {
  if (response.status !== 200) return { kind: "refusal", refusal: refusalOf(response) };
  if (!isJson(response.contentType)) throw protocolError(DATABEND_PROTOCOL_FAULTS.notAnswer, undefined, 200);
  const body = parse(response.text, 200);
  if (!isRecord(body)) return wrongType("answer");
  // The Cloud gateway's refusal may come over any status, ProvisionWarehouseTimeout among them (design 3.11).
  if (typeof body.kind === "string" && body.state === undefined)
    return { kind: "refusal", refusal: refusalOf(response) };
  return { kind: "answer", answer: readBody(body) };
}

/**
 * The first answer's `result-mode` notice, or null when its session echoed `http_json_result_mode` as `display`. A
 * missing echo is a notice with an empty mode: a server below {@link RESULT_MODE_FLOOR} drops the setting it does
 * not know (L7, I6). Nothing is refused.
 */
export function resultModeNotice(answer: DatabendAnswer): DatabendNotice | null {
  const mode = answer.session?.settings[RESULT_MODE_SETTING] ?? "";
  return mode === DISPLAY_MODE ? null : { kind: "result-mode", mode };
}

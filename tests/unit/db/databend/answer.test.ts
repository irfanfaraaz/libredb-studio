/**
 * Reading one HTTP answer of Databend's query API (design 3.12, section 4; I6): only a 200 `application/json` body is
 * an answer, anything else is a refusal read by status, then content type; an answer that does not have the measured
 * shape is `protocol`.
 */
import { describe, expect, test } from "bun:test";
import type { NodeResponse } from "@/lib/db/http/node-transport";
import {
  type DatabendAnswer,
  DATABEND_ANSWER_SENTENCES,
  RESULT_MODE_FLOOR,
  readAnswer,
  resultModeNotice,
} from "@/lib/db/providers/sql/databend/answer";
import {
  DATABEND_ERROR_SENTENCES,
  DATABEND_PROTOCOL_FAULTS,
  refusalError,
} from "@/lib/db/providers/sql/databend/errors";
import { DatabendError } from "@/lib/db/providers/sql/databend/transport";
import { secretForms, serverText } from "@/lib/db/utils/server-text";

function response(status: number, contentType: string | null, text: string): NodeResponse {
  return { status, contentType, retryAfter: null, text };
}

const json = (body: unknown, status = 200) => response(status, "application/json", JSON.stringify(body));

/** A first answer as the pinned image sends it (07 M12, M08). */
const FIRST = {
  id: "01a116fd3e5d7640bc61a15733696db0",
  session_id: "5e7f0c55-0000-4000-8000-000000000001",
  node_id: "BvtSyQG274L1gg2XccJC52",
  state: "Succeeded",
  session: {
    catalog: "default",
    database: "default",
    role: "account_admin",
    settings: { http_json_result_mode: "display", timezone: "UTC" },
    txn_state: "AutoCommit",
    need_sticky: false,
    need_keep_alive: false,
    internal: '{"last_node_id":"BvtSyQG274L1gg2XccJC52"}',
  },
  error: null,
  warnings: ["setting foo is ignored"],
  has_result_set: true,
  schema: [
    { name: "a", type: "Nullable(Int64)" },
    { name: "b", type: "String" },
  ],
  data: [
    ["1", "x"],
    [null, "y"],
  ],
  affect: null,
  result_timeout_secs: 60,
  stats_uri: "/v1/query/01a116fd3e5d7640bc61a15733696db0",
  final_uri: "/v1/query/01a116fd3e5d7640bc61a15733696db0/final",
  next_uri: "/v1/query/01a116fd3e5d7640bc61a15733696db0/page/1",
  kill_uri: "/v1/query/01a116fd3e5d7640bc61a15733696db0/kill",
};

function answerOf(body: unknown): DatabendAnswer {
  const reading = readAnswer(json(body));
  if (reading.kind !== "answer") throw new Error("expected an answer");
  return reading.answer;
}

function protocolOf(run: () => unknown): DatabendError {
  try {
    run();
  } catch (error) {
    expect(error).toBeInstanceOf(DatabendError);
    expect((error as DatabendError).category).toBe("protocol");
    return error as DatabendError;
  }
  throw new Error("expected a protocol error");
}

describe("a 200 JSON answer", () => {
  test("becomes the typed answer", () => {
    const answer = answerOf(FIRST);
    expect(answer).toEqual({
      id: FIRST.id,
      sessionId: FIRST.session_id,
      nodeId: FIRST.node_id,
      state: "Succeeded",
      error: null,
      warnings: ["setting foo is ignored"],
      hasResultSet: true,
      schema: [
        { name: "a", type: "Nullable(Int64)" },
        { name: "b", type: "String" },
      ],
      data: [
        ["1", "x"],
        [null, "y"],
      ],
      nextUri: FIRST.next_uri,
      affect: null,
      session: {
        raw: FIRST.session,
        txnState: "AutoCommit",
        needKeepAlive: false,
        role: "account_admin",
        settings: { http_json_result_mode: "display", timezone: "UTC" },
      },
    });
  });

  test("accepts a charset parameter on the content type", () => {
    const reading = readAnswer(response(200, "application/json; charset=utf-8", JSON.stringify(FIRST)));
    expect(reading.kind).toBe("answer");
  });

  test("a final answer: no session, no next link, no rows; has_result_set absent reads the schema", () => {
    const answer = answerOf({ id: "q", state: "Succeeded", schema: [{ name: "a", type: "UInt8" }], session: null });
    expect(answer.session).toBeNull();
    expect(answer.nextUri).toBeNull();
    expect(answer.nodeId).toBeNull();
    expect(answer.sessionId).toBeNull();
    expect(answer.data).toEqual([]);
    expect(answer.warnings).toEqual([]);
    expect(answer.hasResultSet).toBe(true);
    expect(answerOf({ id: "q", state: "Succeeded" }).hasResultSet).toBe(false);
  });

  test("a session with only some fields reads the rest as absent", () => {
    const answer = answerOf({ id: "q", state: "Running", session: { need_sticky: false } });
    expect(answer.session).toEqual({
      raw: { need_sticky: false },
      txnState: null,
      needKeepAlive: false,
      role: null,
      settings: {},
    });
  });

  test("an in-body error is read with its code, message and detail", () => {
    const answer = answerOf({
      id: "",
      state: "Failed",
      error: { code: 2803, message: "Value bogus is not within the allowed values" },
      next_uri: null,
    });
    expect(answer.error).toEqual({ code: 2803, message: "Value bogus is not within the allowed values", detail: null });
    const detailed = answerOf({ id: "q", state: "Failed", error: { code: 1046, message: "m", detail: "d" } });
    expect(detailed.error).toEqual({ code: 1046, message: "m", detail: "d" });
  });

  test("the affect of USE and SET is read; any other type is not carried", () => {
    expect(answerOf({ ...FIRST, affect: { type: "UseDB", name: "probe" } }).affect).toEqual({
      type: "UseDB",
      name: "probe",
    });
    expect(answerOf({ ...FIRST, affect: { type: "UseCatalog", name: "c" } }).affect).toEqual({
      type: "UseCatalog",
      name: "c",
    });
    expect(
      answerOf({
        ...FIRST,
        affect: { type: "ChangeSettings", keys: ["max_threads"], values: ["4"], is_globals: [true] },
      }).affect,
    ).toEqual({ type: "ChangeSettings", keys: ["max_threads"], values: ["4"], isGlobals: [true] });
    expect(
      answerOf({ ...FIRST, affect: { type: "Create", kind: "table", name: "t", success: true } }).affect,
    ).toBeNull();
  });

  test("a 200 gateway body is a refusal", () => {
    expect(readAnswer(json({ kind: "ProvisionWarehouseTimeout", message: "provision warehouse timeout" }))).toEqual({
      kind: "refusal",
      refusal: {
        status: 200,
        contentType: "application/json",
        code: null,
        gatewayKind: "ProvisionWarehouseTimeout",
        text: "provision warehouse timeout",
      },
    });
  });
});

describe("anything but a 200 JSON body", () => {
  test("a 200 of another content type is protocol", () => {
    const error = protocolOf(() => readAnswer(response(200, "text/plain", "ok")));
    expect(error.message).toBe(DATABEND_ERROR_SENTENCES.protocol(DATABEND_PROTOCOL_FAULTS.notAnswer));
    expect(error.status).toBe(200);
    protocolOf(() => readAnswer(response(200, null, "{}")));
  });

  test("a non-200 text body is a refusal read by its status, with the body as its text", () => {
    expect(readAnswer(response(500, "text/plain", "[HTTP-PANIC] Internal server error"))).toEqual({
      kind: "refusal",
      refusal: {
        status: 500,
        contentType: "text/plain",
        code: null,
        gatewayKind: null,
        text: "[HTTP-PANIC] Internal server error",
      },
    });
  });

  test("a non-200 JSON body gives its code and message", () => {
    const reading = readAnswer(
      json({ error: { code: 5100, message: "Authentication failed: incorrect password" } }, 401),
    );
    expect(reading).toEqual({
      kind: "refusal",
      refusal: {
        status: 401,
        contentType: "application/json",
        code: 5100,
        gatewayKind: null,
        text: "Authentication failed: incorrect password",
      },
    });
  });

  test("a gateway body gives its kind and message", () => {
    const reading = readAnswer(json({ kind: "WarehouseNotFound", message: "warehouse not found" }, 404));
    expect(reading).toEqual({
      kind: "refusal",
      refusal: {
        status: 404,
        contentType: "application/json",
        code: null,
        gatewayKind: "WarehouseNotFound",
        text: "warehouse not found",
      },
    });
  });

  test("databend-go's string error shape gives its message, else the error string", () => {
    const withMessage = readAnswer(json({ error: "Unauthorized", message: "bad token" }, 401));
    expect(withMessage.kind === "refusal" && withMessage.refusal.text).toBe("bad token");
    const bare = readAnswer(json({ error: "Unauthorized" }, 401));
    expect(bare.kind === "refusal" && bare.refusal.text).toBe("Unauthorized");
  });

  test("a non-200 JSON body that does not parse, or names nothing, keeps the raw text", () => {
    const broken = readAnswer(response(502, "application/json", "<html>bad gateway</html>"));
    expect(broken).toEqual({
      kind: "refusal",
      refusal: {
        status: 502,
        contentType: "application/json",
        code: null,
        gatewayKind: null,
        text: "<html>bad gateway</html>",
      },
    });
    const empty = readAnswer(response(503, "application/json", "[]"));
    expect(empty.kind === "refusal" && empty.refusal.text).toBe("[]");
  });

  test("a message-less JSON body that echoes the password escaped is withheld, not shown escaped", () => {
    const cases: readonly [string, string][] = [
      ['pa"ss\\w', JSON.stringify({ detail: 'bad credential pa"ss\\w' })],
      ["a/b/c9", '{"detail":"Basic auth reader:a\\/b\\/c9 refused"}'],
      ["Müller1", '{"detail":"login failed for reader with password M\\u00fcller1"}'],
    ];
    for (const [password, body] of cases) {
      const forms = secretForms([password, `reader:${password}`]);
      const reading = readAnswer(response(502, "application/json", body));
      if (reading.kind !== "refusal") throw new Error("expected a refusal");
      const ctx = {
        request: "get",
        origin: "user",
        sql: "SELECT 1",
        endpoint: { host: "h", port: 8000 },
        timeoutMs: 1000,
        secretForms: forms,
      } as const;
      expect(refusalError(reading.refusal, ctx).message).toBe(
        DATABEND_ERROR_SENTENCES.server(502, serverText(password, forms)),
      );
    }
  });
});

describe("protocol", () => {
  test("a 200 JSON body that does not parse", () => {
    const error = protocolOf(() => readAnswer(response(200, "application/json", "{")));
    expect(error.message).toBe(DATABEND_ERROR_SENTENCES.protocol(DATABEND_PROTOCOL_FAULTS.notJson));
    expect(error.cause).toBeInstanceOf(SyntaxError);
  });

  test("a RangeError while parsing", () => {
    const deep = `{"id":"q","state":"Running","x":${"[".repeat(200_000)}${"]".repeat(200_000)}}`;
    const error = protocolOf(() => readAnswer(response(200, "application/json", deep)));
    expect(error.message).toBe(DATABEND_ERROR_SENTENCES.protocol(DATABEND_PROTOCOL_FAULTS.notJson));
    expect(error.cause).toBeInstanceOf(RangeError);
  });

  test("__proto__ as a key, anywhere", () => {
    for (const text of [
      '{"__proto__":{"polluted":true},"id":"q","state":"Running"}',
      '{"id":"q","state":"Running","session":{"settings":{"__proto__":"x"}}}',
    ]) {
      const error = protocolOf(() => readAnswer(response(200, "application/json", text)));
      expect(error.message).toBe(DATABEND_ERROR_SENTENCES.protocol(DATABEND_PROTOCOL_FAULTS.prototypeKey));
    }
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });

  test("a numeric cell", () => {
    const error = protocolOf(() => answerOf({ ...FIRST, data: [[1, "x"]] }));
    expect(error.message).toBe(DATABEND_ERROR_SENTENCES.protocol(DATABEND_PROTOCOL_FAULTS.cell));
  });

  test("a row that is not an array", () => {
    const error = protocolOf(() => answerOf({ ...FIRST, data: ["1"] }));
    expect(error.message).toBe(DATABEND_ERROR_SENTENCES.protocol(DATABEND_PROTOCOL_FAULTS.cell));
  });

  test("a row of the wrong width", () => {
    const error = protocolOf(() => answerOf({ ...FIRST, data: [["1"]] }));
    expect(error.message).toBe(DATABEND_ERROR_SENTENCES.protocol(DATABEND_PROTOCOL_FAULTS.width(1, 2)));
    expect(DATABEND_PROTOCOL_FAULTS.width(1, 2)).toBe("a row of 1 cells for 2 columns");
  });

  test("a body that is not an object", () => {
    const error = protocolOf(() => readAnswer(json([1])));
    expect(error.message).toBe(DATABEND_ERROR_SENTENCES.protocol(DATABEND_PROTOCOL_FAULTS.field("answer")));
    protocolOf(() => readAnswer(json(null)));
  });

  test("a field of the wrong type", () => {
    const cases: readonly [string, Record<string, unknown>][] = [
      ["id", { id: 1 }],
      ["state", { state: null }],
      ["node_id", { node_id: 7 }],
      ["has_result_set", { has_result_set: "yes" }],
      ["schema", { schema: {} }],
      ["schema", { schema: [{ name: "a" }] }],
      ["schema", { schema: [null] }],
      ["data", { data: "rows" }],
      ["error", { error: "bad" }],
      ["error", { error: { code: "1005", message: "m" } }],
      ["error", { error: { code: 1005, message: "m", detail: 3 } }],
      ["warnings", { warnings: [1] }],
      ["session", { session: [] }],
      ["session", { session: { txn_state: 1 } }],
      ["session", { session: { need_keep_alive: "no" } }],
      ["session", { session: { settings: { a: 1 } } }],
      ["session", { session: { settings: "x" } }],
      ["affect", { affect: { type: "UseDB" } }],
      ["affect", { affect: { type: "ChangeSettings", keys: ["a"], values: ["1"], is_globals: ["yes"] } }],
      ["affect", { affect: { type: "ChangeSettings", keys: ["a"], values: [1], is_globals: [true] } }],
      ["affect", { affect: "UseDB" }],
    ];
    for (const [field, patch] of cases) {
      const error = protocolOf(() => answerOf({ ...FIRST, ...patch }));
      expect(error.message).toBe(DATABEND_ERROR_SENTENCES.protocol(DATABEND_PROTOCOL_FAULTS.field(field)));
    }
  });
});

describe("the result-mode echo (design section 4, I6)", () => {
  test("display gives no notice", () => {
    expect(resultModeNotice(answerOf(FIRST))).toBeNull();
  });

  test("another mode gives its notice", () => {
    const answer = answerOf({
      ...FIRST,
      session: { ...FIRST.session, settings: { http_json_result_mode: "driver" } },
    });
    expect(resultModeNotice(answer)).toEqual({ kind: "result-mode", mode: "driver" });
  });

  test("no echo, as below the version floor, gives the notice with an empty mode", () => {
    expect(resultModeNotice(answerOf({ ...FIRST, session: { ...FIRST.session, settings: {} } }))).toEqual({
      kind: "result-mode",
      mode: "",
    });
    expect(resultModeNotice(answerOf({ ...FIRST, session: null }))).toEqual({ kind: "result-mode", mode: "" });
  });

  test("the warning names the mode, or the floor when nothing was echoed", () => {
    expect(RESULT_MODE_FLOOR).toBe("v1.2.881");
    expect(DATABEND_ANSWER_SENTENCES.resultMode("driver")).toBe(
      'Databend answered in the result mode "driver", not "display", so Studio may show some values differently from how Databend displays them.',
    );
    expect(DATABEND_ANSWER_SENTENCES.resultMode("")).toBe(
      "Databend did not confirm the display result mode, which servers older than v1.2.881 do not have, so Studio may show some values differently from how Databend displays them. Upgrade the server to v1.2.881 or later.",
    );
  });
});

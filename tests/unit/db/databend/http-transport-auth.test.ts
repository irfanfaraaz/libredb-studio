/**
 * The sign-in latch as the transport uses it (design 3.5; C5; X03, X14, I10, I18): a refused password is sent once,
 * then two instances on one key send nothing for 15 minutes; 2215 over HTTP 500 latches, an in-body 2215 does not;
 * two tunnels to one far end share a key; two concurrent runs on an unproven key send one request; a latch wait cut
 * short by the run's own signal is its stop, with nothing sent.
 */
import { describe, expect, test } from "bun:test";
import { AUTH_LATCH_TTL_MS, createAuthLatch } from "@/lib/db/providers/sql/databend/auth-latch";
import { DATABEND_ERROR_SENTENCES as S } from "@/lib/db/providers/sql/databend/errors";
import { createDatabendHttpTransport } from "@/lib/db/providers/sql/databend/http-transport";
import { DatabendError } from "@/lib/db/providers/sql/databend/transport";
import {
  idsOf,
  ok,
  pathsOf,
  runSignal,
  scriptedNodeTransport,
  statement,
  TEST_START,
  testOptions,
  transportDeps,
  transportHarness,
} from "../../../helpers/databend-node-transport";

const FIRST = idsOf(1);
const WRONG_PASSWORD = {
  status: 401,
  body: { error: { code: 5100, message: "Authentication failed: incorrect password" } },
};

/** Databend Cloud's lockout as measured (I19): a 500 Unexpected wrapping the query node's 500 with 2215. */
const CLOUD_LOCKOUT = {
  status: 500,
  body: {
    error: {
      kind: "Unexpected",
      message:
        'status: 500, message: {"error":{"code":2215,"message":"Disable login before 2026-10-08 00:54:35.574391755 UTC because of too many password fails"}}: Unexpected',
    },
  },
};

async function failure(promise: Promise<unknown>): Promise<DatabendError> {
  const error = await promise.then(
    () => {
      throw new Error("expected the run to fail");
    },
    (caught: unknown) => caught,
  );
  expect(error).toBeInstanceOf(DatabendError);
  return error as DatabendError;
}

function sharedLatch() {
  let now = TEST_START;
  return {
    latch: createAuthLatch({ now: () => now }),
    advance: (ms: number) => {
      now += ms;
    },
  };
}

describe("a refused sign-in", () => {
  test("is sent once: a 401 POST is the only request, then the key refuses with no request", async () => {
    const { script, transport } = transportHarness([{ method: "POST", path: "/v1/query", reply: WRONG_PASSWORD }]);
    const refused = await failure(transport.run(statement("SELECT 1")));
    expect(refused.category).toBe("auth");
    expect(refused.message).toBe(`${S.signInRefused} Authentication failed: incorrect password`);
    expect(script.requests).toHaveLength(1);
    // Basic is always sent (I10).
    expect(script.requests[0].headers.authorization).toStartWith("Basic ");

    const latched = await failure(transport.run(statement("SELECT 1")));
    expect(latched.category).toBe("auth");
    expect(latched.message).toBe(S.latched("2026-10-08 02:00", "2026-10-08 02:15"));
    expect(script.requests).toHaveLength(1);
    script.expectDone();
  });

  test("two instances on one key send nothing for 15 minutes, then the next attempt goes out", async () => {
    const shared = sharedLatch();
    const one = transportHarness([{ method: "POST", path: "/v1/query", reply: WRONG_PASSWORD }], {
      latch: shared.latch,
    });
    // The refused run drew the first ids, so the run that goes out is the second.
    const two = transportHarness([{ method: "POST", path: "/v1/query", reply: ok(idsOf(2)) }], { latch: shared.latch });
    await failure(one.transport.run(statement("SELECT 1")));
    expect((await failure(two.transport.run(statement("SELECT 1")))).category).toBe("auth");
    expect(two.script.requests).toHaveLength(0);

    shared.advance(AUTH_LATCH_TTL_MS);
    await two.transport.run(statement("SELECT 1"));
    two.script.expectDone();
  });

  test("two transports built without a latch share the process's one latch: the second sends nothing", async () => {
    // A user no other test signs in as, so the process latch holds no key of another test.
    const options = testOptions({ user: "process-latch" });
    const built = (steps: Parameters<typeof scriptedNodeTransport>[0]) => {
      const script = scriptedNodeTransport(steps);
      // Every injection but the latch, so the transport takes the production one.
      const { createNodeTransport, sleep, random, now, newId, deadline } = transportDeps(script).deps;
      const deps = { createNodeTransport, sleep, random, now, newId, deadline };
      return { script, transport: createDatabendHttpTransport(options, deps) };
    };
    const one = built([{ method: "POST", path: "/v1/query", reply: WRONG_PASSWORD }]);
    const two = built([]);
    await failure(one.transport.run(statement("SELECT 1")));
    expect((await failure(two.transport.run(statement("SELECT 1")))).category).toBe("auth");
    expect(two.script.requests).toHaveLength(0);
    two.script.expectDone();
  });

  test("2215 over HTTP 500 latches and adds the possible lockout", async () => {
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: { status: 500, body: { error: { code: 2215, message: "locked" } } } },
    ]);
    const error = await failure(transport.run(statement("SELECT 1")));
    expect(error.message).toBe(`${S.signInRefused} locked ${S.possibleLockout}`);
    expect((await failure(transport.run(statement("SELECT 1")))).message).toStartWith(
      "Databend refused this sign-in at",
    );
    expect(script.requests).toHaveLength(1);
  });

  test("an in-body 2215, also a complexity error, does not latch", async () => {
    const { script, transport } = transportHarness([
      {
        method: "POST",
        path: "/v1/query",
        reply: ok(FIRST, { state: "Failed", error: { code: 2215, message: "password too simple" } }),
      },
      { method: "POST", path: "/v1/query", reply: ok(idsOf(2)) },
    ]);
    expect((await failure(transport.run(statement("ALTER USER u IDENTIFIED BY 'x'")))).category).toBe("statement");
    await transport.run(statement("SELECT 1"));
    script.expectDone();
  });

  test("a gateway's PasswordAuthFailed latches and says the console login is not a SQL user", async () => {
    const { script, transport } = transportHarness([
      {
        method: "POST",
        path: "/v1/query",
        reply: { status: 403, body: { kind: "PasswordAuthFailed", message: "no" } },
      },
    ]);
    expect((await failure(transport.run(statement("SELECT 1")))).message).toBe(
      `${S.signInRefused} no ${S.cloudSqlUser}`,
    );
    await failure(transport.run(statement("SELECT 1")));
    expect(script.requests).toHaveLength(1);
  });

  test("a Databend Cloud AuthorizationFailed on the POST latches: the next statement sends nothing (I19)", async () => {
    const { script, transport } = transportHarness(
      [
        {
          method: "POST",
          path: "/v1/query",
          reply: {
            status: 401,
            body: {
              error: {
                kind: "AuthorizationFailed",
                message:
                  'status: 401, message: {"error":{"code":5100,"message":"Authentication failed: incorrect password"}}: Authorization failed',
              },
            },
          },
        },
      ],
      { options: testOptions({ warehouse: "default" }) },
    );
    const refused = await failure(transport.run(statement("SELECT 1")));
    expect(refused.category).toBe("auth");
    expect(refused.message).toBe(`${S.signInRefused} Authentication failed: incorrect password ${S.cloudSqlUser}`);
    expect((await failure(transport.run(statement("SELECT 1")))).message).toStartWith(
      "Databend refused this sign-in at",
    );
    expect(script.requests).toHaveLength(1);
    script.expectDone();
  });

  test("a Databend Cloud ForbiddenAccessUser refuses the statement and does not latch: the next one is sent (I19)", async () => {
    const { script, transport } = transportHarness([
      {
        method: "POST",
        path: "/v1/query",
        reply: { status: 403, body: { error: { kind: "ForbiddenAccessUser", message: "Permission denied" } } },
      },
      { method: "POST", path: "/v1/query", reply: ok(idsOf(2)) },
    ]);
    const refused = await failure(transport.run(statement("SHOW WAREHOUSES")));
    expect(refused.category).toBe("statement");
    expect(refused.message).toBe(S.statementForbidden("Permission denied"));
    await transport.run(statement("SELECT 1"));
    expect(script.requests).toHaveLength(2);
    script.expectDone();
  });

  test("a Databend Cloud lockout, wrapped under the Unexpected kind, latches on the POST: one request (I19)", async () => {
    const { script, transport } = transportHarness([{ method: "POST", path: "/v1/query", reply: CLOUD_LOCKOUT }], {
      options: testOptions({ warehouse: "default" }),
    });
    const refused = await failure(transport.run(statement("SELECT 1")));
    expect(refused.category).toBe("auth");
    expect(refused.message).toContain(S.possibleLockout);
    expect((await failure(transport.run(statement("SELECT 1")))).message).toBe(
      S.latched("2026-10-08 02:00", "2026-10-08 02:15"),
    );
    expect(script.requests).toHaveLength(1);
    script.expectDone();
  });

  test("a Databend Cloud lockout on a page latches, and no kill, ROLLBACK or logout repeats the credential", async () => {
    const P = pathsOf(FIRST.queryId);
    const session = { txn_state: "Active", need_keep_alive: true, settings: { http_json_result_mode: "display" } };
    const { script, transport } = transportHarness(
      [
        { method: "POST", path: "/v1/query", reply: ok(FIRST, { state: "Running", session, next_uri: P.page(0) }) },
        { method: "GET", path: P.page(0), reply: CLOUD_LOCKOUT },
      ],
      { options: testOptions({ warehouse: "default" }) },
    );
    expect((await failure(transport.run(statement("SELECT 1")))).category).toBe("auth");
    expect((await failure(transport.run(statement("SELECT 1")))).message).toStartWith(
      "Databend refused this sign-in at",
    );
    expect(script.requests).toHaveLength(2);
    script.expectDone();
  });

  test("a sign-in refused on a page also latches, and no kill, ROLLBACK or logout repeats the credential", async () => {
    const P = pathsOf(FIRST.queryId);
    const session = { txn_state: "Active", need_keep_alive: true, settings: { http_json_result_mode: "display" } };
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST, { state: "Running", session, next_uri: P.page(0) }) },
      { method: "GET", path: P.page(0), reply: WRONG_PASSWORD },
    ]);
    expect((await failure(transport.run(statement("SELECT 1")))).category).toBe("auth");
    expect((await failure(transport.run(statement("SELECT 1")))).message).toStartWith(
      "Databend refused this sign-in at",
    );
    script.expectDone();
  });
});

describe("the key (X03)", () => {
  test("two transports through two tunnels to one far end share it: the second sends nothing", async () => {
    const shared = sharedLatch();
    const bastion = {
      enabled: true,
      host: "bastion.test",
      port: 22,
      username: "jump",
      authMethod: "password",
      password: "tunnel-password",
    };
    const farEnd = { host: "databend.internal", port: 8000 };
    const first = transportHarness([{ method: "POST", path: "/v1/query", reply: WRONG_PASSWORD }], {
      latch: shared.latch,
      options: testOptions({ port: 41001, sshTunnel: bastion }, { farEnd }),
    });
    const second = transportHarness([], {
      latch: shared.latch,
      options: testOptions({ port: 41002, sshTunnel: bastion }, { farEnd }),
    });
    expect(first.options.origin.port).not.toBe(second.options.origin.port);
    await failure(first.transport.run(statement("SELECT 1")));
    expect((await failure(second.transport.run(statement("SELECT 1")))).category).toBe("auth");
    expect(second.script.requests).toHaveLength(0);
  });
});

describe("single flight (X14)", () => {
  test("two concurrent runs on an unproven key against a 401: one request, both refused", async () => {
    const { script, transport } = transportHarness([{ method: "POST", path: "/v1/query", reply: WRONG_PASSWORD }]);
    const [one, two] = await Promise.all([
      failure(transport.run(statement("SELECT 1"))),
      failure(transport.run(statement("SELECT 2"))),
    ]);
    expect(one.category).toBe("auth");
    expect(two.category).toBe("auth");
    expect(script.requests).toHaveLength(1);
    script.expectDone();
  });

  test("a proven key never waits: two runs are in flight at once", async () => {
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST) },
      { method: "POST", path: "/v1/query", reply: { hang: true } },
      { method: "POST", path: "/v1/query", reply: { hang: true } },
    ]);
    await transport.run(statement("SELECT 1"));
    const first = runSignal();
    const second = runSignal();
    const runs = [
      transport.run(statement("SELECT 2", { signal: first.signal })).catch((error: unknown) => error),
      transport.run(statement("SELECT 3", { signal: second.signal })).catch((error: unknown) => error),
    ];
    await script.received(3);
    expect(script.requests).toHaveLength(3);
    script.push(
      { method: "GET", path: pathsOf(idsOf(2).queryId).kill, reply: { status: 200 } },
      { method: "POST", path: "/v1/session/logout", reply: { status: 200 } },
      { method: "GET", path: pathsOf(idsOf(3).queryId).kill, reply: { status: 200 } },
      { method: "POST", path: "/v1/session/logout", reply: { status: 200 } },
    );
    first.cancel();
    await runs[0];
    second.cancel();
    await runs[1];
    script.expectDone();
  });

  test.each([
    ["a cancel", "cancel", "cancelled", S.cancelled],
    ["the statement deadline", "expire", "timeout", S.deadline("60")],
  ] as const)(
    "a wait cut short by %s is that stop, and nothing is sent (I18)",
    async (_label, how, category, message) => {
      const { script, transport } = transportHarness([{ method: "POST", path: "/v1/query", reply: { hang: true } }]);
      const holder = runSignal();
      const holding = transport.run(statement("SELECT 1", { signal: holder.signal })).catch((error: unknown) => error);
      await script.received(1);
      const waiter = runSignal();
      const waiting = failure(transport.run(statement("SELECT 2", { signal: waiter.signal })));
      waiter[how]();
      const error = await waiting;
      expect(error.category).toBe(category);
      expect(error.message).toBe(message);
      expect(script.requests).toHaveLength(1);
      script.push(
        { method: "GET", path: pathsOf(FIRST.queryId).kill, reply: { status: 200 } },
        { method: "POST", path: "/v1/session/logout", reply: { status: 200 } },
      );
      holder.cancel();
      await holding;
      script.expectDone();
    },
  );

  test("a run whose signal fired before it was sent is that stop, with nothing sent", async () => {
    const { script, transport } = transportHarness([]);
    const stopped = runSignal();
    stopped.cancel();
    expect((await failure(transport.run(statement("SELECT 1", { signal: stopped.signal })))).category).toBe(
      "cancelled",
    );
    expect(script.requests).toHaveLength(0);
  });
});

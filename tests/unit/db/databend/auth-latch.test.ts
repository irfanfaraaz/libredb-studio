/**
 * The sign-in latch of design 3.5: the key (scheme, far end, bastion route, user and password, framed and hashed,
 * never the local forward [X03]), the signals that set it and the in-body 2215 that does not, the 15 minutes on an
 * injected clock, the 256-entry bound that evicts expired entries first, then the oldest [X30], and single flight per
 * unproven key [X14]. No test waits on a real timer.
 */
import { describe, expect, test } from "bun:test";
import {
  AUTH_LATCH_MAX_ENTRIES,
  AUTH_LATCH_TTL_MS,
  type AuthLatchIdentity,
  authLatchKey,
  createAuthLatch,
} from "@/lib/db/providers/sql/databend/auth-latch";
import { latchedError } from "@/lib/db/providers/sql/databend/errors";
import { DatabendError } from "@/lib/db/providers/sql/databend/transport";

// Named placeholders, never realistic values: a credential in a test fixture is a stand-in.
const TEST_USER = "reader";
const TEST_PASSWORD = "password";
const START = Date.UTC(2026, 9, 8, 1, 0, 0);
const OK = { status: 200 } as const;
const WRONG_PASSWORD = { status: 401, code: 5100 } as const;

const IDENTITY: AuthLatchIdentity = {
  scheme: "http",
  host: "databend.test",
  port: 8000,
  route: "",
  user: TEST_USER,
  password: TEST_PASSWORD,
};

function clock(start = START) {
  let now = start;
  return {
    now: () => now,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

const live = () => new AbortController().signal;

/** Settles on the next turns of the event loop, so a promise that is still waiting stays pending. */
async function settled<T>(promise: Promise<T>): Promise<"pending" | "resolved" | "rejected"> {
  let state: "pending" | "resolved" | "rejected" = "pending";
  promise.then(
    () => {
      state = "resolved";
    },
    () => {
      state = "rejected";
    },
  );
  // oxlint-disable-next-line no-await-in-loop -- each await yields one turn, which is the point.
  for (let turn = 0; turn < 5; turn++) await Promise.resolve();
  return state;
}

/**
 * Settles each key in turn with one answer, moving the clock by `step` after each: the map keeps the order of its
 * writes, so they may not run at once.
 */
async function writeEach(
  latch: ReturnType<typeof createAuthLatch>,
  keys: readonly string[],
  answer: { readonly status: number; readonly code?: number },
  time?: { readonly advance: (ms: number) => void; readonly step: number },
): Promise<void> {
  for (const key of keys) {
    // oxlint-disable-next-line no-await-in-loop -- each write must land before the next, in order.
    (await latch.acquire(key, live())).settle(answer);
    time?.advance(time.step);
  }
}

/** Every key is refused, latched. */
async function allLatched(latch: ReturnType<typeof createAuthLatch>, keys: readonly string[]): Promise<void> {
  const errors = await Promise.all(keys.map((key) => refusal(latch.acquire(key, live()))));
  for (const error of errors) expect(error.category).toBe("auth");
}

async function refusal(promise: Promise<unknown>): Promise<DatabendError> {
  try {
    await promise;
  } catch (error) {
    return error as DatabendError;
  }
  throw new Error("expected a refusal");
}

describe("authLatchKey", () => {
  test("is a SHA-256 hex digest that holds neither the password nor the user", () => {
    const key = authLatchKey(IDENTITY);
    expect(key).toMatch(/^[0-9a-f]{64}$/);
    expect(key).not.toContain(TEST_PASSWORD);
    expect(key).not.toContain(TEST_USER);
    expect(key).not.toContain(Buffer.from(`${TEST_USER}:${TEST_PASSWORD}`).toString("hex"));
  });

  test("changes with each framed field: scheme, host, port, bastion route, user and password", () => {
    const base = authLatchKey(IDENTITY);
    const variants: Partial<AuthLatchIdentity>[] = [
      { scheme: "https" },
      { host: "other.test" },
      { port: 8001 },
      { route: "4:true9:bastion.a2:224:jump" },
      { user: "writer" },
      { password: "password2" },
      { password: "" },
    ];
    for (const variant of variants) expect(authLatchKey({ ...IDENTITY, ...variant })).not.toBe(base);
  });

  test("is length-framed, so text cannot slide from one field into the next", () => {
    expect(authLatchKey({ ...IDENTITY, user: "ab", password: "c" })).not.toBe(
      authLatchKey({ ...IDENTITY, user: "a", password: "bc" }),
    );
  });
});

describe("the latch", () => {
  test("a refused sign-in latches its key: the next acquire is refused with the dated sentence and nothing is sent", async () => {
    const time = clock();
    const latch = createAuthLatch({ now: time.now });
    const key = authLatchKey(IDENTITY);
    (await latch.acquire(key, live())).settle(WRONG_PASSWORD);
    time.advance(60_000);
    const error = await refusal(latch.acquire(key, live()));
    expect(error).toBeInstanceOf(DatabendError);
    expect(error.category).toBe("auth");
    expect(error.message).toBe(latchedError(new Date(START), new Date(START + AUTH_LATCH_TTL_MS)).message);
    expect(error.message).toStartWith("Databend refused this sign-in at 2026-10-08 01:00");
    expect(error.message).toContain("again before 2026-10-08 01:15");
  });

  test("an in-body 2215 does not latch, and proves the key", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    (await latch.acquire(key, live())).settle({ status: 200, code: 2215 });
    await latch.acquire(key, live());
    expect(await settled(latch.acquire(key, live()))).toBe("resolved");
  });

  test("the latch lifts after 15 minutes on the injected clock, and not a millisecond before", async () => {
    const time = clock();
    const latch = createAuthLatch({ now: time.now });
    const key = authLatchKey(IDENTITY);
    (await latch.acquire(key, live())).settle({ status: 500, code: 2215 });
    time.advance(AUTH_LATCH_TTL_MS - 1);
    expect((await refusal(latch.acquire(key, live()))).category).toBe("auth");
    time.advance(1);
    expect(await settled(latch.acquire(key, live()))).toBe("resolved");
  });

  test("a new password is a new key, which the latch does not hold", async () => {
    const latch = createAuthLatch({ now: clock().now });
    (await latch.acquire(authLatchKey(IDENTITY), live())).settle(WRONG_PASSWORD);
    expect(await settled(latch.acquire(authLatchKey({ ...IDENTITY, password: "password2" }), live()))).toBe("resolved");
  });

  test("a proven key that is later refused latches", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    (await latch.acquire(key, live())).settle(OK);
    (await latch.acquire(key, live())).settle({ status: 500, code: 2215 });
    expect((await refusal(latch.acquire(key, live()))).category).toBe("auth");
  });

  test("a late 200 from an attempt acquired before a newer latch does not lift it", async () => {
    const time = clock();
    const latch = createAuthLatch({ now: time.now });
    const key = authLatchKey(IDENTITY);
    (await latch.acquire(key, live())).settle(OK);
    const stale = await latch.acquire(key, live());
    time.advance(AUTH_LATCH_TTL_MS);
    (await latch.acquire(key, live())).settle({ status: 500, code: 2215 });
    stale.settle(OK);
    expect((await refusal(latch.acquire(key, live()))).category).toBe("auth");
  });
});

describe("eviction [X30]", () => {
  const keys = Array.from({ length: AUTH_LATCH_MAX_ENTRIES + 1 }, (_, index) =>
    authLatchKey({ ...IDENTITY, user: `user${index}` }),
  );

  test("holds at most 256 entries", () => {
    expect(AUTH_LATCH_MAX_ENTRIES).toBe(256);
  });

  test("entries past their 15 minutes make room before any live entry is evicted", async () => {
    const time = clock();
    const latch = createAuthLatch({ now: time.now });
    await writeEach(latch, keys.slice(0, 10), WRONG_PASSWORD);
    time.advance(AUTH_LATCH_TTL_MS / 2);
    await writeEach(latch, keys.slice(10, AUTH_LATCH_MAX_ENTRIES), WRONG_PASSWORD);
    time.advance(AUTH_LATCH_TTL_MS / 2);
    const fresh = Array.from({ length: 10 }, (_, index) => authLatchKey({ ...IDENTITY, user: `fresh${index}` }));
    await writeEach(latch, fresh, WRONG_PASSWORD);
    await allLatched(latch, [...keys.slice(10, AUTH_LATCH_MAX_ENTRIES), ...fresh]);
  });

  test("an expired entry goes first even when it is not the oldest, as after the wall clock steps back", async () => {
    const time = clock(START + 2 * AUTH_LATCH_TTL_MS);
    const latch = createAuthLatch({ now: time.now });
    // The oldest entry is written at a time the clock then steps back from, so it stays live the longest.
    (await latch.acquire(keys[0], live())).settle(WRONG_PASSWORD);
    time.advance(-2 * AUTH_LATCH_TTL_MS);
    (await latch.acquire(keys[1], live())).settle(WRONG_PASSWORD);
    time.advance(1);
    await writeEach(latch, keys.slice(2, AUTH_LATCH_MAX_ENTRIES), WRONG_PASSWORD);
    // Entry 1 alone has expired; entry 0 is the oldest in order and still latched.
    time.advance(AUTH_LATCH_TTL_MS - 1);
    (await latch.acquire(keys[AUTH_LATCH_MAX_ENTRIES], live())).settle(WRONG_PASSWORD);
    expect((await refusal(latch.acquire(keys[0], live()))).category).toBe("auth");
    expect((await refusal(latch.acquire(keys[2], live()))).category).toBe("auth");
    expect((await refusal(latch.acquire(keys[AUTH_LATCH_MAX_ENTRIES], live()))).category).toBe("auth");
  });

  test("with no expired entry, entry 257 evicts the oldest", async () => {
    const time = clock();
    const latch = createAuthLatch({ now: time.now });
    await writeEach(latch, keys.slice(0, AUTH_LATCH_MAX_ENTRIES), WRONG_PASSWORD, { advance: time.advance, step: 1 });
    (await latch.acquire(keys[AUTH_LATCH_MAX_ENTRIES], live())).settle(WRONG_PASSWORD);
    expect(await settled(latch.acquire(keys[0], live()))).toBe("resolved");
    expect((await refusal(latch.acquire(keys[1], live()))).category).toBe("auth");
    expect((await refusal(latch.acquire(keys[AUTH_LATCH_MAX_ENTRIES], live()))).category).toBe("auth");
  });

  test("a key written again moves to the newest place", async () => {
    const time = clock();
    const latch = createAuthLatch({ now: time.now });
    await writeEach(latch, keys.slice(0, AUTH_LATCH_MAX_ENTRIES), OK, { advance: time.advance, step: 1 });
    (await latch.acquire(keys[0], live())).settle(WRONG_PASSWORD);
    (await latch.acquire(keys[AUTH_LATCH_MAX_ENTRIES], live())).settle(OK);
    expect((await refusal(latch.acquire(keys[0], live()))).category).toBe("auth");
  });
});

describe("single flight [X14]", () => {
  test("a second acquire on an unproven key waits, then is refused unsent when the first latches", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    const first = await latch.acquire(key, live());
    const second = latch.acquire(key, live());
    const third = latch.acquire(key, live());
    expect(await settled(second)).toBe("pending");
    first.settle(WRONG_PASSWORD);
    expect((await refusal(second)).message).toBe(
      latchedError(new Date(START), new Date(START + AUTH_LATCH_TTL_MS)).message,
    );
    expect((await refusal(third)).category).toBe("auth");
  });

  test("a waiting acquire proceeds after a 200, as does every other waiter", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    const first = await latch.acquire(key, live());
    const second = latch.acquire(key, live());
    const third = latch.acquire(key, live());
    first.settle(OK);
    expect(await settled(second)).toBe("resolved");
    expect(await settled(third)).toBe("resolved");
  });

  test("a waiting acquire times out unsent when its own deadline fires first, with the signal's reason", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    await latch.acquire(key, live());
    const deadline = new AbortController();
    const second = latch.acquire(key, deadline.signal);
    const reason = new Error("deadline");
    deadline.abort(reason);
    expect(await refusal(second)).toBe(reason as DatabendError);
  });

  test("an acquire whose signal has already fired is refused before it waits", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    await latch.acquire(key, live());
    const reason = new Error("cancelled");
    expect(await refusal(latch.acquire(key, AbortSignal.abort(reason)))).toBe(reason as DatabendError);
  });

  test("a timed-out waiter leaves the queue: the next release goes to the waiter behind it", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    const first = await latch.acquire(key, live());
    const deadline = new AbortController();
    const second = latch.acquire(key, deadline.signal);
    const third = latch.acquire(key, live());
    deadline.abort(new Error("deadline"));
    await refusal(second);
    first.abandon();
    expect(await settled(third)).toBe("resolved");
  });

  test("an answer that neither proves nor latches hands the flight to one waiter, and the next keeps waiting", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    const first = await latch.acquire(key, live());
    const second = latch.acquire(key, live());
    const third = latch.acquire(key, live());
    first.settle({ status: 503 });
    expect(await settled(second)).toBe("resolved");
    expect(await settled(third)).toBe("pending");
    (await second).abandon();
    expect(await settled(third)).toBe("resolved");
    (await third).abandon();
    // The flight is free again: the next acquire does not wait.
    expect(await settled(latch.acquire(key, live()))).toBe("resolved");
  });

  test("settling an attempt twice hands the flight on once", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    const first = await latch.acquire(key, live());
    const second = latch.acquire(key, live());
    const third = latch.acquire(key, live());
    first.abandon();
    first.abandon();
    expect(await settled(second)).toBe("resolved");
    expect(await settled(third)).toBe("pending");
  });

  test("a proven key never waits", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    (await latch.acquire(key, live())).settle(OK);
    const first = latch.acquire(key, live());
    const second = latch.acquire(key, live());
    expect(await settled(first)).toBe("resolved");
    expect(await settled(second)).toBe("resolved");
  });

  test("an attempt on a proven key releases nothing when it ends with no answer", async () => {
    const latch = createAuthLatch({ now: clock().now });
    const key = authLatchKey(IDENTITY);
    (await latch.acquire(key, live())).settle(OK);
    (await latch.acquire(key, live())).abandon();
    expect(await settled(latch.acquire(key, live()))).toBe("resolved");
  });

  test("two keys fly independently", async () => {
    const latch = createAuthLatch({ now: clock().now });
    await latch.acquire(authLatchKey(IDENTITY), live());
    expect(await settled(latch.acquire(authLatchKey({ ...IDENTITY, user: "writer" }), live()))).toBe("resolved");
  });
});

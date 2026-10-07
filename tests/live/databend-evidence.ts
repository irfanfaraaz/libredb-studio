/**
 * The Databend evidence harness (design 9, D14): it runs every scenario of tests/live/databend-evidence-plan.ts
 * against the `databend-http` fixture of docker/databend/README.md over `node:http`, with no provider import, and
 * writes each scenario's exchanges as tests/fixtures/databend/<target>-<date>-<version>/<scenario>.json, plus a
 * manifest.json naming the Studio commit and the harness files it does not hold as run, the image, the server
 * version, the date and each scenario's result and time. The captures feed the transport tests and the replay;
 * tests/fixtures/databend/README.md describes them.
 *
 * Every exchange goes through the scrub of tests/helpers/databend-evidence-scrub.ts (C23): only allow-listed headers
 * and fields, placeholders for ids, IP addresses, user names and the tenant, and nothing at all written while any
 * file holds a secret form or the other names the scrub refuses. A scenario whose answers do not show what the plan
 * expects stops the run, and nothing is written either. The credentials are read from database-compose.yml and
 * docker/databend/fixture.jsonl, the files that set them.
 *
 * The harness never writes to the fixture: it creates nothing but temporary tables in its own client sessions, which
 * end with the session (tests/unit/db/databend/live-environment.test.ts holds that). A query left running by a failed
 * run is killed before the run stops.
 *
 * Run by hand, never by `bun run test` (tests/runner/discover.ts excludes tests/live/), with the fixture up and seeded:
 *   bun tests/live/databend-evidence.ts --target local
 * Only the local target exists; the Cloud acceptance of plan section 7 brings its own.
 */
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import http from "node:http";
import path from "node:path";
import { parse as parseYaml } from "yaml";
import {
  EvidenceScrubber,
  type EvidenceSecrets,
  type RawExchange,
  type ScrubbedExchange,
} from "../helpers/databend-evidence-scrub";
import {
  EVIDENCE_SCENARIOS,
  type EvidenceExpectation,
  type EvidencePrincipal,
  type EvidenceScenario,
  type EvidenceStep,
} from "./databend-evidence-plan";

const ROOT = path.resolve(import.meta.dirname, "../..");
const OUT = path.join(ROOT, "tests/fixtures/databend");
const CONTAINER = "libredb-databend-http";
const SEED_CONTAINER = "libredb-databend-http-seed";
const PORT = 8000;
const REQUEST_TIMEOUT_MS = 60_000;
/** Not the password of any user: the 401 capture's credential. */
const WRONG_PASSWORD = "Wrong123pass!";

function docker(args: readonly string[]): string {
  return execFileSync("docker", args, { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] }).trim();
}

/** The running fixture's image as `tag@digest`; a server not healthy, not pinned or not seeded stops the run. */
function pinnedImage(): string {
  const state = docker([
    "inspect",
    "--format",
    "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}",
    CONTAINER,
  ]);
  if (state !== "running healthy")
    throw new Error(`${CONTAINER} is "${state}", not "running healthy": bring it up as docker/databend/README.md says`);
  const seed = docker(["inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", SEED_CONTAINER]);
  if (seed !== "exited 0") throw new Error(`${SEED_CONTAINER} is "${seed}", not "exited 0": seed the fixture first`);
  const image = docker(["inspect", "--format", "{{.Config.Image}}", CONTAINER]);
  if (!/:[^@/]+@sha256:[0-9a-f]{64}$/.test(image)) throw new Error(`${CONTAINER} runs ${image}, not a tag@digest`);
  return image;
}

function studioCommit(): string {
  return execFileSync("git", ["rev-parse", "HEAD"], { cwd: ROOT, encoding: "utf8" }).trim();
}

/** What a capture depends on besides the server: the compose file, the fixture, the plan, the scrub and this file. */
const HARNESS_PATHS = [
  "database-compose.yml",
  "docker/databend/",
  "tests/helpers/databend-evidence-scrub.ts",
  "tests/live/databend-evidence-plan.ts",
  "tests/live/databend-evidence.ts",
];

/**
 * The harness files that differ from the Studio commit or are not in it, so the manifest never names a commit as the
 * source of captures that commit cannot reproduce. An empty list means the commit alone reproduces the run.
 */
function uncommitted(): string[] {
  const status = execFileSync("git", ["status", "--porcelain", "--untracked-files=all", "--", ...HARNESS_PATHS], {
    cwd: ROOT,
    encoding: "utf8",
  });
  return status
    .split("\n")
    .filter((line) => line !== "")
    .map((line) => line.slice(3))
    .sort();
}

// -- the credentials ----------------------------------------------------------------------------------------------

interface Credential {
  readonly user: string;
  readonly password: string;
}

function readCredentials(): Readonly<Record<EvidencePrincipal, Credential>> {
  const compose = parseYaml(readFileSync(path.join(ROOT, "database-compose.yml"), "utf8"), { merge: true }) as {
    services: Record<string, { environment?: Record<string, string> }>;
  };
  const environment = compose.services["databend-http"]?.environment ?? {};
  const user = environment.QUERY_DEFAULT_USER;
  const password = environment.QUERY_DEFAULT_PASSWORD;
  if (user === undefined || password === undefined) throw new Error("databend-http sets no default user");
  const fixture = readFileSync(path.join(ROOT, "docker/databend/fixture.jsonl"), "utf8");
  const reader = /USER studio_reader IDENTIFIED BY '([^']+)'/.exec(fixture)?.[1];
  if (reader === undefined) throw new Error("fixture.jsonl creates no studio_reader");
  return {
    default: { user, password },
    reader: { user: "studio_reader", password: reader },
    wrong: { user, password: WRONG_PASSWORD },
  };
}

// -- requests -----------------------------------------------------------------------------------------------------

interface Sent {
  readonly status: number;
  readonly headers: Record<string, string>;
  readonly body: string;
}

function send(
  method: "GET" | "POST",
  target: string,
  headers: Record<string, string>,
  payload: unknown,
): Promise<Sent> {
  const text = payload === undefined ? undefined : JSON.stringify(payload);
  const all = { ...headers, ...(text === undefined ? {} : { "content-length": String(Buffer.byteLength(text)) }) };
  return new Promise((resolve, reject) => {
    const request = http.request(
      { host: "127.0.0.1", port: PORT, method, path: target, headers: all, timeout: REQUEST_TIMEOUT_MS, agent: false },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => chunks.push(chunk));
        response.on("error", reject);
        response.on("end", () => {
          const received: Record<string, string> = {};
          for (const [name, value] of Object.entries(response.headers))
            if (value !== undefined) received[name] = Array.isArray(value) ? value.join(", ") : value;
          resolve({
            status: response.statusCode ?? 0,
            headers: received,
            body: Buffer.concat(chunks).toString("utf8"),
          });
        });
      },
    );
    request.on("timeout", () =>
      request.destroy(new Error(`${method} ${target} did not answer in ${REQUEST_TIMEOUT_MS} ms`)),
    );
    request.on("error", reject);
    if (text !== undefined) request.write(text);
    request.end();
  });
}

interface Answer {
  readonly id?: string;
  readonly state?: string;
  readonly session?: Record<string, unknown> & { txn_state?: string; need_keep_alive?: boolean };
  readonly error?: { code?: number; message?: string } | null;
  readonly has_result_set?: boolean;
  readonly data?: unknown[];
  readonly next_uri?: string | null;
  readonly final_uri?: string | null;
  readonly kill_uri?: string | null;
}

function parsed(body: string): Answer {
  try {
    const value: unknown = JSON.parse(body);
    return typeof value === "object" && value !== null ? (value as Answer) : {};
  } catch {
    return {};
  }
}

interface Exchange {
  readonly raw: RawExchange;
  readonly answer: Answer;
  readonly step: EvidenceStep["kind"];
}

/** Runs one scenario's steps in order, every request with the scenario's principal and client session. */
async function runScenario(scenario: EvidenceScenario, credential: Credential): Promise<Exchange[]> {
  const headers: Record<string, string> = {
    authorization: `Basic ${Buffer.from(`${credential.user}:${credential.password}`).toString("base64")}`,
    accept: "application/json",
    "content-type": "application/json",
    "user-agent": "libredb-studio-evidence",
  };
  if (scenario.clientSession) {
    headers["x-databend-client-caps"] = "session_header";
    headers["x-databend-session"] = Buffer.from(
      JSON.stringify({ id: randomUUID(), last_refresh_time: Math.floor(Date.now() / 1000) }),
    )
      // URL-safe base64 with its `=` padding, which Databend requires and Node's base64url leaves out.
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_");
  }
  const exchanges: Exchange[] = [];
  let last: Answer = {};
  let session: Record<string, unknown> | undefined;
  const exchange = async (kind: EvidenceStep["kind"], method: "GET" | "POST", target: string, body?: unknown) => {
    const sent = await send(method, target, headers, body);
    const answer = parsed(sent.body);
    exchanges.push({
      raw: {
        request: { method, path: target, headers: { ...headers }, ...(body === undefined ? {} : { body }) },
        response: { status: sent.status, headers: sent.headers, body: sent.body },
      },
      answer,
      step: kind,
    });
    if (sent.headers["x-databend-session"] !== undefined)
      headers["x-databend-session"] = sent.headers["x-databend-session"];
    return answer;
  };
  try {
    for (const step of scenario.steps) {
      switch (step.kind) {
        case "query": {
          const payload = { sql: step.sql, pagination: step.pagination, session: session ?? step.session };
          // oxlint-disable-next-line no-await-in-loop -- one request at a time, in plan order.
          last = await exchange("query", "POST", "/v1/query", payload);
          if (last.session !== undefined) session = last.session;
          break;
        }
        case "pages":
          while (typeof last.next_uri === "string") {
            // oxlint-disable-next-line no-await-in-loop -- each page names the next.
            const page = await exchange("pages", "GET", last.next_uri);
            if (page.session !== undefined) session = page.session;
            if (last.next_uri === last.final_uri) break;
            last = page;
          }
          break;
        case "next":
          if (typeof last.next_uri !== "string") throw new Error(`${scenario.name}: no next_uri to follow`);
          // oxlint-disable-next-line no-await-in-loop -- in plan order.
          await exchange("next", "GET", last.next_uri);
          break;
        case "final":
        case "kill": {
          const link = step.kind === "final" ? last.final_uri : last.kill_uri;
          if (typeof link !== "string") throw new Error(`${scenario.name}: no ${step.kind}_uri to follow`);
          // oxlint-disable-next-line no-await-in-loop -- in plan order.
          await exchange(step.kind, "GET", link);
          break;
        }
        case "logout":
          // oxlint-disable-next-line no-await-in-loop -- in plan order.
          await exchange("logout", "POST", "/v1/session/logout", {});
          break;
      }
    }
  } catch (error) {
    // A failed scenario leaves no query running on the fixture.
    if (typeof last.kill_uri === "string" && last.state === "Running")
      await send("GET", last.kill_uri, headers, undefined).catch(() => undefined);
    throw error;
  }
  return exchanges;
}

/** What the scenario's answers show that the plan does not expect, as one problem per fact. */
function problems(expect: EvidenceExpectation, exchanges: readonly Exchange[]): string[] {
  const first = exchanges[0];
  const queries = exchanges.filter((exchange) => exchange.step === "query");
  const lastAnswer = exchanges[exchanges.length - 1]?.answer;
  const rowsOf = (exchange: Exchange) => (Array.isArray(exchange.answer.data) ? exchange.answer.data.length : 0);
  const echoed = (first?.answer.session?.settings ?? {}) as Record<string, unknown>;
  const facts: [string, unknown, unknown][] = [
    ["status", expect.status, first?.raw.response.status],
    ["state", expect.state, first?.answer.state],
    ["code", expect.code, first?.answer.error?.code],
    ["need_keep_alive", expect.needKeepAlive, first?.answer.session?.need_keep_alive],
    ["has_result_set", expect.hasResultSet, first?.answer.has_result_set],
    ["rows", expect.rows, exchanges.reduce((sum, exchange) => sum + rowsOf(exchange), 0)],
    ["last code", expect.lastCode, lastAnswer?.error?.code],
    ["txn_state", expect.txnStates?.join(","), queries.map((exchange) => exchange.answer.session?.txn_state).join(",")],
  ];
  const found = facts
    .filter(([, want, saw]) => want !== undefined && want !== saw)
    .map(([name, want, saw]) => `${name} ${JSON.stringify(saw)}, expected ${JSON.stringify(want)}`);
  if (expect.message !== undefined && !(first?.answer.error?.message ?? "").includes(expect.message))
    found.push(`the error message does not hold ${JSON.stringify(expect.message)}`);
  const pages = exchanges.filter((exchange) => rowsOf(exchange) > 0).length;
  if (expect.pagesWithRows !== undefined && pages < expect.pagesWithRows)
    found.push(`${pages} answers carry rows, expected at least ${expect.pagesWithRows}`);
  for (const [name, value] of Object.entries(expect.echoes ?? {}))
    if (echoed[name] !== value) found.push(`the session echoes ${name}=${JSON.stringify(echoed[name])}`);
  for (const name of expect.drops ?? []) if (name in echoed) found.push(`the session echoes ${name}`);
  return found;
}

interface ScenarioResult {
  readonly name: string;
  readonly result: "pass";
  readonly ms: number;
  readonly exchanges: number;
}

async function main(argv: readonly string[]): Promise<number> {
  const target = argv[argv.indexOf("--target") + 1];
  if (!argv.includes("--target") || target !== "local")
    throw new Error("usage: bun tests/live/databend-evidence.ts --target local");
  const image = pinnedImage();
  const credentials = readCredentials();
  const secrets: EvidenceSecrets = { users: [credentials.default, credentials.reader, credentials.wrong] };
  const scrubber = new EvidenceScrubber(secrets);
  const date = new Date().toISOString().slice(0, 10);

  const files: Record<string, unknown> = {};
  const results: ScenarioResult[] = [];
  let version: string | undefined;
  for (const scenario of EVIDENCE_SCENARIOS) {
    const started = performance.now();
    // oxlint-disable-next-line no-await-in-loop -- one scenario at a time, so timings and sessions do not overlap.
    const exchanges = await runScenario(scenario, credentials[scenario.principal]);
    const ms = Math.round(performance.now() - started);
    const found = problems(scenario.expect, exchanges);
    if (found.length > 0) throw new Error(`${scenario.name}: ${found.join("; ")}: nothing written`);
    version ??= exchanges[0]?.raw.response.headers["x-databend-version"];
    const scrubbed: (ScrubbedExchange & { step: EvidenceStep["kind"] })[] = exchanges.map((exchange) => {
      const { request, response } = scrubber.exchange(exchange.raw);
      return { step: exchange.step, request, response };
    });
    files[`${scenario.name}.json`] = {
      scenario: scenario.name,
      principal: scenario.principal,
      clientSession: scenario.clientSession,
      exchanges: scrubbed,
    };
    results.push({ name: scenario.name, result: "pass", ms, exchanges: exchanges.length });
    console.error(`pass ${scenario.name} (${exchanges.length} exchanges, ${ms} ms)`);
  }
  if (version === undefined) throw new Error("no answer carried x-databend-version");
  const serverVersion = (files["version.json"] as { exchanges: { response: { body: { data: string[][] } } }[] })
    .exchanges[0]?.response.body.data[0]?.[0];
  files["manifest.json"] = {
    target,
    studioCommit: studioCommit(),
    uncommitted: uncommitted(),
    image,
    serverVersion,
    capturedAt: date,
    scenarios: results,
  };

  const rendered = scrubber.render(files);
  const directory = path.join(OUT, `${target}-${date}-v${version}`);
  mkdirSync(directory, { recursive: true });
  for (const [name, text] of Object.entries(rendered)) writeFileSync(path.join(directory, name), text);
  console.error(`wrote ${Object.keys(rendered).length} files under ${path.relative(ROOT, directory)}`);
  return 0;
}

main(process.argv.slice(2)).then(
  (code) => process.exit(code),
  (error: unknown) => {
    console.error(error instanceof Error ? (error.stack ?? error.message) : String(error));
    process.exit(1);
  },
);

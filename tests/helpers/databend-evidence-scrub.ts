/**
 * The evidence scrub of the Databend harness (design 8, C23): the only path from a raw HTTP exchange with a Databend
 * server to a file under tests/fixtures/databend/.
 *
 * `exchange` keeps only the allow-listed headers and body fields, recording the names of the fields it dropped, and
 * puts stable placeholders in place of what identifies a server or a person: query ids (`<query-N>`), session ids
 * (`<session-N>`), node ids (`<node-N>`), IP addresses (`<ip-N>`), user names (`<user-N>`, numbered by their place in
 * the secrets) and the tenant (`<tenant>`). A placeholder is stable for the life of the scrubber, so one session id
 * reads the same in every capture of a run. The `x-databend-session` header is URL-safe base64 with `=` padding of
 * JSON carrying the session id, so it is decoded, scrubbed and encoded again in the same form.
 *
 * `render` turns a set of files into text and refuses the whole set, writing nothing, while any file holds a secret
 * form (each password, raw, percent-encoded in any case, form-encoded or in standard or URL-safe base64 with or
 * without padding, anywhere in a string, and each `user:password`, read after its user's placeholder, and in the
 * same base64 spellings), the host, the tenant, the warehouse, an email address or the egress IP in any spelling the placeholders
 * missed. Every string that reads as base64 is also scanned decoded. The error names the file and what it holds,
 * never the value.
 */

/** What a run sends or meets that must never reach a capture. Cloud runs name the last four. */
export interface EvidenceSecrets {
  readonly users: readonly { readonly user: string; readonly password: string }[];
  readonly host?: string;
  readonly tenant?: string;
  readonly warehouse?: string;
  readonly egressIp?: string;
}

/** One request and its answer, as the harness saw them. */
export interface RawExchange {
  readonly request: {
    readonly method: string;
    readonly path: string;
    readonly headers: Readonly<Record<string, string>>;
    readonly body?: unknown;
  };
  readonly response: {
    readonly status: number;
    readonly headers: Readonly<Record<string, string>>;
    readonly body: string;
  };
}

/** An exchange as it may be written: allow-listed, with placeholders, and the names of the fields it dropped. */
export interface ScrubbedExchange {
  readonly request: {
    readonly method: string;
    readonly path: string;
    readonly headers: Record<string, string>;
    readonly body?: unknown;
    readonly dropped: readonly string[];
  };
  readonly response: {
    readonly status: number;
    readonly headers: Record<string, string>;
    readonly body: unknown;
    readonly dropped: readonly string[];
  };
}

export const EVIDENCE_LEAK_SENTENCE =
  "The evidence holds a secret, the host, the tenant, the warehouse, an email address or the egress IP: nothing was written.";

export class EvidenceLeakError extends Error {
  constructor(readonly findings: readonly string[]) {
    super(EVIDENCE_LEAK_SENTENCE);
    this.name = "EvidenceLeakError";
  }
}

const REQUEST_HEADERS = ["content-type", "x-databend-client-caps", "x-databend-session", "x-databend-query-id"];
const RESPONSE_HEADERS = [
  "content-type",
  "x-databend-query-id",
  "x-databend-query-state",
  "x-databend-query-page-rows",
  "x-databend-session-id",
  "x-databend-session",
  "x-databend-version",
];
const SESSION_HEADER = "x-databend-session";

/** An allow-list: `true` keeps the value whole, an object keeps only its own fields of an object value. */
type Shape = { readonly [field: string]: true | Shape };

const REQUEST_BODY: Shape = { sql: true, session: true, pagination: true };
const ANSWER: Shape = {
  id: true,
  session_id: true,
  node_id: true,
  state: true,
  session: {
    catalog: true,
    database: true,
    role: true,
    secondary_roles: true,
    settings: true,
    txn_state: true,
    need_sticky: true,
    need_keep_alive: true,
    internal: true,
  },
  error: { code: true, message: true, detail: true },
  warnings: true,
  has_result_set: true,
  schema: true,
  data: true,
  affect: true,
  result_timeout_secs: true,
  settings: true,
  stats: true,
  stats_uri: true,
  final_uri: true,
  next_uri: true,
  kill_uri: true,
};

const IPV4 = /(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])/g;
const IPV6 =
  /(?<![\w:])(?:(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,6}:(?:[0-9a-f]{1,4}(?::[0-9a-f]{1,4}){0,5})?)(?![\w:])/gi;
const EMAIL = /[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}/;
const BASE64 = /^[A-Za-z0-9+/_-]{8,}={0,2}$/;
const QUERY_URI = /\/v1\/query\/([^/?"]+)/g;
const NODE_PARAM = /[?&]node_id=([^&"]+)/g;

const UNRESERVED = /[A-Za-z0-9._~-]/;

/** Each percent-encoding of a secret: URI-component, every reserved byte in upper and lower case, and form encoding. */
function percentForms(secret: string): string[] {
  const full = [...Buffer.from(secret, "utf8")]
    .map((byte) => {
      const char = String.fromCharCode(byte);
      return UNRESERVED.test(char) ? char : `%${byte.toString(16).padStart(2, "0").toUpperCase()}`;
    })
    .join("");
  const lower = full.replace(/%[0-9A-F]{2}/g, (escape) => escape.toLowerCase());
  const uri = encodeURIComponent(secret);
  return [uri, full, lower, ...[uri, full, lower].map((form) => form.replace(/%20/gi, "+"))];
}

/** Each base64 spelling of a secret, standard and URL-safe, unpadded: a prefix of the padded form, so it finds both. */
function base64Forms(secret: string): string[] {
  const bytes = Buffer.from(secret, "utf8");
  return [bytes.toString("base64").replace(/=+$/, ""), bytes.toString("base64url")];
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const escapeRegExp = (text: string): string => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** The value restricted to the shape's fields, and the dotted names of those it dropped. */
function pick(value: unknown, shape: Shape, prefix: string, dropped: string[]): unknown {
  if (!isRecord(value)) return value;
  const kept: Record<string, unknown> = {};
  for (const [field, inner] of Object.entries(value)) {
    const rule = shape[field];
    if (rule === undefined) dropped.push(`${prefix}${field}`);
    else kept[field] = rule === true ? inner : pick(inner, rule, `${prefix}${field}.`, dropped);
  }
  return kept;
}

function decodeSession(value: string): { id?: unknown } | undefined {
  try {
    const decoded: unknown = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
    return isRecord(decoded) ? decoded : undefined;
  } catch {
    return undefined;
  }
}

/** Every string in a JSON value, with each one that reads as base64 also decoded. */
function stringsOf(value: unknown, out: string[] = []): string[] {
  if (typeof value === "string") {
    out.push(value);
    if (BASE64.test(value)) out.push(Buffer.from(value, "base64").toString("utf8"));
  } else if (Array.isArray(value)) for (const item of value) stringsOf(item, out);
  else if (isRecord(value)) for (const item of Object.values(value)) stringsOf(item, out);
  return out;
}

export class EvidenceScrubber {
  private readonly ids = new Map<string, string>();
  private readonly counts = { query: 0, session: 0, node: 0, ip: 0 };
  private readonly ips = new Map<string, string>();

  constructor(private readonly secrets: EvidenceSecrets) {}

  /** The exchange with only allow-listed parts and every identifier replaced. */
  exchange(raw: RawExchange): ScrubbedExchange {
    const requestDropped: string[] = [];
    const responseDropped: string[] = [];
    let answer: unknown;
    try {
      answer = pick(JSON.parse(raw.response.body), ANSWER, "", responseDropped);
    } catch {
      answer = { text: raw.response.body };
    }
    this.collect(raw, answer);
    const request = {
      method: raw.request.method,
      path: raw.request.path,
      headers: this.headers(raw.request.headers, REQUEST_HEADERS),
      ...(raw.request.body === undefined ? {} : { body: pick(raw.request.body, REQUEST_BODY, "", requestDropped) }),
      dropped: requestDropped.sort(),
    };
    const response = {
      status: raw.response.status,
      headers: this.headers(raw.response.headers, RESPONSE_HEADERS),
      body: answer,
      dropped: responseDropped.sort(),
    };
    return JSON.parse(this.replace(JSON.stringify({ request, response }))) as ScrubbedExchange;
  }

  /** Each file as indented JSON with a final newline, or an {@link EvidenceLeakError} and no text at all. */
  render(files: Readonly<Record<string, unknown>>): Record<string, string> {
    const rendered: Record<string, string> = {};
    const findings: string[] = [];
    for (const [name, value] of Object.entries(files)) {
      const text = `${this.replace(JSON.stringify(value, null, 2))}\n`;
      const corpus = stringsOf(JSON.parse(text)).join("\n");
      for (const label of this.leaks(corpus)) findings.push(`${name} holds the ${label}`);
      rendered[name] = text;
    }
    if (findings.length > 0) throw new EvidenceLeakError(findings);
    return rendered;
  }

  private headers(headers: Readonly<Record<string, string>>, allowed: readonly string[]): Record<string, string> {
    const kept: Record<string, string> = {};
    for (const [name, value] of Object.entries(headers)) {
      const key = name.toLowerCase();
      if (!allowed.includes(key)) continue;
      kept[key] = key === SESSION_HEADER ? this.sessionHeader(value) : value;
    }
    return kept;
  }

  private sessionHeader(value: string): string {
    const decoded = decodeSession(value);
    if (decoded === undefined) return "<session-header>";
    if (typeof decoded.id === "string") this.id("session", decoded.id);
    // Databend refuses the header without its `=` padding, which Node's base64url leaves out.
    return Buffer.from(this.replace(JSON.stringify(decoded)))
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_");
  }

  /** Registers every query, session and node id of the exchange, in the order a reader meets them. */
  private collect(raw: RawExchange, answer: unknown): void {
    const headers = Object.fromEntries(
      [...Object.entries(raw.request.headers), ...Object.entries(raw.response.headers)].map(([k, v]) => [
        k.toLowerCase(),
        v,
      ]),
    );
    const body = isRecord(answer) ? answer : {};
    const session = isRecord(body.session) ? body.session : {};
    let internal: Record<string, unknown> = {};
    try {
      const parsed: unknown = JSON.parse(String(session.internal));
      if (isRecord(parsed)) internal = parsed;
    } catch {
      // No internal state to read.
    }
    const text = `${raw.request.path}\n${raw.response.body}`;
    const queries = [
      headers["x-databend-query-id"],
      body.id,
      ...[...text.matchAll(QUERY_URI)].map((match) => match[1]),
      ...(Array.isArray(internal.last_query_ids) ? internal.last_query_ids : []),
    ];
    const sessions = [headers["x-databend-session-id"], body.session_id];
    const nodes = [
      body.node_id,
      internal.last_node_id,
      headers["x-databend-node-id"],
      ...[...text.matchAll(NODE_PARAM)].map((match) => match[1]),
    ];
    for (const value of queries) this.id("query", value);
    for (const value of sessions) this.id("session", value);
    for (const value of nodes) this.id("node", value);
  }

  private id(kind: "query" | "session" | "node", value: unknown): void {
    if (typeof value !== "string" || value === "" || this.ids.has(value)) return;
    this.counts[kind] += 1;
    this.ids.set(value, `<${kind}-${this.counts[kind]}>`);
  }

  private ip(address: string): string {
    let placeholder = this.ips.get(address);
    if (placeholder === undefined) {
      this.counts.ip += 1;
      placeholder = `<ip-${this.counts.ip}>`;
      this.ips.set(address, placeholder);
    }
    return placeholder;
  }

  /** The text with every registered id, IP address, user name and the tenant replaced. */
  private replace(text: string): string {
    let out = text;
    for (const [value, placeholder] of this.ids) out = out.split(value).join(placeholder);
    out = out.replace(IPV4, (address) => this.ip(address)).replace(IPV6, (address) => this.ip(address));
    this.secrets.users.forEach(({ user }, index) => {
      out = out.replace(new RegExp(`(?<!\\w)${escapeRegExp(user)}(?!\\w)`, "g"), `<user-${index + 1}>`);
    });
    if (this.secrets.tenant !== undefined)
      out = out.replace(new RegExp(`(?<![\\w.-])${escapeRegExp(this.secrets.tenant)}(?![\\w.-])`, "g"), "<tenant>");
    return out;
  }

  /** The labels of what the text holds that no capture may. */
  private leaks(text: string): string[] {
    const lower = text.toLowerCase();
    const has = (value: string | undefined): boolean => value !== undefined && lower.includes(value.toLowerCase());
    const labels: string[] = [];
    const { users, host, tenant, warehouse, egressIp } = this.secrets;
    if (
      users.some(({ password }) =>
        [password, ...percentForms(password), ...base64Forms(password)].some((form) => text.includes(form)),
      )
    )
      labels.push("password");
    if (
      users.some(({ user, password }, index) =>
        [`<user-${index + 1}>:${password}`, ...base64Forms(`${user}:${password}`)].some((form) => text.includes(form)),
      )
    )
      labels.push("credential");
    if (has(host)) labels.push("host");
    if (has(tenant)) labels.push("tenant");
    if (has(warehouse)) labels.push("warehouse");
    if (EMAIL.test(text)) labels.push("email");
    if (egressIp !== undefined && [egressIp, egressIp.replaceAll(".", "-")].some(has)) labels.push("egress IP");
    return labels;
  }
}

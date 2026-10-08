/**
 * Databend Cloud's hosts (design 6.1, section 4.4 of the provider doc).
 *
 * The gateway answers on `<tenant>.gw.<region>.default.databend.com`, under `databend.cn` in the regions in China, and
 * on an older form that names the warehouse in the host, `<tenant>--<warehouse>.gw.<region>.default.databend.com`,
 * which reaches that warehouse with no warehouse header (measured on the test tenant, 2026-10-08). A host is Databend
 * Cloud's when it ends in `.databend.com` or `.databend.cn`, the test BendSQL and databend-jdbc apply to the same
 * question (`core/src/client.rs`, `DatabendSessionHandle.java`).
 *
 * Pure, with no import: the connection dialog reads it in the browser when a DSN is pasted, and the provider reads it
 * for its capabilities and for the warehouse its sentences name.
 */

/** The domains Databend Cloud serves its gateway under. */
const CLOUD_DOMAINS: readonly string[] = [".databend.com", ".databend.cn"];

/** The gateway's own label, the second of an older host. */
const GATEWAY_LABEL = "gw";

/** What separates the tenant from the warehouse in the first label of an older host. */
const WAREHOUSE_SEPARATOR = "--";

/** A warehouse as a host label can name it: letters, digits and hyphens, which Databend Cloud's names are made of. */
const HOST_WAREHOUSE = /^[A-Za-z0-9-]{1,63}$/;

/** Whether `host` is Databend Cloud's, in any case and with or without the trailing dot of a full name. */
export function isDatabendCloudHost(host: string): boolean {
  const name = host.toLowerCase().replace(/\.$/, "");
  return CLOUD_DOMAINS.some((domain) => name.endsWith(domain));
}

/**
 * The warehouse an older Databend Cloud host names, `<tenant>--<warehouse>.gw...`, as it is written in the host, or
 * undefined for every other host.
 */
export function databendCloudHostWarehouse(host: string): string | undefined {
  if (!isDatabendCloudHost(host)) return undefined;
  // A Databend Cloud host ends in two labels of its domain, so it has a second label.
  const [first, second] = host.split(".");
  if (second.toLowerCase() !== GATEWAY_LABEL) return undefined;
  const separator = first.indexOf(WAREHOUSE_SEPARATOR);
  if (separator < 1) return undefined;
  const warehouse = first.slice(separator + WAREHOUSE_SEPARATOR.length);
  return HOST_WAREHOUSE.test(warehouse) ? warehouse : undefined;
}

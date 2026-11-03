// Databricks SQL statement execution can serialize DOUBLE/DECIMAL/BIGINT
// values as JSON strings (to avoid precision loss) even though the
// generated query types declare them as `number` -- coerce defensively
// everywhere a numeric value is formatted, confirmed by a real crash
// (`e.toFixed is not a function`) on queue_list's `cost` column.
export const num = (n: number | string | null | undefined): number => {
  const v = typeof n === 'string' ? Number(n) : (n ?? 0);
  return Number.isFinite(v) ? v : 0;
};

export const fmt = (n: number | string) => Math.round(num(n)).toLocaleString('en-US');
export const money = (n: number | string) => '$' + num(n).toFixed(2);
export const pct = (a: number | string, b: number | string) => (num(b) ? Math.round((100 * num(a)) / num(b)) : 0);
export const clamp = (v: number, a: number, b: number) => Math.max(a, Math.min(b, v));
export const hms = (t: string | number) => new Date(t).toLocaleTimeString('en-GB', { hour12: false });

// Same root cause as num() above, but for BOOLEAN columns: Databricks SQL
// statement execution can deliver "true"/"false" as literal strings, and
// the string "false" is truthy in JS -- confirmed as the real cause of the
// halted banner showing permanently even though control.halted was really
// false in the database.
export const bool = (v: boolean | string | null | undefined): boolean => {
  if (typeof v === 'string') return v.toLowerCase() === 'true';
  return Boolean(v);
};

// Same root cause again, but for STRING columns that happen to hold JSON
// text (e.g. attempt_log.details, a json.dumps(...) of the agent's result
// dict): they can arrive already parsed into a real object instead of the
// raw text, and React throws (error #31) if an object is rendered directly
// as a child. Always render through this instead of the raw field.
export function text(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

export function formatSeconds(s: number | string | null | undefined): string {
  if (s === null || s === undefined) return '—';
  const v = num(s);
  if (v < 60) return `${v.toFixed(0)}s`;
  return `${(v / 60).toFixed(1)}m`;
}

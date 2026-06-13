/** @stelar-time-real Logger */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

const PRIORITY: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3, silent: 4 };
const C: Record<string, string> = { debug: '\x1b[36m', info: '\x1b[32m', warn: '\x1b[33m', error: '\x1b[31m', reset: '\x1b[0m' };
const isBrowser = typeof window !== 'undefined' && typeof process === 'undefined';

export interface LoggerOptions { level?: LogLevel; timestamp?: boolean; prefix?: string; colorize?: boolean; }

export class Logger {
  private level: LogLevel;
  private ts: boolean;
  private pfx: string;
  private color: boolean;

  constructor(o: LoggerOptions = {}) {
    this.level = o.level || 'info';
    this.ts = o.timestamp !== false;
    this.pfx = o.prefix || 'stelar';
    this.color = isBrowser ? false : o.colorize !== false;
  }

  setLevel(l: LogLevel) { this.level = l; return this; }

  private fmt(lvl: string, msg: string, meta?: Record<string, unknown>): string {
    const p: string[] = [];
    if (this.ts) p.push(new Date().toISOString());
    p.push(this.color ? `${C[lvl] || ''}[${this.pfx}:${lvl}]${C.reset}` : `[${this.pfx}:${lvl}]`);
    p.push(msg);
    if (meta && Object.keys(meta).length) try { p.push(JSON.stringify(meta)); } catch { p.push('[circular]'); }
    return p.join(' ');
  }

  private w(lvl: string, err: boolean, msg: string, meta?: Record<string, unknown>) {
    if (PRIORITY[lvl as LogLevel] < PRIORITY[this.level]) return;
    const f = this.fmt(lvl, msg, meta);
    if (isBrowser) (console as any)[lvl]?.(f);
    else (err ? process.stderr : process.stdout).write(f + '\n');
  }

  debug(m: string, meta?: Record<string, unknown>) { this.w('debug', false, m, meta); }
  info(m: string, meta?: Record<string, unknown>) { this.w('info', false, m, meta); }
  warn(m: string, meta?: Record<string, unknown>) { this.w('warn', true, m, meta); }
  error(m: string, meta?: Record<string, unknown>) { this.w('error', true, m, meta); }
}

export const NULL_LOGGER = new Logger({ level: 'silent' });

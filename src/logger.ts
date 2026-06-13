/**
 * @stelar-time-real Logger
 * Zero-dependency structured logger with levels. Works in Node.js and browser.
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

const LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
  silent: 4,
};

export interface LoggerOptions {
  level?: LogLevel;
  timestamp?: boolean;
  prefix?: string;
  colorize?: boolean;
}

const COLORS: Record<string, string> = {
  debug: '\x1b[36m',
  info: '\x1b[32m',
  warn: '\x1b[33m',
  error: '\x1b[31m',
  reset: '\x1b[0m',
};

const isBrowser = typeof window !== 'undefined' && typeof process === 'undefined';

export class Logger {
  private level: LogLevel;
  private timestamp: boolean;
  private prefix: string;
  private colorize: boolean;

  constructor(options: LoggerOptions = {}) {
    this.level = options.level || 'info';
    this.timestamp = options.timestamp !== false;
    this.prefix = options.prefix || 'stelar';
    this.colorize = isBrowser ? false : (options.colorize !== false);
  }

  setLevel(level: LogLevel): this {
    this.level = level;
    return this;
  }

  private shouldLog(level: LogLevel): boolean {
    return LEVEL_PRIORITY[level] >= LEVEL_PRIORITY[this.level];
  }

  private format(level: string, message: string, meta?: Record<string, unknown>): string {
    const parts: string[] = [];

    if (this.timestamp) {
      parts.push(new Date().toISOString());
    }

    if (this.colorize) {
      const c = COLORS[level] || '';
      parts.push(`${c}[${this.prefix}:${level}]${COLORS.reset}`);
    } else {
      parts.push(`[${this.prefix}:${level}]`);
    }

    parts.push(message);

    if (meta && Object.keys(meta).length > 0) {
      try {
        parts.push(JSON.stringify(meta));
      } catch {
        parts.push('[meta: circular]');
      }
    }

    return parts.join(' ');
  }

  private _write(level: string, target: 'stdout' | 'stderr', message: string, meta?: Record<string, unknown>): void {
    const formatted = this.format(level, message, meta);
    if (isBrowser) {
      switch (level) {
        case 'debug': console.debug(formatted); break;
        case 'info': console.info(formatted); break;
        case 'warn': console.warn(formatted); break;
        case 'error': console.error(formatted); break;
      }
    } else {
      const stream = target === 'stderr' ? process.stderr : process.stdout;
      stream.write(formatted + '\n');
    }
  }

  debug(message: string, meta?: Record<string, unknown>): void {
    if (this.shouldLog('debug')) this._write('debug', 'stdout', message, meta);
  }

  info(message: string, meta?: Record<string, unknown>): void {
    if (this.shouldLog('info')) this._write('info', 'stdout', message, meta);
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    if (this.shouldLog('warn')) this._write('warn', 'stderr', message, meta);
  }

  error(message: string, meta?: Record<string, unknown>): void {
    if (this.shouldLog('error')) this._write('error', 'stderr', message, meta);
  }
}

/** No-op logger for zero overhead when logging is disabled */
export const NULL_LOGGER: Logger = new Logger({ level: 'silent' });

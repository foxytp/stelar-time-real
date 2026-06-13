/**
 * @stelar-time-real Logger
 * Zero-dependency structured logger with levels. Works in Node.js and browser.
 */
const LEVEL_PRIORITY = {
    debug: 0,
    info: 1,
    warn: 2,
    error: 3,
    silent: 4,
};
const COLORS = {
    debug: '\x1b[36m',
    info: '\x1b[32m',
    warn: '\x1b[33m',
    error: '\x1b[31m',
    reset: '\x1b[0m',
};
const isBrowser = typeof window !== 'undefined' && typeof process === 'undefined';
export class Logger {
    constructor(options = {}) {
        this.level = options.level || 'info';
        this.timestamp = options.timestamp !== false;
        this.prefix = options.prefix || 'stelar';
        this.colorize = isBrowser ? false : (options.colorize !== false);
    }
    setLevel(level) {
        this.level = level;
        return this;
    }
    shouldLog(level) {
        return LEVEL_PRIORITY[level] >= LEVEL_PRIORITY[this.level];
    }
    format(level, message, meta) {
        const parts = [];
        if (this.timestamp) {
            parts.push(new Date().toISOString());
        }
        if (this.colorize) {
            const c = COLORS[level] || '';
            parts.push(`${c}[${this.prefix}:${level}]${COLORS.reset}`);
        }
        else {
            parts.push(`[${this.prefix}:${level}]`);
        }
        parts.push(message);
        if (meta && Object.keys(meta).length > 0) {
            try {
                parts.push(JSON.stringify(meta));
            }
            catch {
                parts.push('[meta: circular]');
            }
        }
        return parts.join(' ');
    }
    _write(level, target, message, meta) {
        const formatted = this.format(level, message, meta);
        if (isBrowser) {
            switch (level) {
                case 'debug':
                    console.debug(formatted);
                    break;
                case 'info':
                    console.info(formatted);
                    break;
                case 'warn':
                    console.warn(formatted);
                    break;
                case 'error':
                    console.error(formatted);
                    break;
            }
        }
        else {
            const stream = target === 'stderr' ? process.stderr : process.stdout;
            stream.write(formatted + '\n');
        }
    }
    debug(message, meta) {
        if (this.shouldLog('debug'))
            this._write('debug', 'stdout', message, meta);
    }
    info(message, meta) {
        if (this.shouldLog('info'))
            this._write('info', 'stdout', message, meta);
    }
    warn(message, meta) {
        if (this.shouldLog('warn'))
            this._write('warn', 'stderr', message, meta);
    }
    error(message, meta) {
        if (this.shouldLog('error'))
            this._write('error', 'stderr', message, meta);
    }
}
/** No-op logger for zero overhead when logging is disabled */
export const NULL_LOGGER = new Logger({ level: 'silent' });

/** @stelar-time-real Logger */
const PRIORITY = { debug: 0, info: 1, warn: 2, error: 3, silent: 4 };
const C = { debug: '\x1b[36m', info: '\x1b[32m', warn: '\x1b[33m', error: '\x1b[31m', reset: '\x1b[0m' };
const isBrowser = typeof window !== 'undefined' && typeof process === 'undefined';
export class Logger {
    constructor(o = {}) {
        this.level = o.level || 'info';
        this.ts = o.timestamp !== false;
        this.pfx = o.prefix || 'stelar';
        this.color = isBrowser ? false : o.colorize !== false;
    }
    setLevel(l) { this.level = l; return this; }
    fmt(lvl, msg, meta) {
        const p = [];
        if (this.ts)
            p.push(new Date().toISOString());
        p.push(this.color ? `${C[lvl] || ''}[${this.pfx}:${lvl}]${C.reset}` : `[${this.pfx}:${lvl}]`);
        p.push(msg);
        if (meta && Object.keys(meta).length)
            try {
                p.push(JSON.stringify(meta));
            }
            catch {
                p.push('[circular]');
            }
        return p.join(' ');
    }
    w(lvl, err, msg, meta) {
        if (PRIORITY[lvl] < PRIORITY[this.level])
            return;
        const f = this.fmt(lvl, msg, meta);
        if (isBrowser)
            console[lvl]?.(f);
        else
            (err ? process.stderr : process.stdout).write(f + '\n');
    }
    debug(m, meta) { this.w('debug', false, m, meta); }
    info(m, meta) { this.w('info', false, m, meta); }
    warn(m, meta) { this.w('warn', true, m, meta); }
    error(m, meta) { this.w('error', true, m, meta); }
}
export const NULL_LOGGER = new Logger({ level: 'silent' });

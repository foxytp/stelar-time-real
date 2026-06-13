/**
 * @stelar-time-real Logger
 * Zero-dependency structured logger with levels. Works in Node.js and browser.
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';
export interface LoggerOptions {
    level?: LogLevel;
    timestamp?: boolean;
    prefix?: string;
    colorize?: boolean;
}
export declare class Logger {
    private level;
    private timestamp;
    private prefix;
    private colorize;
    constructor(options?: LoggerOptions);
    setLevel(level: LogLevel): this;
    private shouldLog;
    private format;
    private _write;
    debug(message: string, meta?: Record<string, unknown>): void;
    info(message: string, meta?: Record<string, unknown>): void;
    warn(message: string, meta?: Record<string, unknown>): void;
    error(message: string, meta?: Record<string, unknown>): void;
}
/** No-op logger for zero overhead when logging is disabled */
export declare const NULL_LOGGER: Logger;
//# sourceMappingURL=logger.d.ts.map
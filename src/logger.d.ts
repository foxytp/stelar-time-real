/** @stelar-time-real Logger */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';
export interface LoggerOptions {
    level?: LogLevel;
    timestamp?: boolean;
    prefix?: string;
    colorize?: boolean;
}
export declare class Logger {
    private level;
    private ts;
    private pfx;
    private color;
    constructor(o?: LoggerOptions);
    setLevel(l: LogLevel): this;
    private fmt;
    private w;
    debug(m: string, meta?: Record<string, unknown>): void;
    info(m: string, meta?: Record<string, unknown>): void;
    warn(m: string, meta?: Record<string, unknown>): void;
    error(m: string, meta?: Record<string, unknown>): void;
}
export declare const NULL_LOGGER: Logger;
//# sourceMappingURL=logger.d.ts.map
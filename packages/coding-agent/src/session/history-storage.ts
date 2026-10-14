import { Database, type SQLQueryBindings, type Statement } from "bun:sqlite";
import * as fs from "node:fs";
import * as path from "node:path";
import {
	assertSqliteQuickCheck,
	checkpointWal,
	getHistoryDbPath,
	isEnoent,
	isSqliteBusyError,
	isSqliteCorruptionError,
	logger,
	openSqliteDatabaseSync,
	postmortem,
	readDamagedSqliteRows,
} from "@oh-my-pi/pi-utils";
import { salvageSessionIndex } from "./session-index";

/** A unique prompt with provenance from its most recent submission. */
export interface HistoryEntry {
	/** Stable row identifier used by the full-text index. */
	id: number;
	/** Trimmed prompt text, unique across the history database. */
	prompt: string;
	/** Unix timestamp of the most recent submission. */
	created_at: number;
	/** Project working directory of the most recent submission. */
	cwd?: string;
	/** Session ID of the most recent submission, if known. */
	sessionId?: string;
	/** Number of times this prompt has been submitted, including the first. */
	useCount: number;
}

/** Narrows history reads; omitted fields match every prompt. */
export interface HistoryFilter {
	/** Exact project working directory of the prompt's latest submission. */
	cwd?: string;
	/** Exact session id of the prompt's latest submission. */
	sessionId?: string;
}

type HistoryRow = {
	id: number;
	prompt: string;
	created_at: number;
	cwd: string | null;
	session_id: string | null;
	use_count: number;
};

const SQLITE_NOW_EPOCH = "CAST(strftime('%s','now') AS INTEGER)";

/**
 * SQL predicate for a {@link HistoryFilter}, bound as numbered parameters
 * `?first` (cwd) and `?first+1` (session id); binding NULL disables that half.
 * `table` qualifies the columns (`"h."`) when the statement joins.
 */
function historyFilterClause(table: string, first: number): string {
	const session = first + 1;
	return `(?${first} IS NULL OR ${table}cwd = ?${first}) AND (?${session} IS NULL OR ${table}session_id = ?${session})`;
}

/**
 * Admits only rows with the stored shape. A damaged b-tree can surface another
 * table's record through `history` (e.g. an FTS docsize BLOB decoded as
 * `prompt`); TEXT affinity never coerces a BLOB, so without this a
 * non-string `prompt` reaches callers.
 */
function wellFormedRowClause(table: string): string {
	return `typeof(${table}prompt) = 'text' AND typeof(${table}created_at) = 'integer'`;
}

// Escape LIKE wildcards so user input is treated as literal text.
// Matches the `ESCAPE '\\'` clause used by substring-search statements.
function escapeLikePattern(text: string): string {
	return text.replace(/[\\%_]/g, "\\$&");
}

/**
 * Canonical stored form of a prompt: CRLF/CR folded to LF, trailing whitespace
 * stripped from every line, outer whitespace trimmed. Terminal copies pad each
 * line with spaces to the screen width, so without this a resubmitted copy of
 * an existing prompt lands as a byte-distinct duplicate row.
 */
function normalizePrompt(prompt: string): string {
	return prompt
		.replace(/\r\n?/g, "\n")
		.replace(/[^\S\n]+\n/g, "\n")
		.trim();
}

/**
 * Folds rows onto their normalized prompt: the most recent submission wins
 * (the upsert's "latest provenance" rule) and every collapsed row's use count
 * is summed, since each was a submission.
 */
function collapseByPrompt(rows: HistoryRow[]): HistoryRow[] {
	const winners = new Map<string, HistoryRow>();
	for (const row of rows) {
		const prompt = normalizePrompt(row.prompt);
		if (!prompt) continue;
		const incumbent = winners.get(prompt);
		if (!incumbent) {
			winners.set(prompt, { ...row, prompt });
			continue;
		}
		const useCount = incumbent.use_count + row.use_count;
		const rowWins =
			row.created_at > incumbent.created_at || (row.created_at === incumbent.created_at && row.id > incumbent.id);
		winners.set(prompt, rowWins ? { ...row, prompt, use_count: useCount } : { ...incumbent, use_count: useCount });
	}
	return [...winners.values()];
}

/** Selects the canonical row columns, defaulting those that predate `session_id` and `use_count`. */
function historyRowColumns(db: Database): string {
	const names = new Set(
		(db.query("PRAGMA table_info(history)").all() as Array<{ name: string }>).map(col => col.name),
	);
	const sessionId = names.has("session_id") ? "session_id" : "NULL AS session_id";
	const useCount = names.has("use_count") ? "use_count" : "1 AS use_count";
	return `id, prompt, created_at, cwd, ${sessionId}, ${useCount}`;
}

/**
 * Bumped only when stored rows need the one-time dump-and-rebuild pass on open;
 * see `#rebuildHistory`. Purely additive columns go through `#ensureColumn`
 * instead so existing stores are never rewritten for them.
 * v2: verify integrity first, so stores damaged under v1 are quarantined and
 * salvaged instead of rewritten in place.
 */
const HISTORY_DATA_VERSION = 2;

/** Canonical `history` schema; `#rebuildHistory` recreates the table from this exact DDL. */
const HISTORY_TABLE_DDL = `
CREATE TABLE IF NOT EXISTS history (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	prompt TEXT NOT NULL UNIQUE,
	created_at INTEGER NOT NULL DEFAULT (${SQLITE_NOW_EPOCH}),
	cwd TEXT,
	session_id TEXT,
	use_count INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_history_created_at ON history(created_at DESC);
`;

let cancelExitCleanup: (() => void) | undefined;

/**
 * Names the sidecar that records a quarantined store still needing salvage.
 * A peer can hold the replacement's write lock past the busy timeout, so the
 * salvage insert runs only when this file can be dropped afterwards.
 */
function pendingSalvagePath(dbPath: string): string {
	return `${dbPath}.pending-salvage`;
}

/** Backup path recorded by a deferred salvage, if any. */
function readPendingSalvage(dbPath: string): string | undefined {
	try {
		const backup = fs.readFileSync(pendingSalvagePath(dbPath), "utf8").trim();
		return backup || undefined;
	} catch (error) {
		if (!isEnoent(error)) logger.warn("Pending history salvage marker unreadable", { error: String(error) });
		return undefined;
	}
}

/** Drops the marker only when it still names `backupPath`, so a newer marker survives. */
function clearPendingSalvage(dbPath: string, backupPath: string): void {
	if (readPendingSalvage(dbPath) !== backupPath) return;
	try {
		fs.rmSync(pendingSalvagePath(dbPath), { force: true });
	} catch (error) {
		logger.warn("Pending history salvage marker could not be removed", { error: String(error) });
	}
}

/** Stores searchable prompts with only their latest project and session metadata. */
export class HistoryStorage {
	#db: Database;
	#dbPath: string;
	static #instance?: HistoryStorage;
	#sessionResolver?: () => string | undefined;
	#addListener?: () => void;
	#errorListener?: (error: unknown) => void;
	#writeFailureReported = false;

	// Prepared statements
	#upsertRowStmt: Statement;
	#recentStmt: Statement;
	#searchStmt: Statement;
	// Cache substring-fallback prepared statements keyed by token count.
	#substringStmts = new Map<number, Statement>();

	private constructor(dbPath: string, db: Database) {
		this.#dbPath = dbPath;
		this.#db = db;

		// One-shot statements go through the query cache, which close() finalizes; a
		// stray prepare() would turn close() into a zombie that keeps the files open.
		const hadFts = this.#db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='history_fts'").get();
		this.#db.run(`
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
${HISTORY_TABLE_DDL}
		`);

		// Additive column on stores created before the counter existed; runs before
		// the rebuild so the dump below always sees it.
		this.#ensureColumn("use_count", "INTEGER NOT NULL DEFAULT 1");
		const rebuilt = this.#rebuildHistory();

		this.#db.run(`
CREATE VIRTUAL TABLE IF NOT EXISTS history_fts USING fts5(prompt, content='history', content_rowid='id');

CREATE TRIGGER IF NOT EXISTS history_ai AFTER INSERT ON history BEGIN
	INSERT INTO history_fts(rowid, prompt) VALUES (new.id, new.prompt);
END;
		`);

		if (rebuilt || !hadFts) {
			try {
				this.#db.run("INSERT INTO history_fts(history_fts) VALUES('rebuild')");
			} catch (error) {
				logger.warn("HistoryStorage FTS rebuild failed", { error: String(error) });
			}
		}
		this.#recentStmt = this.#db.prepare(
			`SELECT id, prompt, created_at, cwd, session_id, use_count FROM history WHERE ${wellFormedRowClause("")} AND ${historyFilterClause("", 1)} ORDER BY created_at DESC, id DESC LIMIT ?3`,
		);
		this.#searchStmt = this.#db.prepare(
			`SELECT h.id, h.prompt, h.created_at, h.cwd, h.session_id, h.use_count FROM history_fts f JOIN history h ON h.id = f.rowid WHERE history_fts MATCH ?1 AND ${wellFormedRowClause("h.")} AND ${historyFilterClause("h.", 2)} ORDER BY h.created_at DESC, h.id DESC LIMIT ?4`,
		);
		this.#upsertRowStmt = this.#db.prepare(`
INSERT INTO history (prompt, created_at, cwd, session_id)
VALUES (?, ${SQLITE_NOW_EPOCH}, ?, ?)
ON CONFLICT(prompt) DO UPDATE SET
	created_at = excluded.created_at,
	cwd = excluded.cwd,
	session_id = excluded.session_id,
	use_count = history.use_count + 1
		`);
	}

	/**
	 * Opens the process-wide prompt history database. A corrupt store is
	 * quarantined once, and its readable prompts are salvaged into the
	 * replacement.
	 */
	static open(dbPath: string = getHistoryDbPath()): HistoryStorage {
		const existing = HistoryStorage.#instance;
		if (existing) return existing;

		fs.mkdirSync(path.dirname(dbPath), { recursive: true });
		let quarantinedPath: string | undefined;
		return openSqliteDatabaseSync(
			dbPath,
			db => {
				const instance = new HistoryStorage(dbPath, db);
				const salvageSource = quarantinedPath ?? readPendingSalvage(dbPath);
				if (salvageSource) instance.#salvageFrom(salvageSource);
				// Exit-only: a keep-alive cleanup leaves the handle valid so the editor can
				// keep submitting prompts; the real exit closes. Register before publishing
				// so a real-exit-in-progress late registration cannot close this instance.
				cancelExitCleanup = postmortem.register("history-storage", () => HistoryStorage.close(), {
					exitOnly: true,
				});
				HistoryStorage.#instance = instance;
				return instance;
			},
			{
				recoverCorruption: true,
				onCorruptionPreserved: backupPath => {
					quarantinedPath = backupPath;
				},
			},
		);
	}

	/** Checkpoints and closes the process-wide database, and permits reopening it. */
	static close(): void {
		const instance = HistoryStorage.#instance;
		HistoryStorage.#instance = undefined;
		cancelExitCleanup?.();
		cancelExitCleanup = undefined;
		if (instance) instance.#close();
	}

	#close(): void {
		checkpointWal(this.#db);
		for (const stmt of this.#substringStmts.values()) stmt.finalize();
		this.#substringStmts.clear();
		this.#upsertRowStmt.finalize();
		this.#recentStmt.finalize();
		this.#searchStmt.finalize();
		this.#db.close();
	}

	#insertBatch(rows: Array<Pick<HistoryEntry, "prompt" | "cwd" | "sessionId">>): void {
		this.#db.transaction((rows: Array<Pick<HistoryEntry, "prompt" | "cwd" | "sessionId">>) => {
			for (const row of rows) {
				this.#upsertRowStmt.run(row.prompt, row.cwd ?? null, row.sessionId ?? null);
			}
		})(rows);
	}

	/**
	 * Register a resolver that supplies the current session ID for prompts added
	 * without an explicit `sessionId`. Evaluated synchronously at `add()` time so
	 * each write captures the session active when the prompt was submitted.
	 */
	setSessionResolver(resolver: () => string | undefined): void {
		this.#sessionResolver = resolver;
	}

	/** Register a callback run after each successful {@link add}, once the row is durable. */
	setAddListener(listener: () => void): void {
		this.#addListener = listener;
	}

	/** Register a callback for the first failed write in each outage, reset by a successful write. */
	setErrorListener(listener: (error: unknown) => void): void {
		this.#errorListener = listener;
	}

	/**
	 * Stores a prompt, replaces its provenance with the latest submission, and
	 * bumps its use count on resubmission.
	 * The write is synchronous: prompt submission is human-paced, not a hot
	 * path. On success the row is durable the moment `add()` returns and cannot
	 * be lost to an exit racing a deferred flush. Failures are logged, not thrown;
	 * the error listener is notified once per outage until a write succeeds.
	 */
	add(prompt: string, cwd?: string, sessionId?: string): Promise<void> {
		const trimmed = normalizePrompt(prompt);
		if (!trimmed) return Promise.resolve();
		const session = sessionId ?? this.#sessionResolver?.();
		try {
			this.#insertBatch([{ prompt: trimmed, cwd: cwd ?? undefined, sessionId: session || undefined }]);
		} catch (error) {
			logger.error("HistoryStorage add failed", { error: String(error) });
			if (!this.#writeFailureReported) {
				this.#writeFailureReported = true;
				this.#errorListener?.(error);
			}
			return Promise.resolve();
		}
		this.#writeFailureReported = false;
		this.#addListener?.();
		return Promise.resolve();
	}

	/** Returns unique prompts ordered by their most recent submission. */
	getRecent(limit: number, filter: HistoryFilter = {}): HistoryEntry[] {
		const safeLimit = this.#normalizeLimit(limit);
		if (safeLimit === 0) return [];

		try {
			const rows = this.#recentStmt.all(filter.cwd ?? null, filter.sessionId ?? null, safeLimit) as HistoryRow[];
			return rows.map(row => this.#toEntry(row));
		} catch (error) {
			logger.error("HistoryStorage getRecent failed", { error: String(error) });
			return [];
		}
	}

	/** Finds unique prompts matching every query token, newest first. */
	search(query: string, limit: number, filter: HistoryFilter = {}): HistoryEntry[] {
		const safeLimit = this.#normalizeLimit(limit);
		if (safeLimit === 0) return [];

		const tokens = this.#tokenize(query);
		if (tokens.length === 0) return [];

		// 1. FTS5 prefix match (token AND, prefix-wildcard per token).
		//    Handles punctuation by tokenizing query the same way unicode61 tokenizer
		//    indexed the stored text, so "git-commit" -> "git"* "commit"*.
		const ftsQuery = tokens.map(tok => `"${tok.replace(/"/g, '""')}"*`).join(" ");
		let ftsRows: HistoryRow[] = [];
		try {
			ftsRows = this.#searchStmt.all(
				ftsQuery,
				filter.cwd ?? null,
				filter.sessionId ?? null,
				safeLimit,
			) as HistoryRow[];
		} catch (error) {
			// Malformed FTS expression - fall through to substring path.
			logger.debug("HistoryStorage FTS query failed, using substring only", { error: String(error) });
		}

		// 2. Substring fallback (token-AND LIKE). Catches infix matches FTS5's
		//    prefix-only wildcard cannot reach (e.g. "mit" -> "commit"). Bounded
		//    by safeLimit, ordered by recency - no full-table load into JS.
		let subRows: HistoryRow[] = [];
		try {
			subRows = this.#searchSubstring(tokens, safeLimit, filter);
		} catch (error) {
			logger.error("HistoryStorage substring search failed", { error: String(error) });
		}

		if (ftsRows.length === 0) {
			return subRows.map(row => this.#toEntry(row));
		}

		const rowsById = new Map<number, HistoryRow>();
		for (const row of ftsRows) {
			rowsById.set(row.id, row);
		}
		for (const row of subRows) {
			if (!rowsById.has(row.id)) rowsById.set(row.id, row);
		}

		return [...rowsById.values()]
			.sort((a, b) => b.created_at - a.created_at || b.id - a.id)
			.slice(0, safeLimit)
			.map(row => this.#toEntry(row));
	}

	/**
	 * IDs of the sessions whose stored prompts match `query`, ordered by prompt
	 * recency and de-duplicated. Used to augment session ranking in the resume
	 * picker with prompts that the 4KB session-list prefix never sees.
	 */
	matchingSessionIds(query: string, limit = 500): string[] {
		const seen = new Set<string>();
		const ids: string[] = [];
		for (const entry of this.search(query, limit)) {
			const id = entry.sessionId;
			if (!id || seen.has(id)) continue;
			seen.add(id);
			ids.push(id);
		}
		return ids;
	}

	#historySchemaHasColumn(column: string): boolean {
		const columns = this.#db.query("PRAGMA table_info(history)").all() as Array<{ name: string }>;
		return columns.some(col => col.name === column);
	}

	/** Adds `column` to `history` when absent; `definition` must carry a default so existing rows stay valid. */
	#ensureColumn(column: string, definition: string): void {
		if (this.#historySchemaHasColumn(column)) return;
		this.#db.run(`ALTER TABLE history ADD COLUMN ${column} ${definition}`);
	}

	/**
	 * Runs the open-time integrity check, then performs the one-time
	 * dump-and-rebuild pass when `PRAGMA user_version` (owned by this pass —
	 * nothing else versions history.db) is behind. A current store is checked
	 * in a plain read snapshot: taking the write lock first would queue a
	 * healthy open behind a peer's long write transaction and fail with
	 * `SQLITE_BUSY` at the busy timeout instead of opening concurrently under
	 * WAL. When a rebuild is needed, the whole pass runs under a single
	 * IMMEDIATE write lock — rechecking the version inside it — so the dump
	 * snapshots the same table the DROP replaces. A failed check throws
	 * `SQLITE_CORRUPT` and `open` quarantines and salvages the store instead —
	 * dropping tables on a damaged store would free pages other b-trees still
	 * reference. Dumps every well-formed row, folds the prompts through
	 * {@link collapseByPrompt}, and recreates the table from
	 * {@link HISTORY_TABLE_DDL}. Subsumes every legacy shape at once — unixepoch
	 * defaults, missing session_id, non-unique prompt, per-line trailing
	 * padding — without per-shape SQL migrations. Returns whether it ran so the
	 * caller can rebuild the FTS index.
	 */
	#rebuildHistory(): boolean {
		const readVersion = () => this.#db.query<{ user_version: number }, []>("PRAGMA user_version").get()!.user_version;
		if (readVersion() >= HISTORY_DATA_VERSION) {
			assertSqliteQuickCheck(this.#db);
			return false;
		}
		let rebuilt = false;
		this.#db
			.transaction(() => {
				if (readVersion() >= HISTORY_DATA_VERSION) return;
				assertSqliteQuickCheck(this.#db);
				let rows: HistoryRow[];
				try {
					rows = this.#db
						.query(`SELECT ${historyRowColumns(this.#db)} FROM history WHERE ${wellFormedRowClause("")}`)
						.all() as HistoryRow[];
				} catch (error) {
					if (isSqliteCorruptionError(error)) throw error;
					logger.error("HistoryStorage rebuild dump failed", { error: String(error) });
					return;
				}
				const winners = collapseByPrompt(rows);
				this.#db.run("DROP INDEX IF EXISTS idx_history_created_at");
				this.#db.run("DROP TRIGGER IF EXISTS history_ai");
				this.#db.run("DROP TABLE IF EXISTS history_fts");
				this.#db.run("DROP TABLE history");
				this.#db.run(HISTORY_TABLE_DDL);
				this.#insertRows(winners);
				this.#db.run(`PRAGMA user_version = ${HISTORY_DATA_VERSION}`);
				rebuilt = true;
				if (winners.length < rows.length) {
					logger.debug("HistoryStorage collapsed rows during rebuild", {
						before: rows.length,
						after: winners.length,
					});
				}
			})
			.immediate();
		return rebuilt;
	}

	/**
	 * Inserts rows, keeping each original id unless it is taken, and merges a
	 * prompt that already exists like {@link collapseByPrompt}. A rebuild
	 * inserts into an empty table; a salvage can race a peer process that
	 * already wrote to the replacement store, and neither side's prompts may be
	 * dropped.
	 */
	#insertRows(rows: HistoryRow[]): void {
		const insert = this.#db.query(`
INSERT INTO history (id, prompt, created_at, cwd, session_id, use_count)
VALUES (CASE WHEN EXISTS (SELECT 1 FROM history WHERE id = ?1) THEN NULL ELSE ?1 END, ?2, ?3, ?4, ?5, ?6)
ON CONFLICT(prompt) DO UPDATE SET
	use_count = history.use_count + excluded.use_count,
	created_at = max(history.created_at, excluded.created_at),
	cwd = CASE WHEN excluded.created_at > history.created_at THEN excluded.cwd ELSE history.cwd END,
	session_id = CASE WHEN excluded.created_at > history.created_at THEN excluded.session_id ELSE history.session_id END
		`);
		for (const row of rows) {
			insert.run(row.id, row.prompt, row.created_at, row.cwd, row.session_id, row.use_count);
		}
	}

	/**
	 * Copies every readable, well-formed row of a quarantined store, including
	 * the session-index tables it shares, into this fresh one. The prompt
	 * insert is booked by a `<db>.pending-salvage` marker written before the
	 * transaction and removed after it: a `SQLITE_BUSY` racing a peer's write
	 * lock (or a crash mid-salvage) leaves the marker, so the next {@link open}
	 * retries instead of stranding the readable prompts in the backup. Other
	 * failures are logged, the marker is cleared, and this store stays usable.
	 */
	#salvageFrom(backupPath: string): void {
		let source: Database;
		try {
			source = new Database(backupPath, { readonly: true });
		} catch (error) {
			logger.error("HistoryStorage salvage failed", { backupPath, error: String(error) });
			// The recorded backup is gone or unreadable; the marker points nowhere.
			clearPendingSalvage(this.#dbPath, backupPath);
			return;
		}
		try {
			const { rows, damagedRegions } = readDamagedSqliteRows<HistoryRow>(
				source,
				"history",
				historyRowColumns(source),
				wellFormedRowClause(""),
			);
			const salvaged = collapseByPrompt(rows);
			try {
				fs.writeFileSync(pendingSalvagePath(this.#dbPath), `${backupPath}\n`);
			} catch (error) {
				logger.warn("Pending history salvage marker could not be written", { error: String(error) });
			}
			this.#db.transaction(() => this.#insertRows(salvaged))();
			clearPendingSalvage(this.#dbPath, backupPath);
			logger.warn("HistoryStorage salvaged prompts from a corrupt store", {
				backupPath,
				salvaged: salvaged.length,
				damagedRegions,
			});
		} catch (error) {
			if (!isSqliteBusyError(error)) clearPendingSalvage(this.#dbPath, backupPath);
			logger.error("HistoryStorage salvage failed", {
				backupPath,
				deferred: isSqliteBusyError(error),
				error: String(error),
			});
		}
		try {
			salvageSessionIndex(source, this.#db);
		} finally {
			source.close();
		}
	}

	#normalizeLimit(limit: number): number {
		if (!Number.isFinite(limit)) return 0;
		const clamped = Math.max(0, Math.floor(limit));
		return Math.min(clamped, 1000);
	}

	/**
	 * Split on non-alphanumeric runs, mirroring FTS5's `unicode61` tokenizer so
	 * query tokens align with how stored prompts were indexed. Lowercases for
	 * stable substring matching.
	 */
	#tokenize(query: string): string[] {
		return query
			.toLowerCase()
			.split(/[^\p{L}\p{N}]+/u)
			.filter(tok => tok.length > 0);
	}

	#searchSubstring(tokens: string[], limit: number, filter: HistoryFilter): HistoryRow[] {
		const stmt = this.#getSubstringStmt(tokens.length);
		const params: SQLQueryBindings[] = tokens.map(tok => `%${escapeLikePattern(tok)}%`);
		params.push(filter.cwd ?? null, filter.sessionId ?? null, limit);
		return stmt.all(...params) as HistoryRow[];
	}

	#getSubstringStmt(tokenCount: number): Statement {
		let stmt = this.#substringStmts.get(tokenCount);
		if (stmt) return stmt;
		const tokenClauses = Array.from(
			{ length: tokenCount },
			(_, index) => `prompt LIKE ?${index + 1} ESCAPE '\\' COLLATE NOCASE`,
		);
		const whereClause = [...tokenClauses, wellFormedRowClause(""), historyFilterClause("", tokenCount + 1)].join(
			" AND ",
		);
		stmt = this.#db.prepare(
			`SELECT id, prompt, created_at, cwd, session_id, use_count FROM history WHERE ${whereClause} ORDER BY created_at DESC, id DESC LIMIT ?${tokenCount + 3}`,
		);
		this.#substringStmts.set(tokenCount, stmt);
		return stmt;
	}

	#toEntry(row: HistoryRow): HistoryEntry {
		return {
			id: row.id,
			prompt: row.prompt,
			created_at: row.created_at,
			cwd: row.cwd ?? undefined,
			sessionId: row.session_id ?? undefined,
			useCount: row.use_count,
		};
	}
}

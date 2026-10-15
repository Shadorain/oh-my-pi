import { Database, type SQLQueryBindings, type Statement } from "bun:sqlite";
import * as fs from "node:fs";
import * as path from "node:path";
import {
	assertSqliteQuickCheck,
	getHistoryDbPath,
	isEnoent,
	isSqliteCorruptionError,
	logger,
	openSqliteDatabaseSync,
	readDamagedSqliteRows,
	resolveEquivalentPath,
	sqliteFileIdentity,
	withSqliteStoreLease,
} from "@oh-my-pi/pi-utils";
import { writeFileAtomicallySync } from "../utils/atomic-file";
import { readSalvagedSessionIndex, restoreSalvagedSessionIndex, type SalvagedSessionIndex } from "./session-index";

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

type SalvagedHistoryRow = Omit<HistoryRow, "id"> & { id: string };

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
function collapseByPrompt(rows: Array<HistoryRow | SalvagedHistoryRow>): SalvagedHistoryRow[] {
	const winners = new Map<string, SalvagedHistoryRow>();
	for (const row of rows) {
		const prompt = normalizePrompt(row.prompt);
		if (!prompt) continue;
		const incumbent = winners.get(prompt);
		if (!incumbent) {
			winners.set(prompt, { ...row, id: String(row.id), prompt });
			continue;
		}
		const useCount = incumbent.use_count + row.use_count;
		const rowWins =
			row.created_at > incumbent.created_at ||
			(row.created_at === incumbent.created_at && BigInt(row.id) > BigInt(incumbent.id));
		winners.set(
			prompt,
			rowWins ? { ...row, id: String(row.id), prompt, use_count: useCount } : { ...incumbent, use_count: useCount },
		);
	}
	return [...winners.values()];
}

/** Selects the canonical row columns, defaulting those that predate `session_id` and `use_count`. */
function historyRowColumns(db: Database, losslessId = false): string {
	const names = new Set(
		(db.query("PRAGMA table_info(history)").all() as Array<{ name: string }>).map(col => col.name),
	);
	const id = losslessId ? "CAST(id AS TEXT) AS id" : "id";
	const sessionId = names.has("session_id") ? "session_id" : "NULL AS session_id";
	const useCount = names.has("use_count") ? "use_count" : "1 AS use_count";
	return `${id}, prompt, created_at, cwd, ${sessionId}, ${useCount}`;
}

/** Keep the allocation high-water mark in SQLite's integer domain, including values above 2^53. */
function readHistorySequence(db: Database): string | null {
	if (!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'").get()) return null;
	try {
		const row = db
			.query<{ seq: string }, []>(
				"SELECT CAST(seq AS TEXT) AS seq FROM sqlite_sequence WHERE name = 'history' AND typeof(seq)='integer' AND seq >= 0",
			)
			.get();
		return row?.seq ?? null;
	} catch (error) {
		logger.warn("HistoryStorage could not read the history sequence", { error: String(error) });
		return null;
	}
}

/** Predictor checkpoints must remain below newly allocated ids even when the newest rows were deleted. */
function restoreHistorySequence(db: Database, seq: string | null): void {
	if (seq === null) return;
	db.query("UPDATE sqlite_sequence SET seq = max(seq, CAST(? AS INTEGER)) WHERE name = 'history'").run(seq);
	db.query(
		"INSERT INTO sqlite_sequence (name, seq) SELECT 'history', CAST(? AS INTEGER) WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'history')",
	).run(seq);
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

/**
 * Names the sidecar that records a quarantined store still needing salvage.
 * A peer can hold the replacement's write lock past the busy timeout, so the
 * salvage insert runs only when this file can be dropped afterwards.
 */
function pendingSalvagePath(dbPath: string): string {
	return `${dbPath}.pending-salvage`;
}

interface PendingSalvage {
	backupName: string;
	sourceIdentity?: string;
}

/** GC must not delete rows that a queued snapshot can restore immediately afterwards. */
export function hasPendingHistorySalvage(dbPath: string): boolean {
	return readPendingSalvage(resolveEquivalentPath(dbPath, { allowMissing: true })).length > 0;
}

function readPendingSalvage(dbPath: string): PendingSalvage[] {
	try {
		const records: unknown = JSON.parse(fs.readFileSync(pendingSalvagePath(dbPath), "utf8"));
		if (!Array.isArray(records)) throw new Error("Invalid history salvage journal");
		return records.map((record: unknown) => {
			if (
				!record ||
				typeof record !== "object" ||
				!("backupName" in record) ||
				typeof record.backupName !== "string" ||
				!record.backupName ||
				path.basename(record.backupName) !== record.backupName ||
				record.backupName === "." ||
				record.backupName === ".." ||
				("sourceIdentity" in record && typeof record.sourceIdentity !== "string")
			)
				throw new Error("Invalid history salvage journal");
			return {
				backupName: record.backupName,
				sourceIdentity:
					"sourceIdentity" in record && typeof record.sourceIdentity === "string"
						? record.sourceIdentity
						: undefined,
			};
		});
	} catch (error) {
		if (!isEnoent(error)) throw error;
		return [];
	}
}

function recordPendingSalvage(dbPath: string, backupPath: string): void {
	const pending = readPendingSalvage(dbPath);
	const stat = fs.statSync(dbPath, { bigint: true });
	const sourceIdentity = `${stat.dev}:${stat.ino}:${stat.birthtimeNs}`;
	// A failed unlink leaves a writable original. Supersede its older snapshot
	// rather than replaying both or losing writes committed between retries.
	const remaining = pending.filter(record => record.sourceIdentity !== sourceIdentity);
	remaining.push({ backupName: path.basename(backupPath), sourceIdentity });
	writeFileAtomicallySync(pendingSalvagePath(dbPath), JSON.stringify(remaining));
}

function clearPendingSalvage(dbPath: string, backupPath: string): void {
	const backupName = path.basename(backupPath);
	const pending = readPendingSalvage(dbPath);
	const remaining = pending.filter(record => record.backupName !== backupName);
	if (remaining.length === pending.length) return;
	try {
		if (remaining.length) writeFileAtomicallySync(pendingSalvagePath(dbPath), JSON.stringify(remaining));
		else fs.rmSync(pendingSalvagePath(dbPath), { force: true });
	} catch (error) {
		logger.warn("Pending history salvage marker could not be removed", { error: String(error) });
	}
}

/** Stores searchable prompts with only their latest project and session metadata. */
export class HistoryStorage {
	#db!: Database;
	#dbPath: string;
	static #instance?: HistoryStorage;
	#sessionResolver?: () => string | undefined;
	#addListener?: () => void;
	#errorListener?: (error: unknown) => void;
	#writeFailureReported = false;
	#closed = false;
	// File generation whose integrity this process already proved; `undefined`
	// forces a scan, `null` (missing file) and real identities are checkable.
	#checkedIdentity: string | null | undefined;
	// Prepared statements
	#upsertRowStmt!: Statement;
	#recentStmt!: Statement;
	#searchStmt!: Statement;
	// Cache substring-fallback prepared statements keyed by token count.
	#substringStmts = new Map<number, Statement>();

	private constructor(dbPath: string) {
		this.#dbPath = resolveEquivalentPath(dbPath, { allowMissing: true });
	}

	#initialize(db: Database): void {
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
				if (isSqliteCorruptionError(error)) throw error;
				logger.warn("HistoryStorage FTS rebuild failed", { error: String(error) });
			}
		}
		this.#recentStmt = this.#db.query(
			`SELECT id, prompt, created_at, cwd, session_id, use_count FROM history WHERE ${wellFormedRowClause("")} AND ${historyFilterClause("", 1)} ORDER BY created_at DESC, id DESC LIMIT ?3`,
		);
		this.#searchStmt = this.#db.query(
			`SELECT h.id, h.prompt, h.created_at, h.cwd, h.session_id, h.use_count FROM history_fts f JOIN history h ON h.id = f.rowid WHERE history_fts MATCH ?1 AND ${wellFormedRowClause("h.")} AND ${historyFilterClause("h.", 2)} ORDER BY h.created_at DESC, h.id DESC LIMIT ?4`,
		);
		this.#upsertRowStmt = this.#db.query(`
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
		const instance = new HistoryStorage(dbPath);
		instance.#withStore(() => {});
		HistoryStorage.#instance = instance;
		return instance;
	}

	/** Releases the process-wide facade; connections are scoped to each operation. */
	static close(): void {
		if (HistoryStorage.#instance) HistoryStorage.#instance.#closed = true;
		HistoryStorage.#instance = undefined;
	}

	#withStore<T>(operation: () => T): T {
		if (this.#closed) throw new Error("HistoryStorage is closed");
		return withSqliteStoreLease(this.#dbPath, () => {
			for (let attempt = 0; ; attempt++) {
				const db = openSqliteDatabaseSync(
					this.#dbPath,
					database => {
						this.#initialize(database);
						const pending = readPendingSalvage(this.#dbPath);
						const live = pending.length ? sqliteFileIdentity(this.#dbPath) : undefined;
						if (pending.length && typeof live !== "string")
							throw new Error("Could not identify the history recovery target");
						// A newer snapshot carries the ledger of older committed backups.
						// Do not replay older entries until that ledger has been restored.
						for (let i = pending.length - 1; i >= 0; i--) {
							const backupPath = path.join(path.dirname(this.#dbPath), pending[i].backupName);
							// A failed unlink can leave the source live in an already-open
							// peer. Its snapshot is redundant, not another submission.
							if (pending[i].sourceIdentity === live) {
								clearPendingSalvage(this.#dbPath, backupPath);
								continue;
							}
							if (!this.#salvageFrom(backupPath)) break;
						}
						return database;
					},
					{
						recoverCorruption: true,
						onCorruptionPreserved: backupPath => recordPendingSalvage(this.#dbPath, backupPath),
					},
				);
				try {
					return operation();
				} catch (error) {
					// The open-time check is generation-gated, so damage done after
					// that scan first surfaces as an operation error. Invalidate the
					// generation proof and retry once: the re-scan recovers a corrupt
					// store through the same path an open-time failure does.
					if (attempt > 0 || !isSqliteCorruptionError(error)) throw error;
					this.#checkedIdentity = undefined;
				} finally {
					this.#substringStmts.clear();
					db.close();
				}
			}
		});
	}

	#insertBatch(rows: Array<Pick<HistoryEntry, "prompt" | "cwd" | "sessionId">>): void {
		this.#withStore(() => {
			this.#db.transaction(() => {
				for (const row of rows) {
					this.#upsertRowStmt.run(row.prompt, row.cwd ?? null, row.sessionId ?? null);
				}
			})();
		});
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
			return this.#withStore(() => {
				const rows = this.#recentStmt.all(filter.cwd ?? null, filter.sessionId ?? null, safeLimit) as HistoryRow[];
				return rows.map(row => this.#toEntry(row));
			});
		} catch (error) {
			logger.error("HistoryStorage getRecent failed", { error: String(error) });
			return [];
		}
	}

	/** Finds unique prompts matching every query token, newest first. */
	search(query: string, limit: number, filter: HistoryFilter = {}): HistoryEntry[] {
		try {
			return this.#withStore(() => this.#search(query, limit, filter));
		} catch (error) {
			logger.error("HistoryStorage search failed", { error: String(error) });
			return [];
		}
	}

	#search(query: string, limit: number, filter: HistoryFilter): HistoryEntry[] {
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
			if (isSqliteCorruptionError(error)) throw error;
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
			if (isSqliteCorruptionError(error)) throw error;
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
			// A full-store scan is O(pages); gate it to file generations this
			// process hasn't proven — the generation changes exactly when a peer
			// or this process replaces the file.
			const identity = sqliteFileIdentity(this.#dbPath);
			if (identity !== undefined && identity === this.#checkedIdentity) return false;
			assertSqliteQuickCheck(this.#db);
			if (identity !== undefined) this.#checkedIdentity = identity;
			return false;
		}
		let rebuilt = false;
		this.#db
			.transaction(() => {
				if (readVersion() >= HISTORY_DATA_VERSION) return;
				assertSqliteQuickCheck(this.#db);
				const highWater = readHistorySequence(this.#db);
				let rows: SalvagedHistoryRow[];
				try {
					rows = this.#db
						.query(`SELECT ${historyRowColumns(this.#db, true)} FROM history WHERE ${wellFormedRowClause("")}`)
						.all() as SalvagedHistoryRow[];
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
				restoreHistorySequence(this.#db, highWater);
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
		if (rebuilt) {
			const identity = sqliteFileIdentity(this.#dbPath);
			if (identity !== undefined) this.#checkedIdentity = identity;
		}
		return rebuilt;
	}

	/**
	 * Inserts rows, keeping each original id unless it is taken, and merges a
	 * prompt that already exists like {@link collapseByPrompt}. A rebuild
	 * inserts into an empty table; a salvage can race a peer process that
	 * already wrote to the replacement store, and neither side's prompts may be
	 * dropped.
	 */
	#insertRows(rows: SalvagedHistoryRow[]): void {
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
	 * Prompts, index rows, and the applied-backup identity commit together.
	 * The durable pointer survives scanning, transaction failure, and a crash
	 * after COMMIT; the ledger makes a replay after COMMIT a no-op.
	 */
	#salvageFrom(backupPath: string): boolean {
		let source: Database | undefined;
		try {
			this.#db.run("PRAGMA synchronous=FULL");
			this.#db.run("CREATE TABLE IF NOT EXISTS history_salvage_applied (backup_path TEXT PRIMARY KEY)");
			const backupName = path.basename(backupPath);
			const applied = this.#db.query("SELECT 1 FROM history_salvage_applied WHERE backup_path = ?");
			if (applied.get(backupName)) {
				clearPendingSalvage(this.#dbPath, backupPath);
				return true;
			}
			const sourceErrors: string[] = [];
			const readSource = <T>(read: () => T, fallback: T): T => {
				try {
					return read();
				} catch (error) {
					if (
						!isSqliteCorruptionError(error) &&
						!isEnoent(error) &&
						!(
							error instanceof Error &&
							(("code" in error && error.code === "SQLITE_ERROR") || ("errno" in error && error.errno === 1))
						)
					)
						throw error;
					sourceErrors.push(String(error));
					return fallback;
				}
			};
			source = readSource(() => {
				fs.statSync(backupPath);
				return new Database(backupPath, { readonly: true });
			}, undefined);
			let highWater: string | null = null;
			let history: { rows: SalvagedHistoryRow[]; damagedRegions: number } = { rows: [], damagedRegions: 0 };
			let index: SalvagedSessionIndex | undefined;
			let carried: Array<{ backup_path: string }> = [];
			if (source) {
				const backup = source;
				carried = readSource(() => {
					if (!backup.query("SELECT 1 FROM sqlite_master WHERE name='history_salvage_applied'").get()) return [];
					return readDamagedSqliteRows<{ backup_path: string }>(
						backup,
						"history_salvage_applied",
						"backup_path",
						"typeof(backup_path)='text'",
					).rows;
				}, []);
				highWater = readSource(() => readHistorySequence(backup), null);
				history = readSource(
					() =>
						readDamagedSqliteRows<SalvagedHistoryRow>(
							backup,
							"history",
							historyRowColumns(backup, true),
							wellFormedRowClause(""),
						),
					history,
				);
				index = readSource(() => readSalvagedSessionIndex(backup), undefined);
				source.close();
				source = undefined;
			}
			const salvaged = collapseByPrompt(history.rows);
			this.#db
				.transaction(() => {
					if (applied.get(backupName)) return;
					restoreHistorySequence(this.#db, highWater);
					this.#insertRows(salvaged);
					if (index) restoreSalvagedSessionIndex(index, this.#db);
					const mark = this.#db.query("INSERT OR IGNORE INTO history_salvage_applied (backup_path) VALUES (?)");
					for (const row of carried) mark.run(path.basename(row.backup_path));
					mark.run(backupName);
				})
				.immediate();
			clearPendingSalvage(this.#dbPath, backupPath);
			logger.warn("HistoryStorage salvaged a corrupt store", {
				backupPath,
				salvaged: salvaged.length,
				damagedRegions: history.damagedRegions,
				sourceErrors,
			});
			return true;
		} catch (error) {
			// Source corruption was classified before touching the target.
			if (isSqliteCorruptionError(error)) throw error;
			logger.error("HistoryStorage salvage deferred", { backupPath, error: String(error) });
			return false;
		} finally {
			source?.close();
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
		stmt = this.#db.query(
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

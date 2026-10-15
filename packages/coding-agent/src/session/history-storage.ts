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
	sqliteFileIdentity,
	withSqliteStoreLease,
} from "@oh-my-pi/pi-utils";
import { writeFileAtomicallySync } from "../utils/atomic-file";
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
 * Reads `history`'s AUTOINCREMENT high-water mark. `null` when the store
 * predates AUTOINCREMENT or the sequence row itself is unreachable — losing the
 * mark degrades to pre-fix behavior, so a damaged `sqlite_sequence` never
 * blocks salvage.
 */
function readHistorySequence(db: Database): number | null {
	if (!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'").get()) return null;
	try {
		const row = db.query<{ seq: number }, []>("SELECT seq FROM sqlite_sequence WHERE name = 'history'").get();
		return typeof row?.seq === "number" ? row.seq : null;
	} catch (error) {
		logger.warn("HistoryStorage could not read the history sequence", { error: String(error) });
		return null;
	}
}

/**
 * Raises `history`'s sequence to `seq` without lowering it — row-preserving
 * inserts already advanced it to the largest surviving id. Predictor cursors
 * persist `WHERE id > ?` checkpoints across process restarts, so a rebuild or
 * recovery that resets the sequence makes fresh prompts land under the cursor
 * and skip learning until ids catch up.
 */
function restoreHistorySequence(db: Database, seq: number | null): void {
	if (seq === null) return;
	db.query("UPDATE sqlite_sequence SET seq = max(seq, ?) WHERE name = 'history'").run(seq);
	db.query(
		"INSERT INTO sqlite_sequence (name, seq) SELECT 'history', ? WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'history')",
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
	backupPath: string;
	sourceIdentity?: string;
}

/** Accepts the earlier plain-path journal as well as the durable journal record. */
function readPendingSalvage(dbPath: string): PendingSalvage | undefined {
	try {
		const content = fs.readFileSync(pendingSalvagePath(dbPath), "utf8").trim();
		if (!content) throw new Error("Empty history salvage journal");
		if (!content.startsWith("{")) return { backupPath: content };
		const record: unknown = JSON.parse(content);
		if (
			!record ||
			typeof record !== "object" ||
			!("backupPath" in record) ||
			typeof record.backupPath !== "string" ||
			!record.backupPath ||
			("sourceIdentity" in record && typeof record.sourceIdentity !== "string")
		)
			throw new Error("Invalid history salvage journal");
		return {
			backupPath: record.backupPath,
			sourceIdentity:
				"sourceIdentity" in record && typeof record.sourceIdentity === "string" ? record.sourceIdentity : undefined,
		};
	} catch (error) {
		if (!isEnoent(error)) throw error;
		return undefined;
	}
}

function recordPendingSalvage(dbPath: string, backupPath: string, preserving = false): void {
	const pending = readPendingSalvage(dbPath);
	if (pending?.backupPath === backupPath) return;
	let sourceIdentity: string | undefined;
	if (preserving) {
		const stat = fs.statSync(dbPath, { bigint: true });
		sourceIdentity = `${stat.dev}:${stat.ino}:${stat.birthtimeNs}`;
	}
	// An interrupted quarantine can copy the same original again. Retain its
	// first durable snapshot, but never discard a different pending generation.
	if (pending) {
		if (sourceIdentity && pending.sourceIdentity === sourceIdentity) return;
		throw new Error(`History recovery still pending from ${pending.backupPath}`);
	}
	writeFileAtomicallySync(pendingSalvagePath(dbPath), JSON.stringify({ backupPath, sourceIdentity }));
}

/** Drops the marker only when it still names `backupPath`, so a newer marker survives. */
function clearPendingSalvage(dbPath: string, backupPath: string): void {
	if (readPendingSalvage(dbPath)?.backupPath !== backupPath) return;
	try {
		fs.rmSync(pendingSalvagePath(dbPath), { force: true });
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
		this.#dbPath = dbPath;
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
						if (pending) this.#salvageFrom(pending.backupPath);
						return database;
					},
					{
						recoverCorruption: true,
						onCorruptionPreserved: backupPath => recordPendingSalvage(this.#dbPath, backupPath, true),
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
	 * Prompts, index rows, and the applied-backup identity commit together.
	 * The durable pointer survives scanning, transaction failure, and a crash
	 * after COMMIT; the ledger makes a replay after COMMIT a no-op.
	 */
	#salvageFrom(backupPath: string): void {
		let source: Database | undefined;
		try {
			recordPendingSalvage(this.#dbPath, backupPath);
			this.#db.run("CREATE TABLE IF NOT EXISTS history_salvage_applied (backup_path TEXT PRIMARY KEY)");
			const applied = this.#db.query("SELECT 1 FROM history_salvage_applied WHERE backup_path = ?");
			if (applied.get(backupPath)) {
				clearPendingSalvage(this.#dbPath, backupPath);
				return;
			}
			source = new Database(backupPath, { readonly: true });
			const highWater = readHistorySequence(source);
			const { rows, damagedRegions } = readDamagedSqliteRows<HistoryRow>(
				source,
				"history",
				historyRowColumns(source),
				wellFormedRowClause(""),
			);
			const salvaged = collapseByPrompt(rows);
			const backup = source;
			this.#db
				.transaction(() => {
					if (applied.get(backupPath)) return;
					this.#insertRows(salvaged);
					restoreHistorySequence(this.#db, highWater);
					salvageSessionIndex(backup, this.#db);
					this.#db.query("INSERT INTO history_salvage_applied (backup_path) VALUES (?)").run(backupPath);
				})
				.immediate();
			clearPendingSalvage(this.#dbPath, backupPath);
			logger.warn("HistoryStorage salvaged a corrupt store", {
				backupPath,
				salvaged: salvaged.length,
				damagedRegions,
			});
		} catch (error) {
			logger.error("HistoryStorage salvage deferred", { backupPath, error: String(error) });
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

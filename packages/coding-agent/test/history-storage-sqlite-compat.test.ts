import { Database } from "bun:sqlite";
import * as fs from "node:fs";
import { afterEach, beforeEach, expect, it, vi } from "bun:test";
import { HistoryStorage } from "@oh-my-pi/pi-coding-agent/session/history-storage";
import { TempDir } from "@oh-my-pi/pi-utils";
import { readTableSql } from "./helpers/sqlite-inspect";

const LEGACY_TIMESTAMP = 1_700_000_000;

let tempDir: TempDir | null = null;

beforeEach(() => {
	HistoryStorage.close();
});

afterEach(async () => {
	HistoryStorage.close();
	if (tempDir) {
		await Bun.sleep(0);
		await tempDir.remove().catch(() => {});
		tempDir = null;
	}
});

it("migrates legacy history schema away from unixepoch defaults", async () => {
	tempDir = TempDir.createSync("@omp-history-storage-legacy-");
	const dbPath = tempDir.join("history.db");
	const legacyDb = new Database(dbPath);
	legacyDb.exec(`
		CREATE TABLE history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			prompt TEXT NOT NULL,
			created_at INTEGER NOT NULL DEFAULT (unixepoch()),
			cwd TEXT
		);
	`);
	legacyDb
		.prepare("INSERT INTO history (prompt, created_at, cwd) VALUES (?, ?, ?)")
		.run("legacy prompt", LEGACY_TIMESTAMP, "/tmp/legacy");
	legacyDb.close();

	const storage = HistoryStorage.open(dbPath);
	await storage.add("new prompt", "/tmp/new");

	const db = new Database(dbPath, { readonly: true });
	try {
		const prompts = db.prepare("SELECT prompt FROM history ORDER BY id ASC").all() as Array<{ prompt: string }>;
		expect(prompts).toEqual([{ prompt: "legacy prompt" }, { prompt: "new prompt" }]);
		expect(readTableSql(dbPath, "history")).not.toContain("unixepoch(");
		expect(readTableSql(dbPath, "history")).toContain("strftime('%s','now')");
		const indexRow = db
			.prepare("SELECT 1 AS present FROM sqlite_master WHERE type = 'index' AND name = 'idx_history_created_at'")
			.get() as { present?: number } | undefined;
		expect(indexRow?.present).toBe(1);
		const ftsRow = db
			.prepare("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = 'history_fts'")
			.get() as { present?: number } | undefined;
		expect(ftsRow?.present).toBe(1);
	} finally {
		db.close();
	}
});
it("collapses duplicate prompts and keeps the latest project metadata", async () => {
	tempDir = TempDir.createSync("@omp-history-storage-deduplicate-");
	const dbPath = tempDir.join("history.db");
	const legacyDb = new Database(dbPath);
	legacyDb.exec(`
		CREATE TABLE history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			prompt TEXT NOT NULL,
			created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
			cwd TEXT,
			session_id TEXT
		);
	`);
	const insert = legacyDb.prepare("INSERT INTO history (prompt, created_at, cwd, session_id) VALUES (?, ?, ?, ?)");
	insert.run("shared prompt", 1, "/projects/first", "first-session");
	insert.run("shared prompt", 2, "/projects/second", "second-session");
	legacyDb.close();

	const storage = HistoryStorage.open(dbPath);
	expect(storage.getRecent(10)).toEqual([
		{
			id: 2,
			prompt: "shared prompt",
			created_at: 2,
			cwd: "/projects/second",
			sessionId: "second-session",
			useCount: 2,
		},
	]);

	await storage.add("shared prompt", "/projects/latest", "latest-session");

	const [latest] = storage.getRecent(10);
	expect(storage.getRecent(10)).toHaveLength(1);
	expect(latest?.cwd).toBe("/projects/latest");
	expect(latest?.sessionId).toBe("latest-session");
	expect(latest?.created_at).toBeGreaterThan(2);
	expect(latest?.useCount).toBe(3);
	expect(storage.search("shared", 10).map(entry => entry.sessionId)).toEqual(["latest-session"]);

	const verify = new Database(dbPath);
	try {
		expect(() => verify.prepare("INSERT INTO history (prompt) VALUES (?)").run("shared prompt")).toThrow();
	} finally {
		verify.close();
	}
});
it("adds use_count to a rebuilt store in place, without another rebuild", async () => {
	tempDir = TempDir.createSync("@omp-history-storage-use-count-");
	const dbPath = tempDir.join("history.db");
	const v1Db = new Database(dbPath);
	v1Db.exec(`
		CREATE TABLE history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			prompt TEXT NOT NULL UNIQUE,
			created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
			cwd TEXT,
			session_id TEXT
		);
		PRAGMA user_version = 2;
	`);
	v1Db
		.prepare("INSERT INTO history (id, prompt, created_at, cwd, session_id) VALUES (?, ?, ?, ?, ?)")
		.run(7, "counted prompt", LEGACY_TIMESTAMP, "/tmp/v1", "v1-session");
	v1Db.close();

	const storage = HistoryStorage.open(dbPath);
	expect(storage.getRecent(10)).toEqual([
		{
			id: 7,
			prompt: "counted prompt",
			created_at: LEGACY_TIMESTAMP,
			cwd: "/tmp/v1",
			sessionId: "v1-session",
			useCount: 1,
		},
	]);
	await storage.add("counted prompt", "/tmp/again", "again-session");
	expect(storage.getRecent(10).map(entry => [entry.id, entry.useCount])).toEqual([[7, 2]]);
	HistoryStorage.close();

	const verify = new Database(dbPath);
	try {
		expect(verify.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(2);
	} finally {
		verify.close();
	}
});

it("normalizes per-line trailing whitespace so padded resubmissions upsert instead of duplicating", async () => {
	tempDir = TempDir.createSync("@omp-history-storage-normalize-");
	const storage = HistoryStorage.open(tempDir.join("history.db"));

	await storage.add("line one   \t\r\nline two  ", "/projects/first", "first-session");
	await storage.add("line one\nline two", "/projects/second", "second-session");

	const entries = storage.getRecent(10);
	expect(entries).toHaveLength(1);
	expect(entries[0]?.prompt).toBe("line one\nline two");
	expect(entries[0]?.sessionId).toBe("second-session");
});

it("collapses preexisting whitespace-padded duplicates on open, keeping the latest provenance", async () => {
	tempDir = TempDir.createSync("@omp-history-storage-padded-");
	const dbPath = tempDir.join("history.db");
	HistoryStorage.open(dbPath);
	HistoryStorage.close();

	const raw = new Database(dbPath);
	const insert = raw.prepare(
		"INSERT INTO history (prompt, created_at, cwd, session_id, use_count) VALUES (?, ?, ?, ?, ?)",
	);
	insert.run("keep me tidy\nplease", 1, "/projects/old", "old-session", 4);
	insert.run("keep me tidy   \nplease", 2, "/projects/new", "new-session", 1);
	raw.run("PRAGMA user_version = 0");
	raw.close();

	const storage = HistoryStorage.open(dbPath);
	const entries = storage.getRecent(10);
	expect(entries).toHaveLength(1);
	expect(entries[0]).toMatchObject({
		prompt: "keep me tidy\nplease",
		created_at: 2,
		cwd: "/projects/new",
		sessionId: "new-session",
		useCount: 5,
	});
	// FTS was rebuilt after the delete+update, so no stale index rows remain.
	expect(storage.search("tidy", 10).map(entry => entry.sessionId)).toEqual(["new-session"]);
});

// #13926: report failed persistence once per outage without changing the log-only promise contract.
it("reports failed writes once per outage and permits persistence after recovery", async () => {
	tempDir = TempDir.createSync("@omp-history-storage-write-failure-");
	const dbPath = tempDir.join("history.db");
	const storage = HistoryStorage.open(dbPath);
	const peer = new Database(dbPath);
	const reportFailure = vi.fn();
	const reportSuccess = vi.fn();
	storage.setErrorListener(reportFailure);
	storage.setAddListener(reportSuccess);
	try {
		peer.run(
			"CREATE TRIGGER reject_prompt BEFORE INSERT ON history BEGIN SELECT RAISE(ABORT, 'write rejected'); END",
		);
		await storage.add("lost while locked", "/project", "session");
		await storage.add("lost again while locked", "/project", "session");
		expect(reportFailure).toHaveBeenCalledTimes(1);
		expect(reportFailure).toHaveBeenCalledWith(expect.any(Error));
		expect(reportSuccess).not.toHaveBeenCalled();
		peer.run("DROP TRIGGER reject_prompt");
		expect(storage.getRecent(10)).toEqual([]);

		await storage.add("lost while locked", "/project", "session");
		expect(reportSuccess).toHaveBeenCalledTimes(1);
		peer.run("CREATE TRIGGER reject_again BEFORE INSERT ON history BEGIN SELECT RAISE(ABORT, 'write rejected'); END");
		await storage.add("lost after recovery", "/project", "session");
		expect(reportFailure).toHaveBeenCalledTimes(2);
		expect(reportSuccess).toHaveBeenCalledTimes(1);
		peer.run("DROP TRIGGER reject_again");
		HistoryStorage.close();
		const reopened = HistoryStorage.open(dbPath);
		expect(reopened.search("locked", 10)).toMatchObject([
			{ prompt: "lost while locked", cwd: "/project", sessionId: "session", useCount: 1 },
		]);
	} finally {
		if (peer.inTransaction) peer.run("ROLLBACK");
		peer.close();
	}
});

// A damaged b-tree can decode another table's BLOB as `prompt`; recalling it in
// the editor threw "text.replace is not a function" on Up-arrow.
it.each([
	{ store: "a current store", userVersion: 2 },
	{ store: "a store awaiting the rebuild pass", userVersion: 1 },
])("never returns a non-text prompt from $store", async ({ userVersion }) => {
	tempDir = TempDir.createSync("@omp-history-storage-blob-prompt-");
	const dbPath = tempDir.join("history.db");
	await HistoryStorage.open(dbPath).add("readable prompt", "/project", "session");
	HistoryStorage.close();
	const raw = new Database(dbPath);
	raw.prepare("INSERT INTO history (prompt, created_at) VALUES (?, ?)").run(
		new Uint8Array([0x30, 0x15]),
		4_000_000_000,
	);
	raw.run(`PRAGMA user_version = ${userVersion}`);
	raw.close();

	const storage = HistoryStorage.open(dbPath);
	expect(storage.getRecent(10).map(entry => entry.prompt)).toEqual(["readable prompt"]);
	expect(storage.search("readable", 10).map(entry => entry.prompt)).toEqual(["readable prompt"]);
});

it("quarantines a damaged store on the rebuild pass and salvages its prompts and recaps", async () => {
	tempDir = TempDir.createSync("@omp-history-storage-damaged-");
	const dbPath = tempDir.join("history.db");
	const storage = HistoryStorage.open(dbPath);
	await storage.add("first prompt", "/project", "session-a");
	await storage.add("second prompt", "/project", "session-b");
	HistoryStorage.close();

	const raw = new Database(dbPath);
	raw.exec(`
		CREATE TABLE session_recaps (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			cwd TEXT NOT NULL,
			recap TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);
		INSERT INTO session_recaps (session_id, cwd, recap, created_at) VALUES ('session-a', '/project', 'kept recap', 5);
		PRAGMA user_version = 1;
		PRAGMA journal_mode = DELETE;
	`);
	const pageSize = raw.query<{ page_size: number }, []>("PRAGMA page_size").get()!.page_size;
	const indexRoot = raw
		.query<{ rootpage: number }, []>("SELECT rootpage FROM sqlite_master WHERE name = 'idx_history_created_at'")
		.get()!.rootpage;
	raw.close();
	// Scribble over the created_at index only: the table pages stay readable for salvage.
	const fd = fs.openSync(dbPath, "r+");
	fs.writeSync(fd, Buffer.alloc(pageSize, 0xff), 0, pageSize, (indexRoot - 1) * pageSize);
	fs.closeSync(fd);

	const repaired = HistoryStorage.open(dbPath);
	expect(repaired.getRecent(10).map(entry => entry.prompt)).toEqual(["second prompt", "first prompt"]);
	await repaired.add("after repair", "/project", "session-c");
	expect(repaired.search("repair", 10).map(entry => entry.prompt)).toEqual(["after repair"]);
	HistoryStorage.close();

	expect(fs.readdirSync(tempDir.path()).some(name => name.startsWith("history.db.corrupt-"))).toBe(true);
	const verify = new Database(dbPath, { readonly: true });
	try {
		expect(verify.query<{ integrity_check: string }, []>("PRAGMA integrity_check").all()).toEqual([
			{ integrity_check: "ok" },
		]);
		expect(verify.query("SELECT session_id, recap FROM session_recaps").all()).toEqual([
			{ session_id: "session-a", recap: "kept recap" },
		]);
	} finally {
		verify.close();
	}
});

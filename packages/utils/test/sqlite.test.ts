import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";
import {
	isSqliteCorruptionError,
	onSqliteStoreQuarantine,
	openSqliteDatabase,
	openSqliteDatabaseSync,
	readDamagedSqliteRows,
} from "../src/sqlite";
import { TempDir } from "../src/temp";

async function corruptSchemaPages(dbPath: string): Promise<Buffer<ArrayBuffer>> {
	const db = new Database(dbPath);
	db.run("CREATE TABLE entries (value TEXT)");
	db.run("INSERT INTO entries VALUES ('evidence')");
	db.run("PRAGMA wal_checkpoint(TRUNCATE)");
	db.close();

	const damaged = await fs.promises.readFile(dbPath);
	damaged.fill(0xff, 100);
	await fs.promises.writeFile(dbPath, damaged);
	return damaged;
}

async function backupNames(dirPath: string, dbName = "store.db"): Promise<string[]> {
	return (await fs.promises.readdir(dirPath)).filter(name => name.startsWith(`${dbName}.corrupt-`));
}

test("failed asynchronous initialization releases and rolls back its write transaction", async () => {
	await using dir = await TempDir.create("@omp-sqlite-init-");
	const dbPath = dir.join("store.db");
	await expect(
		openSqliteDatabase(dbPath, async db => {
			db.run("CREATE TABLE entries (value TEXT)");
			db.run("BEGIN IMMEDIATE");
			db.run("INSERT INTO entries VALUES ('uncommitted')");
			await Promise.resolve();
			db.run("INSERT INTO missing_table VALUES (1)");
		}),
	).rejects.toThrow(dbPath);

	const rows = await openSqliteDatabase(dbPath, db => {
		try {
			db.run("INSERT INTO entries VALUES ('reopened')");
			return db.query<{ value: string }, []>("SELECT value FROM entries").all();
		} finally {
			db.close();
		}
	});
	expect(rows).toEqual([{ value: "reopened" }]);
});

test("corruption recovery is opt-in and the default preserves the active evidence", async () => {
	await using dir = await TempDir.create("@omp-sqlite-default-corrupt-");
	const dbPath = dir.join("store.db");
	const damaged = Buffer.from("not a sqlite database".repeat(64));
	await fs.promises.writeFile(dbPath, damaged);

	let failure: unknown;
	try {
		await openSqliteDatabase(dbPath, db => db.query("SELECT name FROM sqlite_master").all());
	} catch (error) {
		failure = error;
	}

	expect(isSqliteCorruptionError(failure)).toBe(true);
	expect(failure).toBeInstanceOf(Error);
	if (!(failure instanceof Error)) throw new Error("Expected SQLite initialization to fail");
	expect(failure.message).toContain(dbPath);
	expect(await fs.promises.readFile(dbPath)).toEqual(damaged);
	expect(await backupNames(dir.path())).toEqual([]);
});

test("synchronous recovery preserves malformed schema pages and yields a persistent database", async () => {
	await using dir = await TempDir.create("@omp-sqlite-sync-corrupt-");
	const dbPath = dir.join("store.db");
	const damaged = await corruptSchemaPages(dbPath);

	openSqliteDatabaseSync(
		dbPath,
		db => {
			db.run("CREATE TABLE recovered (value TEXT)");
			db.run("INSERT INTO recovered VALUES ('usable')");
			db.close();
		},
		{ recoverCorruption: true },
	);

	const backups = (await backupNames(dir.path())).filter(name => !/-wal$|-shm$|-journal$/.test(name));
	expect(backups).toHaveLength(1);
	const backupPath = path.join(dir.path(), backups[0]!);
	expect(await fs.promises.readFile(backupPath)).toEqual(damaged);
	if (process.platform !== "win32") {
		expect((await fs.promises.stat(backupPath)).mode & 0o777).toBe(0o600);
	}

	const rows = openSqliteDatabaseSync(dbPath, db => {
		try {
			return db.query<{ value: string }, []>("SELECT value FROM recovered").all();
		} finally {
			db.close();
		}
	});
	expect(rows).toEqual([{ value: "usable" }]);
});

test("recovery preserves sidecars present at corruption detection under one private backup name", async () => {
	await using dir = await TempDir.create("@omp-sqlite-sidecars-");
	const dbPath = dir.join("store.db");
	const contents = new Map<string, Buffer<ArrayBuffer>>([
		["", Buffer.from("not a database")],
		["-wal", Buffer.from("damaged wal evidence")],
		["-shm", Buffer.from("damaged shm evidence")],
		["-journal", Buffer.from("damaged journal evidence")],
	]);
	for (const [suffix, bytes] of contents) await fs.promises.writeFile(`${dbPath}${suffix}`, bytes);

	await openSqliteDatabase(
		dbPath,
		async db => {
			try {
				db.run("CREATE TABLE recovered (value TEXT)");
			} catch (error) {
				// SQLite rebuilds its disposable shared-memory index while opening.
				contents.set("-shm", await fs.promises.readFile(`${dbPath}-shm`));
				// SQLite removes invalid rollback journals before reporting corruption.
				await fs.promises.writeFile(`${dbPath}-journal`, contents.get("-journal")!);
				throw error;
			}
			db.close();
		},
		{ recoverCorruption: true },
	);

	const mainBackups = (await backupNames(dir.path())).filter(name => !/-wal$|-shm$|-journal$/.test(name));
	expect(mainBackups).toHaveLength(1);
	const backupPath = path.join(dir.path(), mainBackups[0]!);
	for (const [suffix, bytes] of contents) {
		expect(await fs.promises.readFile(`${backupPath}${suffix}`)).toEqual(bytes);
		if (process.platform !== "win32") {
			expect((await fs.promises.stat(`${backupPath}${suffix}`)).mode & 0o777).toBe(0o600);
		}
	}
});

test("concurrent failed openers adopt one replacement without discarding each other's writes", async () => {
	await using dir = await TempDir.create("@omp-sqlite-concurrent-");
	const dbPath = dir.join("store.db");
	const damaged = await corruptSchemaPages(dbPath);
	const ready = Promise.withResolvers<void>();
	let opened = 0;
	const initialize = async (db: Database): Promise<Database> => {
		if (++opened === 2) ready.resolve();
		await ready.promise;
		db.run("PRAGMA journal_mode=WAL");
		db.run("CREATE TABLE IF NOT EXISTS recovered (value TEXT)");
		return db;
	};

	const handles = await Promise.all([
		openSqliteDatabase(dbPath, initialize, { recoverCorruption: true }),
		openSqliteDatabase(dbPath, initialize, { recoverCorruption: true }),
	]);
	try {
		handles[0].run("INSERT INTO recovered VALUES ('first')");
		handles[1].run("INSERT INTO recovered VALUES ('second')");
		expect(handles[0].query<{ value: string }, []>("SELECT value FROM recovered ORDER BY value").all()).toEqual([
			{ value: "first" },
			{ value: "second" },
		]);
	} finally {
		for (const db of handles) db.close();
	}
	const backups = await backupNames(dir.path());
	expect(backups).toHaveLength(1);
	expect(await fs.promises.readFile(path.join(dir.path(), backups[0]!))).toEqual(damaged);
});

test("a second corruption failure surfaces without rotating the first backup again", async () => {
	await using dir = await TempDir.create("@omp-sqlite-repeat-corrupt-");
	const dbPath = dir.join("store.db");
	const damaged = await corruptSchemaPages(dbPath);
	await expect(
		openSqliteDatabase(dbPath, db => db.run("CREATE TABLE recovered (value TEXT)"), {
			recoverCorruption: true,
			onCorruptionPreserved: backupPath => fs.copyFileSync(backupPath, dbPath),
		}),
	).rejects.toMatchObject({ code: "SQLITE_CORRUPT", message: expect.stringContaining(dbPath) });
	const backups = await backupNames(dir.path());
	expect(backups).toHaveLength(1);
	expect(await fs.promises.readFile(path.join(dir.path(), backups[0]!))).toEqual(damaged);
});

test("opt-in recovery never rotates a non-corruption SQLite failure", async () => {
	await using dir = await TempDir.create("@omp-sqlite-error-");
	const dbPath = dir.join("store.db");

	let failure: unknown;
	try {
		openSqliteDatabaseSync(dbPath, db => db.run("INSERT INTO missing_table VALUES (1)"), { recoverCorruption: true });
	} catch (error) {
		failure = error;
	}

	expect(isSqliteCorruptionError(failure)).toBe(false);
	expect(failure).toBeInstanceOf(Error);
	if (!(failure instanceof Error)) throw new Error("Expected SQLite initialization to fail");
	expect(failure.message).toContain(dbPath);
	expect(await backupNames(dir.path())).toEqual([]);
});

/** Point the freelist trunk at a page past EOF: reads still work, but the next page allocation fails. */
async function corruptFreelist(dbPath: string) {
	const seed = new Database(dbPath);
	seed.run("CREATE TABLE existing (value TEXT)");
	seed.close();
	const damaged = await fs.promises.readFile(dbPath);
	damaged.writeUInt32BE(0x0d000000, 32);
	damaged.writeUInt32BE(1, 36);
	await fs.promises.writeFile(dbPath, damaged);
	return damaged;
}

test("recovery still fires when a multi-statement script hides the corruption behind a later error", async () => {
	await using dir = await TempDir.create("@omp-sqlite-hidden-corrupt-");
	const dbPath = dir.join("store.db");
	const damaged = await corruptFreelist(dbPath);

	const initialize = (db: Database) => {
		// Bun reports only the final statement's step error, so the failed CREATE is dropped here...
		db.run("CREATE TABLE IF NOT EXISTS added (value TEXT); CREATE TABLE IF NOT EXISTS existing (value TEXT);");
		// ...and resurfaces as SQLITE_ERROR, which on its own would not trigger recovery.
		db.prepare("SELECT value FROM added").finalize();
		return db;
	};

	const db = await openSqliteDatabase(dbPath, initialize, { recoverCorruption: true });
	try {
		expect(db.query("SELECT value FROM added").all()).toEqual([]);
	} finally {
		db.close();
	}
	const backups = (await backupNames(dir.path())).filter(name => !/-wal$|-shm$|-journal$/.test(name));
	expect(backups).toHaveLength(1);
	expect(await fs.promises.readFile(path.join(dir.path(), backups[0]!))).toEqual(damaged);
});

test("a non-corruption init failure on a store that fails quick_check is preserved as corruption", async () => {
	await using dir = await TempDir.create("@omp-sqlite-init-fails-on-corrupt-");
	const dbPath = dir.join("store.db");
	const damaged = await corruptFreelist(dbPath);
	const initFailure = new Error("init failed");
	let attempts = 0;
	let preserved: unknown;

	const db = await openSqliteDatabase(
		dbPath,
		handle => {
			if (attempts++ === 0) throw initFailure;
			return handle;
		},
		{ recoverCorruption: true, onCorruptionPreserved: (_backupPath, error) => (preserved = error) },
	);
	db.close();

	expect(attempts).toBe(2);
	expect(isSqliteCorruptionError(preserved)).toBe(true);
	expect((preserved as Error | undefined)?.cause).toBe(initFailure);
	const backups = (await backupNames(dir.path())).filter(name => !/-wal$|-shm$|-journal$/.test(name));
	expect(backups).toHaveLength(1);
	expect(await fs.promises.readFile(path.join(dir.path(), backups[0]!))).toEqual(damaged);
});

test("a long-lived connection's quarantine hook runs after the evidence is copied and before the store is removed", async () => {
	await using dir = await TempDir.create("@omp-sqlite-quarantine-hook-");
	const dbPath = dir.join("store.db");
	const damaged = await corruptSchemaPages(dbPath);
	const observed: Array<{ storePresent: boolean; evidence: string[] }> = [];
	const unregister = onSqliteStoreQuarantine(dbPath, () => {
		observed.push({ storePresent: fs.existsSync(dbPath), evidence: fs.readdirSync(dir.path()) });
	});
	try {
		openSqliteDatabaseSync(
			dbPath,
			db => {
				db.query("SELECT count(*) FROM sqlite_master").get();
				db.close();
			},
			{ recoverCorruption: true },
		);
	} finally {
		unregister();
	}

	expect(observed).toHaveLength(1);
	expect(observed[0]!.storePresent).toBe(true);
	const backup = observed[0]!.evidence.find(name => /^store\.db\.corrupt-[^-]+-[0-9a-f-]+$/.test(name));
	expect(backup).toBeDefined();
	expect(await fs.promises.readFile(path.join(dir.path(), backup!))).toEqual(damaged);
});

test("damaged-row salvage terminates on a huge sparse AUTOINCREMENT high-water mark", async () => {
	await using dir = await TempDir.create("@omp-sqlite-salvage-sparse-");
	const db = new Database(dir.join("store.db"));
	try {
		db.run("CREATE TABLE entries (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)");
		db.run("INSERT INTO entries (id, value) VALUES (1, 'first'), (1099511627776, 'far')");
		db.run("UPDATE sqlite_sequence SET seq = 4503599627370495 WHERE name = 'entries'");

		expect(readDamagedSqliteRows(db, "entries", "id, value")).toEqual({
			rows: [
				{ id: 1, value: "first" },
				{ id: 1099511627776, value: "far" },
			],
			damagedRegions: 0,
		});
	} finally {
		db.close();
	}
});

test.each([
	{ region: "a middle leaf", page: (pages: number) => Math.floor(pages / 2), lastSurvives: true },
	{ region: "the rightmost leaf", page: (pages: number) => pages, lastSurvives: false },
])("damaged-row salvage reads past damage in $region", async ({ page, lastSurvives }) => {
	await using dir = await TempDir.create("@omp-sqlite-salvage-damaged-");
	const dbPath = dir.join("store.db");
	const total = 40;
	const seed = new Database(dbPath);
	// No AUTOINCREMENT: a damaged rightmost page leaves no readable max rowid to bound the scan.
	seed.run("CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT)");
	const insert = seed.prepare("INSERT INTO entries (id, value) VALUES (?, ?)");
	// ~1.5 KB rows: at most two per leaf, and appends allocate leaves in rowid order.
	for (let id = 1; id <= total; id++) insert.run(id, `${id}:${"x".repeat(1500)}`);
	const pageSize = seed.query<{ page_size: number }, []>("PRAGMA page_size").get()!.page_size;
	const pageCount = seed.query<{ page_count: number }, []>("PRAGMA page_count").get()!.page_count;
	seed.close();
	const fd = fs.openSync(dbPath, "r+");
	fs.writeSync(fd, Buffer.alloc(pageSize, 0xff), 0, pageSize, (page(pageCount) - 1) * pageSize);
	fs.closeSync(fd);

	const db = new Database(dbPath, { readonly: true });
	try {
		const { rows, damagedRegions } = readDamagedSqliteRows<{ id: number }>(db, "entries", "id");
		const ids = rows.map(row => row.id);
		expect(damagedRegions).toBeGreaterThanOrEqual(1);
		expect(ids[0]).toBe(1);
		expect(ids.length).toBeGreaterThanOrEqual(total - 2);
		expect(ids.includes(total)).toBe(lastSurvives);
		expect(ids).toEqual([...ids].sort((a, b) => a - b));
	} finally {
		db.close();
	}
});

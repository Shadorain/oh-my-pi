import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { TextPredictor } from "@oh-my-pi/pi-natives";
import { withSqliteStoreLease } from "@oh-my-pi/pi-utils/sqlite";
import { Engine } from "../src/predict/daemon";

function createHistory(dbPath: string, count: number, finalPrompt = `prompt ${count}`): void {
	const db = new Database(dbPath);
	try {
		db.run("CREATE TABLE history (id INTEGER PRIMARY KEY, prompt)");
		const insert = db.prepare("INSERT INTO history (id, prompt) VALUES (?, ?)");
		for (let id = 1; id <= count; id++) insert.run(id, id === count ? finalPrompt : `prompt ${id}`);
	} finally {
		db.close();
	}
}

function engine(observe: (prompts: string[]) => Promise<void>): Engine {
	const predictor = { observe, persist: async () => {} } as unknown as TextPredictor;
	return new Engine("ngram", "", predictor, 0);
}

describe("text-predict history ingestion", () => {
	it("releases each history lease before observe and reopens to see a replacement database", async () => {
		const dir = fs.mkdtempSync(path.join(os.tmpdir(), "omp-predict-ingest-"));
		const dbPath = path.join(dir, "history.db");
		const replacementPath = path.join(dir, "replacement.db");
		createHistory(dbPath, 1_001, "old final prompt");
		expect(fs.existsSync(`${dbPath}-wal`)).toBe(false);
		expect(fs.existsSync(`${dbPath}-shm`)).toBe(false);
		let releaseObserve!: () => void;
		const observePending = new Promise<void>(resolve => {
			releaseObserve = resolve;
		});
		let firstBatch!: (prompts: string[]) => void;
		const firstBatchSeen = new Promise<string[]>(resolve => {
			firstBatch = resolve;
		});
		let batches = 0;
		const instance = engine(async prompts => {
			batches++;
			if (batches === 1) {
				firstBatch(prompts);
				await observePending;
			}
		});

		try {
			const ingestion = instance.ingest(dbPath);
			const observed = await firstBatchSeen;
			expect(observed).toHaveLength(1_000);
			expect(instance.cursor).toBe(0);
			withSqliteStoreLease(dbPath, () => {
				new Database(dbPath).close();
			});
			createHistory(replacementPath, 1_001, "replacement final prompt");
			withSqliteStoreLease(dbPath, () => {
				fs.rmSync(dbPath);
				fs.renameSync(replacementPath, dbPath);
			});
			expect(fs.existsSync(`${dbPath}-wal`)).toBe(false);
			expect(fs.existsSync(`${dbPath}-shm`)).toBe(false);
			releaseObserve();
			expect(await ingestion).toBe(1_001);
			expect(instance.cursor).toBe(1_001);
			expect(batches).toBe(2);
		} finally {
			releaseObserve();
			fs.rmSync(dir, { recursive: true, force: true });
		}
	});

	it("does not advance the cursor when observe rejects, so retry receives the same prompts", async () => {
		const dir = fs.mkdtempSync(path.join(os.tmpdir(), "omp-predict-ingest-"));
		const dbPath = path.join(dir, "history.db");
		createHistory(dbPath, 1, "retry me");
		const seen: string[][] = [];
		let reject = true;
		const instance = engine(async prompts => {
			seen.push(prompts);
			if (reject) {
				reject = false;
				throw new Error("observe failed");
			}
		});
		try {
			await expect(instance.ingest(dbPath)).rejects.toThrow("observe failed");
			expect(instance.cursor).toBe(0);
			expect(await instance.ingest(dbPath)).toBe(1);
			expect(instance.cursor).toBe(1);
			expect(seen).toEqual([["retry me"], ["retry me"]]);
		} finally {
			fs.rmSync(dir, { recursive: true, force: true });
		}
	});

	it("skips non-text prompts and does not create a missing history database", async () => {
		const dir = fs.mkdtempSync(path.join(os.tmpdir(), "omp-predict-ingest-"));
		const dbPath = path.join(dir, "history.db");
		const db = new Database(dbPath);
		try {
			db.run("CREATE TABLE history (id INTEGER PRIMARY KEY, prompt)");
			db.run("INSERT INTO history VALUES (1, 'valid'), (2, X'0102'), (3, NULL)");
		} finally {
			db.close();
		}
		const seen: string[][] = [];
		const instance = engine(async prompts => {
			seen.push(prompts);
		});
		try {
			expect(await instance.ingest(dbPath)).toBe(1);
			expect(seen).toEqual([["valid"]]);
			const missingPath = path.join(dir, "missing.db");
			expect(await instance.ingest(missingPath)).toBe(0);
			expect(fs.existsSync(missingPath)).toBe(false);
		} finally {
			fs.rmSync(dir, { recursive: true, force: true });
		}
	});
});

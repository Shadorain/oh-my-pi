/**
 * Decode a response body honoring the declared charset (Content-Type header,
 * then a cheap <meta charset> sniff), falling back to UTF-8.
 */
export function decodeBody(bytes: Buffer, contentTypeHeader: string): string {
	let label = /charset\s*=\s*"?([\w-]+)"?/i.exec(contentTypeHeader)?.[1];
	if (!label) {
		// All charsets we can decode are ASCII-compatible in the prefix, so a
		// latin1 view of the first 2KB is enough to find a <meta charset>.
		label = /<meta[^>]+charset\s*=\s*["']?([\w-]+)/i.exec(bytes.subarray(0, 2048).toString("latin1"))?.[1];
	}
	if (label && !/^utf-?8$/i.test(label)) {
		try {
			// Bun.Encoding's union is narrower than the runtime, which accepts
			// WHATWG labels (shift_jis, euc-kr, gbk, big5, …); unknowns throw here.
			return new TextDecoder(label as Bun.Encoding).decode(bytes);
		} catch {
			// Unknown/unsupported label — fall back to UTF-8.
		}
	}
	// TextDecoder drops a leading BOM, which JSON.parse would otherwise reject.
	return new TextDecoder().decode(bytes);
}

/**
 * Read a response body up to `maxBytes`, stopping once the cap is crossed.
 * `bytes` never exceeds the cap; `truncated` reports whether the body was cut
 * mid-stream. Returns `null` when the response carries no readable body.
 */
export async function readBodyCapped(
	response: Response,
	maxBytes: number,
): Promise<{ bytes: Buffer; truncated: boolean } | null> {
	const reader = response.body?.getReader();
	if (!reader) return null;

	const chunks: Uint8Array[] = [];
	let totalSize = 0;
	let truncated = false;

	try {
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;

			const accepted = Math.min(value.byteLength, maxBytes - totalSize);
			if (accepted < value.byteLength) {
				// Crossing chunk: keep the head up to the cap, mark the body cut
				// and stop reading so an oversized response cannot balloon memory.
				if (accepted > 0) {
					chunks.push(value.subarray(0, accepted));
					totalSize += accepted;
				}
				truncated = true;
				await reader.cancel().catch(() => undefined);
				break;
			}
			chunks.push(value);
			totalSize += accepted;
		}
	} finally {
		reader.releaseLock();
	}

	// A single chunk is decoded in place; only multi-chunk bodies need a concat copy.
	const bytes =
		chunks.length === 1
			? Buffer.from(chunks[0].buffer, chunks[0].byteOffset, chunks[0].byteLength)
			: Buffer.concat(chunks, totalSize);
	return { bytes, truncated };
}

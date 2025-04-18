/*
 * # Functional Requirements
 *
 * Vector Similarity Worker is a scheduled Cloudflare Worker that generates embeddings for processing messages and updates vector similarity scores across different topics and industries. It enables efficient vector search capabilities by maintaining up-to-date embeddings and similarity scores in a TimescaleDB database.
 *
 * ## Key Functionality:
 * - Message selection filters for messages where `embedding` is null in the `unique_messages` table and `timestamp` of the corresponding `messageId` record in the `message_feed` table is less than 1 day old:
 *   - Only distinct rows from `unique_messages` are processed. If same unique message appeared multiple times in recent feed, duplicates are not taken into account. To properly deduplicate distinct feed messages, timestamp column cannot be included in select.
 *   - Unique messages with empty text "" are not processed
 * - Vector similarity scores are calculated between new and existing message per topic-industry pair:
 *   - Text embeddings are obtained using Cloudflare Workers AI model
 *   - The worker dynamically fetches all existing distinct pairs from `synth_data_prod` table to account for different topic-industry pairs
 *   - The worker populates `similarity` field with the corrsesponding similairity score values and copy it to `main` field if `main` is null. 
 *
 * ## Additional Documentation
 * ### Hyperdrive Usage with TimescaleDB
 * ```ts
 * import { Client } from "pg";
 * const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
 * await client.connect();
 * ### Workers AI native binding 
 * ```ts
 * const resp = await env.AI.run(modelName, { text: inputs });
 * const embedding = resp.data[j];
 * const formattedEmbedding = `[${embedding.join(',')}]`;
 * ```
 * * ### Vector Search using pgvector
 * ```ts
 * SELECT content
 * FROM table
 * ORDER BY embedding <=> $1::vector
 * LIMIT 1;
 * ```
 * ## PostgreSQL DB Schemas
 * -- Message feed table with metadata
 * CREATE TABLE message_feed (
 *     id INTEGER GENERATED ALWAYS AS IDENTITY,
 *     timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
 *     message_id INTEGER NOT NULL,
 *     PRIMARY KEY (id, timestamp),
 *     CONSTRAINT fk_message_id FOREIGN KEY (message_id) REFERENCES unique_messages(id),
 *     UNIQUE (timestamp, platformName, platformMessageId)
 * );
 *
 * -- Deduplicated message content and vector embeddings table
 * CREATE TABLE unique_messages (
 *     id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 *     content TEXT NOT NULL UNIQUE,
 *     embedding VECTOR(768)
 * );
 *
 * -- Scores for classified message contents for each topic-industry pair
 * CREATE TABLE message_scores (
 *     id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 *     topic TEXT NOT NULL,
 *     industry TEXT NOT NULL,
 *     main REAL,
 *     similarity REAL,
 *     message_id INTEGER NOT NULL,
 *     CONSTRAINT fk_message_id FOREIGN KEY (message_id) REFERENCES unique_messages(id)
 * );
 *
 * -- Synthetic data production table
 * CREATE TABLE synth_data_prod (
 *     id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 *     topic TEXT NOT NULL,
 *     industry TEXT NOT NULL,
 *     content TEXT NOT NULL,
 *     embedding VECTOR(768) NOT NULL
 * );
 *
 * ## Specific Project Priorities:
 * - Batching: The worker insert scores for all topic-industry pairs using batching in a single query and updates embeddings using batching in a single query. Additionally, it implements batching when obtaining embeddings. 
 */

import { env, SELF } from 'cloudflare:test';
import { describe, expect, it, vi, beforeEach } from 'vitest';

declare module 'cloudflare:test' {
	interface ProvidedEnv {
		AI: Ai;
		HYPERDRIVE: Hyperdrive;
	}
}

// mock setup code
beforeEach(() => {
	vi.clearAllMocks();
});

// mock node-postgres Client
vi.mock('pg', async () => {
	return {
		Client: vi.fn(() => ({
			connect: vi.fn(),
			query: vi.fn(),
			end: vi.fn(),
		})),
	};
});

// spy on Drizzle operators
vi.mock('drizzle-orm', async () => {
	return {
		__esModule: true,
		...(await vi.importActual('drizzle-orm')),
	};
});
vi.spyOn(DrizzleOperators, 'cosineDistance');

// mock Drizzle select query
vi.mock('drizzle-orm/node-postgres');


const createMockSelect = () => ({
	innerJoin: vi.fn().mockReturnThis(),
	where: vi.fn().mockReturnThis(),
	groupBy: vi.fn().mockReturnThis(),
	limit: vi.fn().mockReturnThis(),
	orderBy: vi.fn().mockReturnThis(),
	execute: vi.fn().mockImplementation(() => {
		return [];
	}),
	then: vi.fn().mockImplementation(function (this: any, resolve) {
		resolve(this.execute());
	}),
});

// latest unprocessed unique messages
const selectDistinctUniqueMessagesColumns = vi.fn();
const selectDistinctUniqueMessagesMock = createMockSelect();
// industry/topic pairs
let selectDistinctSynthDataColumns = vi.fn();
const selectDistinctSynthDataMock = createMockSelect();
// similarity score
const selectSynthDataColumns = vi.fn();
const selectSynthDataMock = createMockSelect();

const dbMock = {
	selectDistinct: vi.fn().mockImplementation((columns) => ({
		from: (table: PgTable) => {
			switch (table) {
				case uniqueMessages:
					selectDistinctUniqueMessagesColumns(columns);
					return selectDistinctUniqueMessagesMock;
				case synthDataProd:
					selectDistinctSynthDataColumns(columns);
					return selectDistinctSynthDataMock;
			}
		}
	})),
	select: vi.fn().mockImplementation((columns) => ({
		from: (table: PgTable) => {
			switch (table) {
				case synthDataProd:
					selectSynthDataColumns(columns);
					return selectSynthDataMock;
			}
		}
	})),
	insert: vi.fn().mockReturnValue({
		values: vi.fn().mockReturnThis(),
		onConflictDoUpdate: vi.fn().mockReturnThis(),
		execute: vi.fn().mockImplementation(() => {
			return [];
		}),
		then: vi.fn().mockImplementation(function (this: any, resolve) {
			resolve(this.execute());
		}),
	}),
	update: vi.fn().mockImplementation(() => ({
		set: vi.fn().mockReturnThis(),
		where: vi.fn().mockReturnThis(),
		execute: vi.fn().mockImplementation(() => {
			return [];
		}),
		then: vi.fn().mockImplementation(function (this: any, resolve) {
			resolve(this.execute());
		}),
	})),
};

// @ts-ignore
vi.mocked(drizzle, true).mockReturnValue(dbMock);
vi.spyOn(env.AI, 'run');

describe('Vector Similarity Worker', async () => {
	const mockMessages = [
		{
			id: 644369,
			content: '#Cryptocurrency valued at over $13.9 million was stolen from the #SouthKorean exchange #GDAC.',
			embedding: null,
			timestamp: new Date('2024-08-12T12:35:00Z'),
		},
		{
			id: 644368,
			content: 'South Korean crypto exchange GDAC hacked for nearly $14M',
			embedding: null,
			timestamp: new Date('2024-08-12T11:00:00Z'),
		},
	];
	selectDistinctUniqueMessagesMock.execute.mockResolvedValue(mockMessages);

	// default until mocked more specifically
	selectSynthDataMock.execute.mockResolvedValue([{ similarity: 0.0 }])

	const AiStub: Record<string, number[]> = {
		'#Cryptocurrency valued at over $13.9 million was stolen from the #SouthKorean exchange #GDAC.': [0.1, 0.2, 0.3],
		'South Korean crypto exchange GDAC hacked for nearly $14M': [0.4, 0.5, 0.6],
	};
	// @ts-ignore
	vi.mocked(env.AI.run).mockImplementation(async (model, { text: texts }: { text: string[] }) => {
		return {
			shape: [texts.length, 3],
			data: texts.map((text) => AiStub[text]),
		};
	});

	const mockTopicIndustries = [
		{ topic: 'cyberattack', industry: 'finance_blockchain' },
		{ topic: 'solvency', industry: 'finance_blockchain' },
	];
	selectDistinctSynthDataMock.execute.mockResolvedValue(mockTopicIndustries);

	it('should retrieve unprocessed messages for embedding generation', async () => {
		await SELF.scheduled();

		expect(selectDistinctUniqueMessagesColumns).toHaveBeenCalledWith({
			id: uniqueMessages.id,
			content: uniqueMessages.content,
		});
		expect(selectDistinctUniqueMessagesMock.innerJoin).toHaveBeenCalledWith(messageFeed, expect.anything());
		expect(selectDistinctUniqueMessagesMock.limit).toHaveBeenCalledWith(100);
	});

	it('generates vector embeddings for messages and saves them', async () => {
		await SELF.scheduled();

		expect(env.AI.run).toHaveBeenCalledWith(expect.any(String), {
			text: [
				'#Cryptocurrency valued at over $13.9 million was stolen from the #SouthKorean exchange #GDAC.',
				'South Korean crypto exchange GDAC hacked for nearly $14M',
			],
		});

		expect(dbMock.update).toHaveBeenCalledWith(uniqueMessages);
	});

	it('fetches all existing distinct pairs of topic and industry', async () => {
		await SELF.scheduled();

		expect(selectDistinctSynthDataColumns).toHaveBeenCalledWith(expect.objectContaining({
			topic: expect.anything(),
			industry: expect.anything(),
		}));
		expect(selectDistinctSynthDataMock.execute).toHaveBeenCalled();
	});
	
	it('calculates similarity scores and saves them', async () => {
		selectSynthDataMock.execute
			// message 1 - cyberattack / finance_blockchain
			.mockResolvedValueOnce([{ similarity: 0.92 }])
			// message 1 - solvency / finance_blockchain
			.mockResolvedValueOnce([{ similarity: 0.49 }])
			// message 2 - cyberattack / finance_blockchain
			.mockResolvedValueOnce([{ similarity: 0.21 }])
			// message 2 - solvency / finance_blockchain
			.mockResolvedValueOnce([{ similarity: 0.35 }]);

		await SELF.scheduled();

		expect(DrizzleOperators.cosineDistance).toHaveBeenCalledWith(expect.anything(), [0.1, 0.2, 0.3]);
		expect(DrizzleOperators.cosineDistance).toHaveBeenCalledWith(expect.anything(), [0.4, 0.5, 0.6]);

		expect(dbMock.insert).toHaveBeenCalledWith(messageScores);
	});
});

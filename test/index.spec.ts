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
 * ## Similarity Search Score
 * It's calculated using the following query:
 * ```ts
 * import { cosineDistance, gt, sql, and, eq } from 'drizzle-orm';
 * const similarity = sql<number>`1 - (${cosineDistance(table.embedding, embedding)})`;
 *
 *    const result = await db
 *      .select({ similarity })
 *      .from(table)
 *      .where(and(eq(table.topic, topic)))
 *      .orderBy((t) => desc(t.similarity))
 *      .limit(1)
 *      .execute();
 * ```
 *
 * ## Batch update embeddings example
 * Batch updating vectors requires extra care. Consider following example:
 * ```js
 * const sqlChunks = [];
 * const ids = [];
 * sqlChunks.push(sql`(case`);
 *
 * sqlChunks.push(sql`when ${uniqueMessages.id} = ${unprocessedMessages[i].id} then ${'[' + embeddings.data[i].join(',') + ']'}::vector`);
 * ids.push(unprocessedMessages[i].id);
 *
 * sqlChunks.push(sql`end)`);
 * const embeddingSql = sql.join(sqlChunks, sql.raw(' '));
 *
 * await db.update(uniqueMessages)
 *   .set({ embedding: embeddingSql })
 *   .where(inArray(messageScores.messageId, ids));
 * ```
 *
 * ## Drizzle schema
 * import { messageFeed, messageScores, synthDataProd, uniqueMessages } from 'schemas';
 * 
 * // Raw message feed with metadata
 * const messageFeed = pgTable('message_feed', {
 *   id: integer('id').generatedAlwaysAsIdentity(),
 *   timestamp: timestamp('timestamp', { withTimezone: true }).notNull().defaultNow(),
 *   messageId: integer('message_id').references(() => uniqueMessages.id).notNull(),
 * }, (table) => ({
 *   pk: primaryKey({ columns: [table.id, table.timestamp] }),
 *   unq: unique().on(table.timestamp, table.platformName, table.platformMessageId),
 * }));
 * // Deduplicated message content and vector embeddings for similarity vector search capabilities
 * const uniqueMessages = pgTable('unique_messages', {
 *   id: integer('id').primaryKey().generatedAlwaysAsIdentity(),
 *   content: text('content').unique().notNull(),
 *   embedding: vector('embedding', { dimensions: 768 }),
 * });
 * // Scores for classified message contents for each topic-industry pair
 * const messageScores = pgTable('message_scores', {
 *   id: integer('id').primaryKey().generatedAlwaysAsIdentity(),
 *   topic: text('topic').notNull(),
 *   industry: text('industry').notNull(),
 *   main: real('main'),
 *   similarity: real('similarity'),
 *   messageId: integer('message_id').references(() => uniqueMessages.id).notNull(),
 * });
 *
 * export const synthDataProd = pgTable('synth_data_prod', {
 *   id: integer('id').primaryKey().generatedAlwaysAsIdentity(),
 *   topic: text('topic').notNull(),
 *   industry: text('industry').notNull(),
 *   content: text('content').notNull(),
 *   embedding: vector('embedding', { dimensions: 768 }).notNull(),
 * });
 *
 * ## Specific Project Priorities:
 * - Batching: The worker insert scores for all topic-industry pairs using batching in a single query and updates embeddings using batching in a single query. Additionally, it implements batching when obtaining embeddings. 
 * - Robust error handling and extensive logging techniques: respecting Cloudflare Workers' logs limitations, including `INFO` and `ERROR` levels, wrapping each processing stage in a try-catch block, and descriptive wording with contextual information, such as processing stage, task name, etc.
 */

import { env, SELF } from 'cloudflare:test';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { messageFeed, messageScores, synthDataProd, uniqueMessages } from 'schemas';
import { drizzle } from 'drizzle-orm/node-postgres';
import { type PgTable } from 'drizzle-orm/pg-core';
import * as DrizzleOperators from 'drizzle-orm';

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

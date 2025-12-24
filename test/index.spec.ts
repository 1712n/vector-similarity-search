/*
 * # Functional Requirements
 *
 * Vector Similarity Worker is a scheduled Cloudflare Worker that generates embeddings for processing messages and calculates vector similarity scores across different topics (e.g., "cyberattack") and industries (e.g., "finance"). It compares new messages against pre-classified synthetic reference data to determine how closely they match each topic-industry combination, enabling efficient text classification in a TimescaleDB database. The system leverages vector similarity as a classification mechanism, where high-dimensional embeddings from synthetic reference data serve as classification reference points in the vector space.
 *
 * ## Key Functionality:
 * - Message selection filters for messages where `embedding` is null in the `unique_messages` table and `timestamp` of the corresponding `message_id` record in the `message_feed` table is less than 1 day old:
 *   - Only distinct rows from `unique_messages` are processed. If same unique message appeared multiple times in recent feed, duplicates are not taken into account. To properly deduplicate distinct feed messages, timestamp column cannot be included in select.
 *   - Unique messages with empty text are not processed
 * - Vector similarity scores are calculated between new and existing message per topic-industry pair:
 *   - Text embeddings are obtained using Cloudflare Workers AI model (`@cf/baai/bge-m3`)
 *   - Similarity scores are calculated efficiently by first querying unique topic-industry combinations from synth_data_prod, then using LATERAL JOIN to perform KNN vector search for each combination.
 *   - The worker iterates through topic-industry combinations, running one optimized query per combination that finds the best match for all messages in the batch.
 *   - Similarity search scores are obtained from the closest match (highest similarity score) per message-topic-industry combination
 *   - The worker populates `similarity` field with the corresponding similarity score values and copies it to `main` field if `main` is null
 *
 * ## Additional Documentation
 * ### Hyperdrive Usage with TimescaleDB
 * ```ts
 * import { Client } from "pg";
 * const client = new Client({ 
 *   connectionString: env.HYPERDRIVE.connectionString,
 *   connectionTimeoutMillis: 30000,
 *   query_timeout: 30000
 * });
 * await client.connect();
 * ```
 * ### Workers AI native binding 
 * ```ts
 * const resp = await env.AI.run(modelName, { text: inputs });
 * const embedding = resp.data[j];
 * const formattedEmbedding = `[${embedding.join(",")}]`;
 * ```
 * ### Vector Search using pgvector
 * ```ts
 * // Get unique topic-industry combinations:
 * SELECT DISTINCT topic, industry 
 * FROM synth_data_prod 
 * WHERE embedding IS NOT NULL
 * 
 * // For each combination, find best match for all messages using LATERAL JOIN:
 * SELECT 
 *   m.id as message_id,
 *   $1::text as topic,
 *   $2::text as industry,
 *   1 - (m.embedding <=> s.embedding) AS similarity
 * FROM unique_messages m
 * CROSS JOIN LATERAL (
 *   SELECT embedding
 *   FROM synth_data_prod
 *   WHERE topic = $1 
 *     AND industry = $2
 *     AND embedding IS NOT NULL
 *   ORDER BY embedding <=> m.embedding
 *   LIMIT 1
 * ) s
 * WHERE m.id = ANY($3::int[])
 *   AND m.embedding IS NOT NULL
 * 
 * // For embedding updates:
 * UPDATE unique_messages SET embedding = $1::vector WHERE id = $2
 * ```
 * ## PostgreSQL DB Schemas
 * -- Message feed table with metadata
 * CREATE TABLE message_feed (
 *     id INTEGER GENERATED ALWAYS AS IDENTITY,
 *     timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
 *     message_id INTEGER NOT NULL,
 *     platform_name TEXT NOT NULL,
 *     platform_message_id TEXT NOT NULL,
 *     PRIMARY KEY (id, timestamp),
 *     CONSTRAINT fk_message_id FOREIGN KEY (message_id) REFERENCES unique_messages(id),
 *     UNIQUE (timestamp, platform_name, platform_message_id)
 * );
 *
 * -- Deduplicated message content and vector embeddings table
 * CREATE TABLE unique_messages (
 *     id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 *     content TEXT NOT NULL UNIQUE,
 *     embedding VECTOR(1024)
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
 *     CONSTRAINT fk_message_id FOREIGN KEY (message_id) REFERENCES unique_messages(id),
 *     UNIQUE (message_id, topic, industry)
 * );
 *
 * -- Synthetic data production table
 * CREATE TABLE synth_data_prod (
 *     id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 *     topic TEXT NOT NULL,
 *     industry TEXT NOT NULL,
 *     content TEXT NOT NULL,
 *     embedding VECTOR(1024) NOT NULL
 * );
 *
 * ## Specific Project Priorities:
 * - Embedding Updates: The worker updates embeddings in the unique_messages table sequentially per message (this is acceptable as there are at most 100 messages per run)
 * - Score Insertion Batching: The worker inserts all calculated scores into message_scores table in a single INSERT query with multiple value sets
 * - AI Embeddings Batching: The worker processes up to 100 messages per run and obtains all embeddings in a single batch call to the AI model
 * - Connection Management: The worker includes proper error handling for connection issues and ensures the database connection is properly closed in all scenarios (success or failure)
 */

import { env, SELF } from 'cloudflare:test';
import { Client } from 'pg';

declare module 'cloudflare:test' {
	interface ProvidedEnv {
		AI: Ai;
		HYPERDRIVE: Hyperdrive;
	}
}
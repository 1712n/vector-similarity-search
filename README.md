# vector-similarity-search
A Cloudflare Worker that calculates semantic similarity between messages across topic-industry pairs to power relevance-based search.
> 🤖 Powered by [WALL-E](https://github.com/1712n/wall-e), a GitHub bot that supercharges spec-driven development through automated generation of Cloudflare Workers. 

## Implementation approach

- **Cloudflare Workers AI**: Vector embeddings (bge-base-en-v1.5) for semantic comparison
- **PostgreSQL + pgvector**: Vector storage and cosine similarity search


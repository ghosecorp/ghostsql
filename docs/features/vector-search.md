# Vector Search

GhostSQL provides first-class support for vector embeddings and similarity search.

## Vector Type

You can define columns with the `VECTOR(n)` type, where `n` is the number of dimensions.

```sql
CREATE TABLE embeddings (
    id INT,
    vec VECTOR(3)
);
```

## Similarity Operators

GhostSQL supports standard PostgreSQL similarity operators:

*   `<->`: L2 (Euclidean) distance.
*   `<=>`: Cosine distance.

### Example Query

```sql
-- Find the closest row using L2 distance
SELECT * FROM embeddings 
ORDER BY vec <-> '[0.1, 0.2, 0.3]' 
LIMIT 1;
```

## Vector Functions

For compatibility, GhostSQL also supports functional syntax:

*   `L2_DISTANCE(v1, v2)`
*   `COSINE_DISTANCE(v1, v2)`

```sql
SELECT id FROM embeddings 
ORDER BY COSINE_DISTANCE(vec, '[0.9, 0.1, 0.0]') 
LIMIT 5;
```

## Indexing (HNSW)

For large datasets, you can create an HNSW index to speed up nearest neighbor searches:

```sql
CREATE INDEX my_idx ON embeddings USING HNSW (vec) WITH (m=16, ef_construction=200);
```

## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by improving partition pruning, join strategies, caching, and aggregation efficiency. Changes focus on predicate pushdown, column pruning, join order, and reducing repeated computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Explicit column selection and predicate pushdown for partition pruning
- Join order and broadcast hints for efficient joins
- Caching expensive intermediate aggregations with CACHED hint
- Precomputing and reusing aggregates to reduce window function passes and data shuffling

> This PR was generated automatically. Please run tests and review carefully.

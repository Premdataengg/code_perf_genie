## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by improving partition pruning, column pruning, join order, and predicate pushdown. Broadcast hints and selective column usage further enhance performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Added partition pruning and column pruning to avoid full table scans
- Reordered joins and pushed down predicates for efficient join execution
- Applied BROADCAST hint only to small dimension table

> This PR was generated automatically. Please run tests and review carefully.

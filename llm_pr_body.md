## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, using CTEs for repeated subqueries, pruning columns, and limiting result sets. Broadcast hints and partition pruning are applied where appropriate.

**Files updated**
- `src/queries/anti_patterns/01_basic_select.sql`
- `src/queries/anti_patterns/02_filtering.sql`
- `src/queries/anti_patterns/03_aggregation.sql`
- `src/queries/anti_patterns/04_join.sql`
- `src/queries/anti_patterns/05_window_functions.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Pushdown filters and use CTEs to avoid repeated subqueries and enable partition pruning
- Column pruning and LIMIT reduce data shuffling and memory usage
- Broadcast hints used only for small tables
- Performance gain: less data scanned, fewer shuffles, more efficient joins

> This PR was generated automatically. Please run tests and review carefully.

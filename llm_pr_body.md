## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pruning columns, pushing down filters, reordering joins, adding CTEs for caching, and reducing redundant window aggregations for improved Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and shuffled.
- Join reordering and early filtering minimize intermediate data size.
- Caching intermediate aggregations avoids recomputation.
- Precomputing aggregations and using CTEs reduce duplicate expensive window calculations.

> This PR was generated automatically. Please run tests and review carefully.

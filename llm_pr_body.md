## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by applying predicate pushdown, column pruning, CTEs for repeated subqueries, and improved join/filter strategies. These changes enhance partition pruning, reduce data shuffling, and improve overall query performance while keeping outputs identical.

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
- Key improvement: predicate pushdown, column pruning, CTEs for repeated logic, and early filtering in joins.
- Performance gain: reduced data scanned, improved partition pruning, less shuffling, and more efficient query plans.

> This PR was generated automatically. Please run tests and review carefully.

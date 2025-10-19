## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning and reducing redundant subqueries using CTEs, improving scan efficiency and reducing computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Added partition pruning and column selection for large table scan
- Replaced repeated subqueries with a CTE to avoid redundant computation

> This PR was generated automatically. Please run tests and review carefully.

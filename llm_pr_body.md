## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by reducing redundant scans and enabling partition pruning using CTEs and column selection.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`


**Notes**
- Replaced repeated subqueries with a CTE for employee filtering to avoid multiple scans.
- Pushed filters and column selection into the main query for better partition pruning and reduced data shuffling.

> This PR was generated automatically. Please run tests and review carefully.

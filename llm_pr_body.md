## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters for partition pruning, pruning columns, and using CTEs to avoid repeated subqueries. These changes reduce data scanned and improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Pushed filters into main query for partition pruning and column pruning
- Used CTE for employee filtering to avoid repeated subqueries and scans
- Performance gain from reduced data scanned and more efficient joins

> This PR was generated automatically. Please run tests and review carefully.

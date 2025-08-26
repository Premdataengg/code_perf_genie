## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by reducing redundant scans, enabling partition pruning, adding broadcast hints, and ensuring intermediate aggregations are referenced efficiently.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Eliminated repeated subqueries with CTEs for filtered employees.
- Moved partition-pruning predicates to main query for better pruning.
- Added broadcast hint to projects join for small tables.
- Ensured expensive aggregations are referenced once for Spark caching.
- Performance gains include reduced I/O, improved join efficiency, and less recomputation.

> This PR was generated automatically. Please run tests and review carefully.

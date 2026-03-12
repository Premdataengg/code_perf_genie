## Daily Spark Optimization

**Summary**
Optimized anti-pattern query by introducing a CTE for employee filtering, enabling predicate pushdown and column pruning to reduce repeated scans and improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Removes repeated subqueries and enables Spark to scan/filter employees only once.
- Reduces data shuffling and memory usage, improving performance for large datasets.

> This PR was generated automatically. Please run tests and review carefully.

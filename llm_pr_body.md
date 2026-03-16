## Daily Spark Optimization

**Summary**
Optimized anti-pattern query by introducing a CTE for employee filtering, enabling predicate pushdown and column pruning to reduce data shuffling and improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Removes repeated subqueries by using a CTE for employee filtering
- Pushes down predicates and prunes columns early, reducing data scanned and shuffled

> This PR was generated automatically. Please run tests and review carefully.

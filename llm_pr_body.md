## Daily Spark Optimization

**Summary**
Optimized the worst anti-pattern query by pushing down filters, removing repeated subqueries, and pruning columns early. Broadcast hints are retained only for small tables.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Pushes down filters and prunes columns before joins to reduce data scanned and shuffled
- Removes repeated subqueries for employees, reducing computation and improving performance

> This PR was generated automatically. Please run tests and review carefully.

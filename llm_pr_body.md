## Daily Spark Optimization

**Summary**
The optimization focuses on column pruning and efficient use of window functions. Only necessary columns are selected in the CTE and final SELECT, reducing memory usage and shuffle. Partition pruning is enforced in the CTE WHERE clause, and window functions are partitioned and ordered for optimal performance. No behavioral changes are introduced, but the query is now more efficient for Spark SQL execution.

**Files updated**
- `src/queries/best_practices/01_best_practice_partitioning.sql`


**Notes**
- Partitioning improvement
- Column pruning
- Efficient window function usage

> This PR was generated automatically. Please run tests and review carefully.

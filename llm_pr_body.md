## Daily Spark Optimization

**Summary**
Optimized inefficient join query by filtering and pruning early, reordering joins, and using broadcast only on the small table.

**Files updated**
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Pushed down filters before joins for partition pruning and reduced data shuffling
- Used CTEs for clarity and efficiency, and removed unnecessary columns from intermediate steps

> This PR was generated automatically. Please run tests and review carefully.

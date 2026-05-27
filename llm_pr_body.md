## Daily Spark Optimization

**Summary**
Optimized inefficient queries by pushing filters before JOINs for predicate pushdown and partition pruning, selecting only required columns, and using broadcast hints for small tables. This reduces data shuffling, memory usage, and improves query execution speed.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Pushed filters into CTEs or WHERE clauses before JOINs for partition pruning
- Selected only necessary columns to minimize I/O and memory
- Added BROADCAST hint to small tables for efficient joins
- Avoided SELECT * and unnecessary data movement

> This PR was generated automatically. Please run tests and review carefully.

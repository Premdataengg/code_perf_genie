## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pruning columns, adding broadcast hints, pushing down filters, and restructuring joins to reduce data shuffling and improve partition pruning.

**Files updated**
- `src/queries/anti_patterns/01_basic_select.sql`
- `src/queries/anti_patterns/04_join.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Column pruning and explicit column selection reduce unnecessary data transfer.
- Broadcast hints and join reordering minimize shuffle and improve join efficiency.
- Filter pushdown enables partition pruning and avoids full table scans.
- Replacing CROSS JOINs with filtered JOINs prevents cartesian explosion.

> This PR was generated automatically. Please run tests and review carefully.

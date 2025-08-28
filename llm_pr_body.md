## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, pruning columns, and restructuring joins for better partition pruning and reduced shuffle.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Pushed filters into CTEs for early partition pruning and reduced data volume.
- Selected only required columns to enable column pruning and minimize I/O.
- Optimized join order and broadcast usage to reduce shuffle and improve join efficiency.

> This PR was generated automatically. Please run tests and review carefully.

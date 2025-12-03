## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, pruning columns, reordering joins, using efficient window frames, and simulating caching of expensive intermediate results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and improve partition pruning.
- Join reordering and early filtering reduce shuffle and join costs.
- Efficient window frames and simulated caching avoid recomputation and improve aggregation performance.

> This PR was generated automatically. Please run tests and review carefully.

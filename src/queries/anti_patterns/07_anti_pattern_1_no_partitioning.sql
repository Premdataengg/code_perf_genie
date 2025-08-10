-- ANTI-PATTERN 1: No Partitioning Strategy
-- This query violates the best practice of proper partitioning
-- Problem: Full table scan on large dataset without partition pruning
WITH filtered_large_dataset AS (
    SELECT employee_id, project_id, amount, department, transaction_id, date
    FROM large_dataset
    WHERE amount > 1000 
        AND amount < 50000
        AND department IN ('Engineering', 'Marketing', 'Sales')
        AND transaction_id % 2 = 0
        AND employee_id BETWEEN 1 AND 100
)
SELECT 
    employee_id,
    project_id,
    amount,
    department,
    transaction_id,
    date,
    SUM(amount) OVER (PARTITION BY department ORDER BY transaction_id) as running_total,
    AVG(amount) OVER (PARTITION BY department) as dept_avg,
    COUNT(*) OVER (PARTITION BY department) as dept_count
FROM filtered_large_dataset
ORDER BY department, amount DESC
LIMIT 1000
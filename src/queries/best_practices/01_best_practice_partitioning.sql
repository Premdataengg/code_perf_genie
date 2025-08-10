-- BEST PRACTICE 1: Proper Partitioning Strategy
-- Demonstrates efficient partitioning and filtering for better performance

-- Create partitioned view with proper filtering and column pruning
WITH partitioned_data AS (
    SELECT 
        employee_id,
        amount,
        department,
        transaction_id,
        date
    FROM (
        SELECT 
            (id % 100) as employee_id,
            (id % 10) as project_id,
            (rand(-2194833205724441416) * 100000.0) as amount,
            CASE 
                WHEN (id % 3) = 0 THEN 'Engineering'
                WHEN (id % 3) = 1 THEN 'Marketing'
                ELSE 'Sales'
            END as department,
            (id % 1000) as transaction_id,
            '2024-01-01' as date
        FROM range(0, 1000, 1)
    )
    WHERE amount > 1000.0 
    AND amount < 50000.0
    AND department IN ('Engineering', 'Marketing', 'Sales')
    AND (transaction_id % 2) = 0
    AND employee_id >= 1 
    AND employee_id <= 100
)

-- Efficient aggregation with proper partitioning and column pruning
SELECT 
    department,
    employee_id,
    amount,
    transaction_id,
    date,
    -- Use efficient window functions with partitioning
    SUM(amount) OVER (
        PARTITION BY department 
        ORDER BY transaction_id 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total,
    AVG(amount) OVER (PARTITION BY department) as dept_avg,
    COUNT(*) OVER (PARTITION BY department) as dept_count
FROM partitioned_data
ORDER BY department, amount DESC
LIMIT 50;
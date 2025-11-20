-- ANTI-PATTERN 5: Memory Inefficient Operations
-- This query violates the best practice of memory-efficient operations
-- Problem: Collecting large datasets, complex string operations, and inefficient data types
SELECT 
    e.id as employee_id,
    e.name as employee_name,
    e.department as employee_dept,
    e.salary as employee_salary,
    d.dept_name as dept_name,
    d.category as dept_category,
    d.manager as dept_manager,
    p.project_id,
    p.project_name,
    p.budget as project_budget,
    l.amount as transaction_amount,
    l.transaction_id,
    l.date as transaction_date,
    -- Only essential string/date operations
    CONCAT(e.name, ' - ', e.department) as employee_info,
    CONCAT(d.dept_name, ' (', d.category, ')') as department_info,
    CONCAT(p.project_name, ' - Budget: $', CAST(p.budget AS STRING)) as project_info,
    CONCAT('Transaction #', CAST(l.transaction_id AS STRING)) as transaction_info,
    -- Efficient aggregations using window functions
    AVG(e.salary) OVER (PARTITION BY e.department) as dept_avg_salary,
    MAX(p.budget) OVER (PARTITION BY p.department) as dept_max_budget,
    SUM(l.amount) OVER (PARTITION BY e.id) as employee_total_transactions,
    -- Simple calculations
    CAST(e.salary AS DOUBLE) / NULLIF(CAST(p.budget AS DOUBLE), 0) as salary_budget_ratio,
    CAST(l.amount AS DOUBLE) / NULLIF(CAST(e.salary AS DOUBLE), 0) as transaction_salary_ratio,
    -- Value category
    CASE 
        WHEN e.salary > 80000 AND p.budget > 100000 THEN 'High Value Employee + High Budget Project'
        WHEN e.salary > 80000 AND p.budget <= 100000 THEN 'High Value Employee + Low Budget Project'
        WHEN e.salary <= 80000 AND p.budget > 100000 THEN 'Low Value Employee + High Budget Project'
        ELSE 'Low Value Employee + Low Budget Project'
    END as value_category
FROM employees e
JOIN departments d ON e.department = d.dept_name
JOIN projects p ON e.department = p.department
JOIN large_dataset l ON e.id = l.employee_id
WHERE e.salary > 50000
    AND p.budget > 30000
    AND l.amount > 5000
    AND d.category IN ('Tech', 'Business')
    AND LENGTH(e.name) > 5
    AND LENGTH(p.project_name) > 10
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC

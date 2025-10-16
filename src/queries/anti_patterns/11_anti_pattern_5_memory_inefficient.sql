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
    -- Pruned unnecessary string/date operations and subqueries
    CONCAT(e.name, ' - ', e.department) as employee_info,
    CONCAT(d.dept_name, ' (', d.category, ')') as department_info,
    CONCAT(p.project_name, ' - Budget: $', CAST(p.budget AS STRING)) as project_info,
    CONCAT('Transaction #', CAST(l.transaction_id AS STRING)) as transaction_info,
    -- Kept only essential calculations
    CASE 
        WHEN e.salary > 80000 AND p.budget > 100000 THEN 'High Value Employee + High Budget Project'
        WHEN e.salary > 80000 AND p.budget <= 100000 THEN 'High Value Employee + Low Budget Project'
        WHEN e.salary <= 80000 AND p.budget > 100000 THEN 'Low Value Employee + High Budget Project'
        ELSE 'Low Value Employee + Low Budget Project'
    END as value_category
FROM (
    SELECT * FROM employees WHERE salary > 50000 AND LENGTH(name) > 5
) e
JOIN (
    SELECT * FROM departments WHERE category IN ('Tech', 'Business')
) d ON e.department = d.dept_name
JOIN (
    SELECT * FROM projects WHERE budget > 30000 AND LENGTH(project_name) > 10
) p ON e.department = p.department
JOIN (
    SELECT * FROM large_dataset WHERE amount > 5000
) l ON e.id = l.employee_id
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
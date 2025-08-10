-- Optimized: Remove unnecessary subqueries and expensive operations, use broadcast join for small dimension tables
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
    -- Efficient string operations
    CONCAT(e.name, ' - ', e.department) as employee_info,
    CONCAT(d.dept_name, ' (', d.category, ')') as department_info,
    CONCAT(p.project_name, ' - Budget: $', CAST(p.budget AS STRING)) as project_info,
    CONCAT('Transaction #', CAST(l.transaction_id AS STRING)) as transaction_info,
    -- Efficient date operations
    CAST(l.date AS DATE) as transaction_date_typed,
    -- Simple calculations
    CAST(e.salary AS DOUBLE) / NULLIF(CAST(p.budget AS DOUBLE), 0) as salary_budget_ratio,
    -- Conditional logic
    CASE 
        WHEN e.salary > 80000 AND p.budget > 100000 THEN 'High Value Employee + High Budget Project'
        WHEN e.salary > 80000 AND p.budget <= 100000 THEN 'High Value Employee + Low Budget Project'
        WHEN e.salary <= 80000 AND p.budget > 100000 THEN 'Low Value Employee + High Budget Project'
        ELSE 'Low Value Employee + Low Budget Project'
    END as value_category
FROM employees e
JOIN /*+ BROADCAST(d) */ departments d ON e.department = d.dept_name
JOIN projects p ON e.department = p.department
JOIN large_dataset l ON e.id = l.employee_id
WHERE e.salary > 50000
    AND p.budget > 30000
    AND l.amount > 5000
    AND d.category IN ('Tech', 'Business')
    AND LENGTH(e.name) > 5
    AND LENGTH(p.project_name) > 10
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
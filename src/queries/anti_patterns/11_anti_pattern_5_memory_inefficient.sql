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
    -- Expensive string operations
    CONCAT(e.name, ' - ', e.department, ' - ', CAST(e.salary AS STRING)) as employee_info,
    CONCAT(d.dept_name, ' (', d.category, ') - Manager: ', d.manager) as department_info,
    CONCAT(p.project_name, ' - Budget: $', CAST(p.budget AS STRING), ' - Duration: ', p.start_date, ' to ', p.end_date) as project_info,
    CONCAT('Transaction #', CAST(l.transaction_id AS STRING), ' - Amount: $', CAST(l.amount AS STRING), ' - Date: ', l.date) as transaction_info,
    -- Complex string manipulations
    UPPER(SUBSTRING(e.name, 1, 3)) as name_prefix,
    LOWER(SUBSTRING(e.department, 1, 5)) as dept_prefix,
    REPLACE(e.name, ' ', '_') as name_underscore,
    REPLACE(e.department, ' ', '_') as dept_underscore,
    -- Expensive date operations
    CAST(l.date AS DATE) as transaction_date_typed,
    DATE_ADD(CAST(l.date AS DATE), 30) as future_date,
    DATEDIFF(CAST(p.end_date AS DATE), CAST(p.start_date AS DATE)) as project_duration_days,
    -- Complex calculations with type conversions
    CAST(e.salary AS DOUBLE) / CAST(p.budget AS DOUBLE) as salary_budget_ratio,
    CAST(l.amount AS DOUBLE) / CAST(e.salary AS DOUBLE) as transaction_salary_ratio,
    CAST(p.budget AS DOUBLE) / CAST(l.amount AS DOUBLE) as budget_transaction_ratio,
    -- Expensive conditional logic
    CASE 
        WHEN e.salary > 80000 AND p.budget > 100000 THEN 'High Value Employee + High Budget Project'
        WHEN e.salary > 80000 AND p.budget <= 100000 THEN 'High Value Employee + Low Budget Project'
        WHEN e.salary <= 80000 AND p.budget > 100000 THEN 'Low Value Employee + High Budget Project'
        ELSE 'Low Value Employee + Low Budget Project'
    END as value_category,
    CASE 
        WHEN l.amount > 50000 THEN 'High Value Transaction'
        WHEN l.amount > 25000 THEN 'Medium Value Transaction'
        WHEN l.amount > 10000 THEN 'Low Value Transaction'
        ELSE 'Minimal Value Transaction'
    END as transaction_category,
    -- Complex nested conditions
    CASE 
        WHEN e.salary > 80000 AND p.budget > 100000 AND l.amount > 50000 THEN 'Premium Tier'
        WHEN e.salary > 70000 AND p.budget > 80000 AND l.amount > 30000 THEN 'Gold Tier'
        WHEN e.salary > 60000 AND p.budget > 60000 AND l.amount > 20000 THEN 'Silver Tier'
        WHEN e.salary > 50000 AND p.budget > 40000 AND l.amount > 10000 THEN 'Bronze Tier'
        ELSE 'Standard Tier'
    END as overall_tier,
    -- Expensive aggregations in subqueries
    (SELECT AVG(salary) FROM employees WHERE department = e.department) as dept_avg_salary_subquery,
    (SELECT MAX(budget) FROM projects WHERE department = p.department) as dept_max_budget_subquery,
    (SELECT SUM(amount) FROM large_dataset WHERE employee_id = e.id) as employee_total_transactions_subquery,
    -- Complex mathematical operations
    POWER(e.salary, 2) as salary_squared,
    SQRT(p.budget) as budget_sqrt,
    LOG(l.amount) as amount_log,
    EXP(e.salary / 10000) as salary_exp,
    -- String concatenation with multiple fields
    CONCAT(
        'Employee: ', e.name, 
        ' | Department: ', e.department, 
        ' | Salary: $', CAST(e.salary AS STRING),
        ' | Project: ', p.project_name,
        ' | Budget: $', CAST(p.budget AS STRING),
        ' | Transaction: $', CAST(l.amount AS STRING)
    ) as full_details
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

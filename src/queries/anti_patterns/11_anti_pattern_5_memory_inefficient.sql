-- ANTI-PATTERN 5: Memory Inefficient Operations (Optimized)
-- Optimization: Prune columns, avoid unnecessary string/date operations, remove subqueries in SELECT
WITH filtered_employees AS (
    SELECT id, name, department, salary FROM employees WHERE salary > 50000 AND LENGTH(name) > 5
),
filtered_projects AS (
    SELECT project_id, project_name, department, budget, start_date, end_date FROM projects WHERE budget > 30000 AND LENGTH(project_name) > 10
),
filtered_large_dataset AS (
    SELECT employee_id, amount, transaction_id, date FROM large_dataset WHERE amount > 5000
),
filtered_departments AS (
    SELECT dept_name, category, manager FROM departments WHERE category IN ('Tech', 'Business')
)
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
    -- Simple calculations
    CAST(e.salary AS DOUBLE) / NULLIF(CAST(p.budget AS DOUBLE), 0) as salary_budget_ratio,
    CAST(l.amount AS DOUBLE) / NULLIF(CAST(e.salary AS DOUBLE), 0) as transaction_salary_ratio,
    -- Tier logic remains unchanged
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
    CASE 
        WHEN e.salary > 80000 AND p.budget > 100000 AND l.amount > 50000 THEN 'Premium Tier'
        WHEN e.salary > 70000 AND p.budget > 80000 AND l.amount > 30000 THEN 'Gold Tier'
        WHEN e.salary > 60000 AND p.budget > 60000 AND l.amount > 20000 THEN 'Silver Tier'
        WHEN e.salary > 50000 AND p.budget > 40000 AND l.amount > 10000 THEN 'Bronze Tier'
        ELSE 'Standard Tier'
    END as overall_tier,
    -- Simple math
    POWER(e.salary, 2) as salary_squared,
    SQRT(p.budget) as budget_sqrt,
    LOG(l.amount) as amount_log
FROM filtered_employees e
JOIN filtered_departments d ON e.department = d.dept_name
JOIN filtered_projects p ON e.department = p.department
JOIN filtered_large_dataset l ON e.id = l.employee_id
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC

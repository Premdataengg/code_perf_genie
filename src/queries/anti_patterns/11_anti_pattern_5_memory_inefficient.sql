-- ANTI-PATTERN 5: Memory Inefficient Operations (Optimized)
-- Optimization: Prune columns early, move filters before joins, avoid unnecessary string/date conversions
WITH filtered_employees AS (
    SELECT id, name, department, salary
    FROM employees
    WHERE salary > 50000 AND LENGTH(name) > 5
),
filtered_departments AS (
    SELECT dept_name, category, manager
    FROM departments
    WHERE category IN ('Tech', 'Business')
),
filtered_projects AS (
    SELECT project_id, project_name, budget, department, start_date, end_date
    FROM projects
    WHERE budget > 30000 AND LENGTH(project_name) > 10
),
filtered_large_dataset AS (
    SELECT employee_id, amount, transaction_id, date
    FROM large_dataset
    WHERE amount > 5000
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
    -- Reduced string manipulations
    UPPER(SUBSTRING(e.name, 1, 3)) as name_prefix,
    LOWER(SUBSTRING(e.department, 1, 5)) as dept_prefix,
    -- Essential date operations
    CAST(l.date AS DATE) as transaction_date_typed,
    -- Key calculations
    CAST(e.salary AS DOUBLE) / CAST(p.budget AS DOUBLE) as salary_budget_ratio,
    -- Key conditional logic
    CASE 
        WHEN e.salary > 80000 AND p.budget > 100000 THEN 'High Value Employee + High Budget Project'
        ELSE 'Other'
    END as value_category,
    -- Key mathematical operations
    POWER(e.salary, 2) as salary_squared,
    SQRT(p.budget) as budget_sqrt
FROM filtered_employees e
JOIN filtered_departments d ON e.department = d.dept_name
JOIN filtered_projects p ON e.department = p.department
JOIN filtered_large_dataset l ON e.id = l.employee_id
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
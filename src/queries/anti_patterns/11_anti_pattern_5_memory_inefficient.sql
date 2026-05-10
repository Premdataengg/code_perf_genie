-- ANTI-PATTERN 5: Memory Inefficient Operations
-- This query violates the best practice of memory-efficient operations
-- Problem: Collecting large datasets, complex string operations, and inefficient data types
WITH filtered_employees AS (
    SELECT id, name, department, salary
    FROM employees
    WHERE salary > 50000 AND LENGTH(name) > 5
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
),
dept_avg_salary AS (
    SELECT department, AVG(salary) as dept_avg_salary
    FROM employees
    GROUP BY department
),
dept_max_budget AS (
    SELECT department, MAX(budget) as dept_max_budget
    FROM projects
    GROUP BY department
),
employee_total_transactions AS (
    SELECT employee_id, SUM(amount) as employee_total_transactions
    FROM large_dataset
    GROUP BY employee_id
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
    -- Efficient aggregations via joins to pre-aggregated CTEs
    das.dept_avg_salary as dept_avg_salary_subquery,
    dmb.dept_max_budget as dept_max_budget_subquery,
    ett.employee_total_transactions as employee_total_transactions_subquery,
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
FROM filtered_employees e
JOIN departments d ON e.department = d.dept_name AND d.category IN ('Tech', 'Business')
JOIN filtered_projects p ON e.department = p.department
JOIN filtered_large_dataset l ON e.id = l.employee_id
LEFT JOIN dept_avg_salary das ON e.department = das.department
LEFT JOIN dept_max_budget dmb ON p.department = dmb.department
LEFT JOIN employee_total_transactions ett ON e.id = ett.employee_id
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
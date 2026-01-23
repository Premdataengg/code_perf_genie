-- ANTI-PATTERN 5: Memory Inefficient Operations
-- Optimized: Prune columns, avoid subqueries, use window functions for aggregations, simplify string/date ops
WITH base AS (
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
        p.start_date,
        p.end_date,
        l.amount as transaction_amount,
        l.transaction_id,
        l.date as transaction_date
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
)
SELECT 
    employee_id,
    employee_name,
    employee_dept,
    employee_salary,
    dept_name,
    dept_category,
    dept_manager,
    project_id,
    project_name,
    project_budget,
    transaction_amount,
    transaction_id,
    transaction_date,
    -- Efficient string operations
    CONCAT(employee_name, ' - ', employee_dept, ' - ', CAST(employee_salary AS STRING)) as employee_info,
    CONCAT(dept_name, ' (', dept_category, ') - Manager: ', dept_manager) as department_info,
    CONCAT(project_name, ' - Budget: $', CAST(project_budget AS STRING), ' - Duration: ', start_date, ' to ', end_date) as project_info,
    CONCAT('Transaction #', CAST(transaction_id AS STRING), ' - Amount: $', CAST(transaction_amount AS STRING), ' - Date: ', transaction_date) as transaction_info,
    -- String manipulations
    UPPER(SUBSTRING(employee_name, 1, 3)) as name_prefix,
    LOWER(SUBSTRING(employee_dept, 1, 5)) as dept_prefix,
    REPLACE(employee_name, ' ', '_') as name_underscore,
    REPLACE(employee_dept, ' ', '_') as dept_underscore,
    -- Date operations
    CAST(transaction_date AS DATE) as transaction_date_typed,
    DATE_ADD(CAST(transaction_date AS DATE), 30) as future_date,
    DATEDIFF(CAST(end_date AS DATE), CAST(start_date AS DATE)) as project_duration_days,
    -- Calculations
    CAST(employee_salary AS DOUBLE) / CAST(project_budget AS DOUBLE) as salary_budget_ratio,
    CAST(transaction_amount AS DOUBLE) / CAST(employee_salary AS DOUBLE) as transaction_salary_ratio,
    CAST(project_budget AS DOUBLE) / CAST(transaction_amount AS DOUBLE) as budget_transaction_ratio,
    -- Conditional logic
    CASE 
        WHEN employee_salary > 80000 AND project_budget > 100000 THEN 'High Value Employee + High Budget Project'
        WHEN employee_salary > 80000 AND project_budget <= 100000 THEN 'High Value Employee + Low Budget Project'
        WHEN employee_salary <= 80000 AND project_budget > 100000 THEN 'Low Value Employee + High Budget Project'
        ELSE 'Low Value Employee + Low Budget Project'
    END as value_category,
    CASE 
        WHEN transaction_amount > 50000 THEN 'High Value Transaction'
        WHEN transaction_amount > 25000 THEN 'Medium Value Transaction'
        WHEN transaction_amount > 10000 THEN 'Low Value Transaction'
        ELSE 'Minimal Value Transaction'
    END as transaction_category,
    CASE 
        WHEN employee_salary > 80000 AND project_budget > 100000 AND transaction_amount > 50000 THEN 'Premium Tier'
        WHEN employee_salary > 70000 AND project_budget > 80000 AND transaction_amount > 30000 THEN 'Gold Tier'
        WHEN employee_salary > 60000 AND project_budget > 60000 AND transaction_amount > 20000 THEN 'Silver Tier'
        WHEN employee_salary > 50000 AND project_budget > 40000 AND transaction_amount > 10000 THEN 'Bronze Tier'
        ELSE 'Standard Tier'
    END as overall_tier,
    -- Aggregations using window functions instead of subqueries
    AVG(employee_salary) OVER (PARTITION BY employee_dept) as dept_avg_salary,
    MAX(project_budget) OVER (PARTITION BY employee_dept) as dept_max_budget,
    SUM(transaction_amount) OVER (PARTITION BY employee_id) as employee_total_transactions,
    -- Mathematical operations
    POWER(employee_salary, 2) as salary_squared,
    SQRT(project_budget) as budget_sqrt,
    LOG(transaction_amount) as amount_log,
    EXP(employee_salary / 10000) as salary_exp,
    -- String concatenation
    CONCAT(
        'Employee: ', employee_name, 
        ' | Department: ', employee_dept, 
        ' | Salary: $', CAST(employee_salary AS STRING),
        ' | Project: ', project_name,
        ' | Budget: $', CAST(project_budget AS STRING),
        ' | Transaction: $', CAST(transaction_amount AS STRING)
    ) as full_details
FROM base
ORDER BY employee_salary DESC, project_budget DESC, transaction_amount DESC
-- ANTI-PATTERN 4: Inefficient Aggregation Strategy (Optimized)
-- Optimization: Precompute aggregations in CTEs, reduce redundant window functions, prune columns
WITH filtered_employees AS (
    SELECT * FROM employees WHERE salary > 50000
),
filtered_projects AS (
    SELECT * FROM projects WHERE budget > 30000
),
filtered_large_dataset AS (
    SELECT * FROM large_dataset WHERE amount > 5000
),
filtered_departments AS (
    SELECT * FROM departments WHERE category IN ('Tech', 'Business')
),
emp_proj AS (
    SELECT 
        e.id as employee_id,
        e.name as employee_name,
        e.department as employee_dept,
        e.salary as employee_salary,
        d.dept_name,
        d.category,
        d.manager,
        p.project_id,
        p.project_name,
        p.budget as project_budget,
        p.department as project_dept
    FROM filtered_employees e
    JOIN filtered_departments d ON e.department = d.dept_name
    JOIN filtered_projects p ON e.department = p.department
),
joined_data AS (
    SELECT 
        ep.employee_id,
        ep.employee_name,
        ep.employee_dept,
        ep.employee_salary,
        ep.dept_name,
        ep.category,
        ep.manager,
        ep.project_id,
        ep.project_name,
        ep.project_budget,
        l.amount as transaction_amount,
        l.transaction_id,
        l.department as transaction_dept
    FROM emp_proj ep
    JOIN filtered_large_dataset l ON ep.employee_id = l.employee_id
)
SELECT 
    employee_id,
    employee_name,
    employee_dept,
    employee_salary,
    dept_name,
    category,
    manager,
    project_id,
    project_name,
    project_budget,
    transaction_amount,
    transaction_id,
    -- Only essential window functions retained
    ROW_NUMBER() OVER (ORDER BY employee_salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY employee_dept ORDER BY employee_salary DESC) as dept_salary_rank,
    AVG(employee_salary) OVER (PARTITION BY employee_dept) as dept_avg_salary,
    AVG(employee_salary) OVER (PARTITION BY category) as category_avg_salary,
    AVG(project_budget) OVER (PARTITION BY project_dept) as dept_avg_budget,
    AVG(transaction_amount) OVER (PARTITION BY transaction_dept) as dept_avg_transaction,
    SUM(employee_salary + project_budget + transaction_amount) OVER (PARTITION BY employee_dept) as dept_total_value,
    SUM(employee_salary + project_budget + transaction_amount) OVER (PARTITION BY category) as category_total_value
FROM joined_data
ORDER BY employee_salary DESC, project_budget DESC, transaction_amount DESC
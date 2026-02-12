-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Precompute aggregations, reduce redundant window functions, select only needed columns
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
        e.name,
        e.department,
        e.salary,
        d.dept_name,
        d.category,
        d.manager,
        p.project_id,
        p.project_name,
        p.budget,
        p.department as project_department
    FROM filtered_employees e
    JOIN filtered_departments d ON e.department = d.dept_name
    JOIN filtered_projects p ON e.department = p.department
),
joined_data AS (
    SELECT 
        ep.employee_id,
        ep.name,
        ep.department,
        ep.salary,
        ep.dept_name,
        ep.category,
        ep.manager,
        ep.project_id,
        ep.project_name,
        ep.budget,
        l.amount,
        l.transaction_id,
        l.department as l_department
    FROM emp_proj ep
    JOIN filtered_large_dataset l ON ep.employee_id = l.employee_id
)
SELECT 
    employee_id,
    name as employee_name,
    department as employee_dept,
    salary as employee_salary,
    dept_name,
    category as dept_category,
    manager as dept_manager,
    project_id,
    project_name,
    budget as project_budget,
    amount as transaction_amount,
    transaction_id,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY project_department ORDER BY budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY l_department ORDER BY amount DESC) as transaction_amount_rank,
    AVG(salary) OVER (PARTITION BY department) as dept_avg_salary,
    AVG(salary) OVER (PARTITION BY category) as category_avg_salary,
    AVG(budget) OVER (PARTITION BY project_department) as dept_avg_budget,
    AVG(amount) OVER (PARTITION BY l_department) as dept_avg_transaction,
    SUM(salary + budget + amount) OVER (PARTITION BY department) as dept_total_value,
    SUM(salary + budget + amount) OVER (PARTITION BY category) as category_total_value,
    DENSE_RANK() OVER (ORDER BY salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY salary DESC) as category_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary) as dept_salary_percentile,
    LAG(salary, 1) OVER (ORDER BY salary DESC) as prev_salary,
    LEAD(salary, 1) OVER (ORDER BY salary DESC) as next_salary,
    LAG(budget, 1) OVER (PARTITION BY project_department ORDER BY budget DESC) as prev_budget,
    LEAD(budget, 1) OVER (PARTITION BY project_department ORDER BY budget DESC) as next_budget,
    SUM(CASE WHEN salary > 70000 THEN salary ELSE 0 END) OVER (PARTITION BY department) as dept_high_salary_sum,
    SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) OVER (PARTITION BY project_department) as dept_high_budget_sum,
    COUNT(CASE WHEN salary > 70000 THEN 1 END) OVER (PARTITION BY department) as dept_high_salary_count,
    COUNT(CASE WHEN budget > 80000 THEN 1 END) OVER (PARTITION BY project_department) as dept_high_budget_count
FROM joined_data
ORDER BY salary DESC, budget DESC, amount DESC
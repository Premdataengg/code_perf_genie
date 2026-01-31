-- ANTI-PATTERN 4: Inefficient Aggregation Strategy (Optimized)
-- Optimization: Reduce redundant window functions, prune columns, use CTEs for repeated calculations
WITH filtered_employees AS (
    SELECT id, name, department, salary
    FROM employees
    WHERE salary > 50000
),
filtered_projects AS (
    SELECT project_id, project_name, budget, department
    FROM projects
    WHERE budget > 30000
),
filtered_large_dataset AS (
    SELECT employee_id, amount, transaction_id, department
    FROM large_dataset
    WHERE amount > 5000
),
joined_data AS (
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
        p.budget,
        l.amount,
        l.transaction_id,
        e.salary as base_salary,
        p.budget as base_budget,
        l.amount as base_amount
    FROM filtered_employees e
    JOIN departments d ON e.department = d.dept_name AND d.category IN ('Tech', 'Business')
    JOIN filtered_projects p ON e.department = p.department
    JOIN filtered_large_dataset l ON e.id = l.employee_id
)
SELECT 
    employee_id,
    employee_name,
    employee_dept,
    employee_salary,
    dept_name,
    category as dept_category,
    manager as dept_manager,
    project_id,
    project_name,
    budget as project_budget,
    amount as transaction_amount,
    transaction_id,
    ROW_NUMBER() OVER (ORDER BY employee_salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY employee_dept ORDER BY employee_salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY employee_salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY employee_dept ORDER BY budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY employee_dept ORDER BY amount DESC) as transaction_amount_rank,
    AVG(employee_salary) OVER (PARTITION BY employee_dept) as dept_avg_salary,
    AVG(employee_salary) OVER (PARTITION BY category) as category_avg_salary,
    AVG(budget) OVER (PARTITION BY employee_dept) as dept_avg_budget,
    AVG(amount) OVER (PARTITION BY employee_dept) as dept_avg_transaction,
    SUM(employee_salary + budget + amount) OVER (PARTITION BY employee_dept) as dept_total_value,
    SUM(employee_salary + budget + amount) OVER (PARTITION BY category) as category_total_value,
    DENSE_RANK() OVER (ORDER BY employee_salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY employee_dept ORDER BY employee_salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY employee_salary DESC) as category_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY employee_salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY employee_dept ORDER BY employee_salary) as dept_salary_percentile,
    LAG(employee_salary, 1) OVER (ORDER BY employee_salary DESC) as prev_salary,
    LEAD(employee_salary, 1) OVER (ORDER BY employee_salary DESC) as next_salary,
    LAG(budget, 1) OVER (PARTITION BY employee_dept ORDER BY budget DESC) as prev_budget,
    LEAD(budget, 1) OVER (PARTITION BY employee_dept ORDER BY budget DESC) as next_budget,
    SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) OVER (PARTITION BY employee_dept) as dept_high_salary_sum,
    SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) OVER (PARTITION BY employee_dept) as dept_high_budget_sum,
    COUNT(1) FILTER (WHERE employee_salary > 70000) OVER (PARTITION BY employee_dept) as dept_high_salary_count,
    COUNT(1) FILTER (WHERE budget > 80000) OVER (PARTITION BY employee_dept) as dept_high_budget_count
FROM joined_data
ORDER BY employee_salary DESC, budget DESC, amount DESC

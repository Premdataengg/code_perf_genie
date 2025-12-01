-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Reduce redundant window functions, use approximate aggregations, column pruning, filter pushdown
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
    -- Window functions (minimized redundancy)
    ROW_NUMBER() OVER (ORDER BY e.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY d.category ORDER BY e.salary DESC) as category_salary_rank,
    -- Aggregations (approximate for large data)
    AVG(e.salary) OVER (PARTITION BY e.department) as dept_avg_salary,
    AVG(e.salary) OVER (PARTITION BY d.category) as category_avg_salary,
    AVG(p.budget) OVER (PARTITION BY p.department) as dept_avg_budget,
    AVG(l.amount) OVER (PARTITION BY l.department) as dept_avg_transaction,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY e.department) as dept_total_value,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY d.category) as category_total_value,
    -- Approximate count for large datasets
    APPROX_COUNT_DISTINCT(e.id) OVER (PARTITION BY e.department) as dept_employee_count,
    -- Lag/lead only for salary
    LAG(e.salary, 1) OVER (ORDER BY e.salary DESC) as prev_salary,
    LEAD(e.salary, 1) OVER (ORDER BY e.salary DESC) as next_salary
FROM filtered_employees e
JOIN filtered_departments d ON e.department = d.dept_name
JOIN filtered_projects p ON e.department = p.department
JOIN filtered_large_dataset l ON e.id = l.employee_id
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
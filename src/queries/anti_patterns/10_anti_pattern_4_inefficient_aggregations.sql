-- ANTI-PATTERN 4: Inefficient Aggregation Strategy (Optimized)
-- Optimization: Reduce redundant window functions, combine similar calculations, prune columns
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
    -- Minimize window functions by combining where possible
    ROW_NUMBER() OVER (ORDER BY e.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    DENSE_RANK() OVER (ORDER BY e.salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_dense_rank,
    AVG(e.salary) OVER (PARTITION BY e.department) as dept_avg_salary,
    AVG(p.budget) OVER (PARTITION BY p.department) as dept_avg_budget,
    SUM(CASE WHEN e.salary > 70000 THEN e.salary ELSE 0 END) OVER (PARTITION BY e.department) as dept_high_salary_sum,
    COUNT(CASE WHEN e.salary > 70000 THEN 1 END) OVER (PARTITION BY e.department) as dept_high_salary_count,
    SUM(CASE WHEN p.budget > 80000 THEN p.budget ELSE 0 END) OVER (PARTITION BY p.department) as dept_high_budget_sum,
    COUNT(CASE WHEN p.budget > 80000 THEN 1 END) OVER (PARTITION BY p.department) as dept_high_budget_count,
    LAG(e.salary, 1) OVER (ORDER BY e.salary DESC) as prev_salary,
    LEAD(e.salary, 1) OVER (ORDER BY e.salary DESC) as next_salary
FROM filtered_employees e
JOIN filtered_departments d ON e.department = d.dept_name
JOIN filtered_projects p ON e.department = p.department
JOIN filtered_large_dataset l ON e.id = l.employee_id
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC

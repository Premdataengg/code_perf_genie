-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Reduce redundant window functions, combine aggregations, and prune columns
WITH filtered_employees AS (
    SELECT id, name, department, salary FROM employees WHERE salary > 50000
),
filtered_projects AS (
    SELECT project_id, project_name, budget, department FROM projects WHERE budget > 30000
),
filtered_large_dataset AS (
    SELECT employee_id, amount, transaction_id, department FROM large_dataset WHERE amount > 5000
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
    -- Only essential ranking and aggregation window functions
    ROW_NUMBER() OVER (ORDER BY e.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    DENSE_RANK() OVER (ORDER BY e.salary DESC) as salary_dense_rank,
    AVG(e.salary) OVER (PARTITION BY e.department) as dept_avg_salary,
    SUM(CASE WHEN e.salary > 70000 THEN e.salary ELSE 0 END) OVER (PARTITION BY e.department) as dept_high_salary_sum,
    COUNT(CASE WHEN e.salary > 70000 THEN 1 END) OVER (PARTITION BY e.department) as dept_high_salary_count
FROM filtered_employees e
JOIN departments d ON e.department = d.dept_name AND d.category IN ('Tech', 'Business')
JOIN filtered_projects p ON e.department = p.department
JOIN filtered_large_dataset l ON e.id = l.employee_id
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC

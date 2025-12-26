-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- This query violates the best practice of efficient aggregations
-- Problem: Multiple passes over data, complex window functions, and inefficient grouping
-- OPTIMIZED: Reduce redundant window functions, prune columns, and use efficient aggregations
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
    -- Keep only essential window functions
    ROW_NUMBER() OVER (ORDER BY e.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY d.category ORDER BY e.salary DESC) as category_salary_rank,
    -- Use efficient window aggregations
    AVG(e.salary) OVER (PARTITION BY e.department) as dept_avg_salary,
    AVG(e.salary) OVER (PARTITION BY d.category) as category_avg_salary,
    AVG(p.budget) OVER (PARTITION BY p.department) as dept_avg_budget,
    AVG(l.amount) OVER (PARTITION BY l.department) as dept_avg_transaction,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY e.department) as dept_total_value,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY d.category) as category_total_value,
    -- Use SUM(CASE WHEN ...) for conditional counts
    SUM(CASE WHEN e.salary > 70000 THEN e.salary ELSE 0 END) OVER (PARTITION BY e.department) as dept_high_salary_sum,
    SUM(CASE WHEN p.budget > 80000 THEN p.budget ELSE 0 END) OVER (PARTITION BY p.department) as dept_high_budget_sum,
    SUM(CASE WHEN e.salary > 70000 THEN 1 ELSE 0 END) OVER (PARTITION BY e.department) as dept_high_salary_count,
    SUM(CASE WHEN p.budget > 80000 THEN 1 ELSE 0 END) OVER (PARTITION BY p.department) as dept_high_budget_count
FROM employees e
JOIN departments d ON e.department = d.dept_name AND d.category IN ('Tech', 'Business')
JOIN projects p ON e.department = p.department AND p.budget > 30000
JOIN large_dataset l ON e.id = l.employee_id AND l.amount > 5000
WHERE e.salary > 50000
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
-- NOTE: For large datasets, consider pre-aggregating where possible and avoid unnecessary window functions.
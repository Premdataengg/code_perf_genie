-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- This query violates the best practice of efficient aggregations
-- Problem: Multiple passes over data, complex window functions, and inefficient grouping
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
    ROW_NUMBER() OVER (ORDER BY e.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY d.category ORDER BY e.salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY p.department ORDER BY p.budget DESC) as project_budget_rank,
    -- Removed redundant window functions and pruned columns
    AVG(e.salary) OVER (PARTITION BY e.department) as dept_avg_salary,
    AVG(p.budget) OVER (PARTITION BY p.department) as dept_avg_budget,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY e.department) as dept_total_value,
    DENSE_RANK() OVER (ORDER BY e.salary DESC) as salary_dense_rank,
    -- Kept only essential ranking and aggregation window functions
    PERCENT_RANK() OVER (ORDER BY e.salary) as salary_percentile,
    LAG(e.salary, 1) OVER (ORDER BY e.salary DESC) as prev_salary,
    LEAD(e.salary, 1) OVER (ORDER BY e.salary DESC) as next_salary,
    SUM(CASE WHEN e.salary > 70000 THEN e.salary ELSE 0 END) OVER (PARTITION BY e.department) as dept_high_salary_sum,
    COUNT(CASE WHEN e.salary > 70000 THEN 1 END) OVER (PARTITION BY e.department) as dept_high_salary_count
FROM (
    SELECT * FROM employees WHERE salary > 50000
) e
JOIN (
    SELECT * FROM departments WHERE category IN ('Tech', 'Business')
) d ON e.department = d.dept_name
JOIN (
    SELECT * FROM projects WHERE budget > 30000
) p ON e.department = p.department
JOIN (
    SELECT * FROM large_dataset WHERE amount > 5000
) l ON e.id = l.employee_id
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
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
    -- Consolidated window functions
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    DENSE_RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_dense_rank,
    AVG(e.salary) OVER (PARTITION BY e.department) as dept_avg_salary,
    SUM(CASE WHEN e.salary > 70000 THEN e.salary ELSE 0 END) OVER (PARTITION BY e.department) as dept_high_salary_sum,
    COUNT(CASE WHEN e.salary > 70000 THEN 1 END) OVER (PARTITION BY e.department) as dept_high_salary_count,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY e.department) as dept_total_value,
    PERCENT_RANK() OVER (PARTITION BY e.department ORDER BY e.salary) as dept_salary_percentile,
    LAG(e.salary, 1) OVER (PARTITION BY e.department ORDER BY e.salary DESC) as prev_salary,
    LEAD(e.salary, 1) OVER (PARTITION BY e.department ORDER BY e.salary DESC) as next_salary
FROM employees e
JOIN departments d ON e.department = d.dept_name
JOIN projects p ON e.department = p.department
JOIN large_dataset l ON e.id = l.employee_id
WHERE e.salary > 50000
    AND p.budget > 30000
    AND l.amount > 5000
    AND d.category IN ('Tech', 'Business')
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
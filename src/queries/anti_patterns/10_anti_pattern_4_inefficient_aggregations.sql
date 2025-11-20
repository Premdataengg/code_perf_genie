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
    -- Only essential window functions
    ROW_NUMBER() OVER (ORDER BY e.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    DENSE_RANK() OVER (ORDER BY e.salary DESC) as salary_dense_rank,
    -- Aggregations only once per partition
    AVG(e.salary) OVER (PARTITION BY e.department) as dept_avg_salary,
    AVG(p.budget) OVER (PARTITION BY p.department) as dept_avg_budget,
    SUM(CASE WHEN e.salary > 70000 THEN e.salary ELSE 0 END) OVER (PARTITION BY e.department) as dept_high_salary_sum,
    SUM(CASE WHEN p.budget > 80000 THEN p.budget ELSE 0 END) OVER (PARTITION BY p.department) as dept_high_budget_sum,
    SUM(CASE WHEN e.salary > 70000 THEN 1 ELSE 0 END) OVER (PARTITION BY e.department) as dept_high_salary_count,
    SUM(CASE WHEN p.budget > 80000 THEN 1 ELSE 0 END) OVER (PARTITION BY p.department) as dept_high_budget_count
FROM employees e
JOIN departments d ON e.department = d.dept_name
JOIN projects p ON e.department = p.department
JOIN large_dataset l ON e.id = l.employee_id
WHERE e.salary > 50000
    AND p.budget > 30000
    AND l.amount > 5000
    AND d.category IN ('Tech', 'Business')
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC

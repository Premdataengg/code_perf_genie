-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Use pre-aggregated CTEs, combine window functions, and minimize multiple passes
WITH dept_stats AS (
    SELECT department, AVG(salary) as dept_avg_salary, SUM(CASE WHEN salary > 70000 THEN salary ELSE 0 END) as dept_high_salary_sum, SUM(CASE WHEN salary > 70000 THEN 1 ELSE 0 END) as dept_high_salary_count
    FROM employees WHERE salary > 50000 GROUP BY department
),
category_stats AS (
    SELECT d.category, AVG(e.salary) as category_avg_salary
    FROM employees e JOIN departments d ON e.department = d.dept_name WHERE e.salary > 50000 GROUP BY d.category
),
project_stats AS (
    SELECT department, AVG(budget) as dept_avg_budget, SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) as dept_high_budget_sum, SUM(CASE WHEN budget > 80000 THEN 1 ELSE 0 END) as dept_high_budget_count
    FROM projects WHERE budget > 30000 GROUP BY department
),
transaction_stats AS (
    SELECT department, AVG(amount) as dept_avg_transaction
    FROM large_dataset WHERE amount > 5000 GROUP BY department
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
    ROW_NUMBER() OVER (ORDER BY e.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY d.category ORDER BY e.salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY p.department ORDER BY p.budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY l.department ORDER BY l.amount DESC) as transaction_amount_rank,
    ds.dept_avg_salary,
    cs.category_avg_salary,
    ps.dept_avg_budget,
    ts.dept_avg_transaction,
    (e.salary + p.budget + l.amount) as total_value,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY e.department) as dept_total_value,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY d.category) as category_total_value,
    DENSE_RANK() OVER (ORDER BY e.salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY d.category ORDER BY e.salary DESC) as category_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY e.salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY e.department ORDER BY e.salary) as dept_salary_percentile,
    LAG(e.salary, 1) OVER (ORDER BY e.salary DESC) as prev_salary,
    LEAD(e.salary, 1) OVER (ORDER BY e.salary DESC) as next_salary,
    LAG(p.budget, 1) OVER (PARTITION BY p.department ORDER BY p.budget DESC) as prev_budget,
    LEAD(p.budget, 1) OVER (PARTITION BY p.department ORDER BY p.budget DESC) as next_budget,
    ds.dept_high_salary_sum,
    ps.dept_high_budget_sum,
    ds.dept_high_salary_count,
    ps.dept_high_budget_count
FROM employees e
JOIN departments d ON e.department = d.dept_name
JOIN projects p ON e.department = p.department
JOIN large_dataset l ON e.id = l.employee_id
LEFT JOIN dept_stats ds ON e.department = ds.department
LEFT JOIN category_stats cs ON d.category = cs.category
LEFT JOIN project_stats ps ON p.department = ps.department
LEFT JOIN transaction_stats ts ON l.department = ts.department
WHERE e.salary > 50000
    AND p.budget > 30000
    AND l.amount > 5000
    AND d.category IN ('Tech', 'Business')
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
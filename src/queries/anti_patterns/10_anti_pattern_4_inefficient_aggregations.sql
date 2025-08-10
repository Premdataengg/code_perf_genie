-- Optimized: Reduce redundant window functions and precompute aggregations
WITH dept_salary_stats AS (
    SELECT department, AVG(salary) as dept_avg_salary, COUNT(*) as dept_count, SUM(CASE WHEN salary > 70000 THEN salary ELSE 0 END) as dept_high_salary_sum, COUNT(CASE WHEN salary > 70000 THEN 1 END) as dept_high_salary_count
    FROM employees
    GROUP BY department
),
dept_budget_stats AS (
    SELECT department, AVG(budget) as dept_avg_budget, SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) as dept_high_budget_sum, COUNT(CASE WHEN budget > 80000 THEN 1 END) as dept_high_budget_count
    FROM projects
    GROUP BY department
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
    DENSE_RANK() OVER (ORDER BY e.salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_dense_rank,
    ds.dept_avg_salary,
    ds.dept_count,
    dbs.dept_avg_budget,
    dbs.dept_high_budget_sum,
    dbs.dept_high_budget_count,
    ds.dept_high_salary_sum,
    ds.dept_high_salary_count
FROM employees e
JOIN /*+ BROADCAST(d) */ departments d ON e.department = d.dept_name
JOIN projects p ON e.department = p.department
JOIN large_dataset l ON e.id = l.employee_id
LEFT JOIN dept_salary_stats ds ON e.department = ds.department
LEFT JOIN dept_budget_stats dbs ON p.department = dbs.department
WHERE e.salary > 50000
    AND p.budget > 30000
    AND l.amount > 5000
    AND d.category IN ('Tech', 'Business')
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
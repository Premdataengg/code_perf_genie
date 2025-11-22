-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Precompute aggregates, reduce window function passes, and reuse results
WITH dept_agg AS (
    SELECT department, AVG(salary) as dept_avg_salary, COUNT(*) as dept_count, SUM(CASE WHEN salary > 70000 THEN salary ELSE 0 END) as dept_high_salary_sum, COUNT(CASE WHEN salary > 70000 THEN 1 END) as dept_high_salary_count
    FROM employees
    GROUP BY department
),
cat_agg AS (
    SELECT d.category, AVG(e.salary) as category_avg_salary, SUM(e.salary + p.budget + l.amount) as category_total_value
    FROM employees e
    JOIN departments d ON e.department = d.dept_name
    JOIN projects p ON e.department = p.department
    JOIN large_dataset l ON e.id = l.employee_id
    WHERE e.salary > 50000 AND p.budget > 30000 AND l.amount > 5000 AND d.category IN ('Tech', 'Business')
    GROUP BY d.category
),
proj_agg AS (
    SELECT department, AVG(budget) as dept_avg_budget, SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) as dept_high_budget_sum, COUNT(CASE WHEN budget > 80000 THEN 1 END) as dept_high_budget_count
    FROM projects
    GROUP BY department
),
trans_agg AS (
    SELECT department, AVG(amount) as dept_avg_transaction
    FROM large_dataset
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
    PERCENT_RANK() OVER (ORDER BY e.salary) as salary_percentile,
    LAG(e.salary, 1) OVER (ORDER BY e.salary DESC) as prev_salary,
    LEAD(e.salary, 1) OVER (ORDER BY e.salary DESC) as next_salary,
    LAG(p.budget, 1) OVER (PARTITION BY p.department ORDER BY p.budget DESC) as prev_budget,
    LEAD(p.budget, 1) OVER (PARTITION BY p.department ORDER BY p.budget DESC) as next_budget,
    da.dept_avg_salary,
    da.dept_count,
    pa.dept_avg_budget,
    ta.dept_avg_transaction,
    da.dept_high_salary_sum,
    pa.dept_high_budget_sum,
    da.dept_high_salary_count,
    pa.dept_high_budget_count,
    ca.category_avg_salary,
    ca.category_total_value,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY e.department) as dept_total_value
FROM employees e
JOIN departments d ON e.department = d.dept_name
JOIN projects p ON e.department = p.department
JOIN large_dataset l ON e.id = l.employee_id
JOIN dept_agg da ON e.department = da.department
JOIN proj_agg pa ON e.department = pa.department
JOIN trans_agg ta ON e.department = ta.department
JOIN cat_agg ca ON d.category = ca.category
WHERE e.salary > 50000
    AND p.budget > 30000
    AND l.amount > 5000
    AND d.category IN ('Tech', 'Business')
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC

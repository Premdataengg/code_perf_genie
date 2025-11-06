-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- This query violates the best practice of efficient aggregations
-- Problem: Multiple passes over data, complex window functions, and inefficient grouping
WITH emp_filtered AS (
    SELECT id, name, department, salary FROM employees WHERE salary > 50000
),
dep_filtered AS (
    SELECT dept_name, category, manager FROM departments WHERE category IN ('Tech', 'Business')
),
proj_filtered AS (
    SELECT project_id, project_name, budget, department FROM projects WHERE budget > 30000
),
large_filtered AS (
    SELECT amount, transaction_id, department, employee_id FROM large_dataset WHERE amount > 5000
),
dept_salary_agg AS (
    SELECT department, AVG(salary) as dept_avg_salary, COUNT(*) as dept_count, SUM(CASE WHEN salary > 70000 THEN salary ELSE 0 END) as dept_high_salary_sum, COUNT(CASE WHEN salary > 70000 THEN 1 END) as dept_high_salary_count FROM emp_filtered GROUP BY department
),
cat_salary_agg AS (
    SELECT d.category, AVG(e.salary) as category_avg_salary, SUM(e.salary + 0) as category_total_value FROM emp_filtered e JOIN dep_filtered d ON e.department = d.dept_name GROUP BY d.category
),
dept_budget_agg AS (
    SELECT department, AVG(budget) as dept_avg_budget, SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) as dept_high_budget_sum, COUNT(CASE WHEN budget > 80000 THEN 1 END) as dept_high_budget_count FROM proj_filtered GROUP BY department
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
    db.dept_avg_budget,
    -- Only keep one window sum for dept_total_value
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY e.department) as dept_total_value,
    cs.category_total_value,
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
    db.dept_high_budget_sum,
    ds.dept_high_salary_count,
    db.dept_high_budget_count
FROM emp_filtered e
JOIN dep_filtered d ON e.department = d.dept_name
JOIN proj_filtered p ON e.department = p.department
JOIN large_filtered l ON e.id = l.employee_id
LEFT JOIN dept_salary_agg ds ON e.department = ds.department
LEFT JOIN cat_salary_agg cs ON d.category = cs.category
LEFT JOIN dept_budget_agg db ON p.department = db.department
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
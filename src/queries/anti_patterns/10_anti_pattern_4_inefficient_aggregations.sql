-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Precompute aggregates and reuse them, prune columns early, avoid redundant window functions
WITH filtered_employees AS (
    SELECT id, name, department, salary FROM employees WHERE salary > 50000
),
filtered_projects AS (
    SELECT project_id, project_name, department, budget FROM projects WHERE budget > 30000
),
filtered_large_dataset AS (
    SELECT employee_id, amount, transaction_id, department FROM large_dataset WHERE amount > 5000
),
filtered_departments AS (
    SELECT dept_name, category, manager FROM departments WHERE category IN ('Tech', 'Business')
),
department_salary_aggs AS (
    SELECT department, AVG(salary) as dept_avg_salary, COUNT(*) as dept_salary_count, SUM(salary) as dept_salary_sum FROM filtered_employees GROUP BY department
),
category_salary_aggs AS (
    SELECT d.category, AVG(e.salary) as category_avg_salary, COUNT(*) as category_salary_count, SUM(e.salary) as category_salary_sum
    FROM filtered_employees e
    JOIN filtered_departments d ON e.department = d.dept_name
    GROUP BY d.category
),
dept_budget_aggs AS (
    SELECT department, AVG(budget) as dept_avg_budget, SUM(budget) as dept_budget_sum FROM filtered_projects GROUP BY department
),
dept_transaction_aggs AS (
    SELECT department, AVG(amount) as dept_avg_transaction, SUM(amount) as dept_transaction_sum FROM filtered_large_dataset GROUP BY department
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
    dt.dept_avg_transaction,
    (e.salary + p.budget + l.amount) OVER (PARTITION BY e.department) as dept_total_value,
    (e.salary + p.budget + l.amount) OVER (PARTITION BY d.category) as category_total_value,
    DENSE_RANK() OVER (ORDER BY e.salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY d.category ORDER BY e.salary DESC) as category_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY e.salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY e.department ORDER BY e.salary) as dept_salary_percentile,
    LAG(e.salary, 1) OVER (ORDER BY e.salary DESC) as prev_salary,
    LEAD(e.salary, 1) OVER (ORDER BY e.salary DESC) as next_salary,
    LAG(p.budget, 1) OVER (PARTITION BY p.department ORDER BY p.budget DESC) as prev_budget,
    LEAD(p.budget, 1) OVER (PARTITION BY p.department ORDER BY p.budget DESC) as next_budget,
    SUM(CASE WHEN e.salary > 70000 THEN e.salary ELSE 0 END) OVER (PARTITION BY e.department) as dept_high_salary_sum,
    SUM(CASE WHEN p.budget > 80000 THEN p.budget ELSE 0 END) OVER (PARTITION BY p.department) as dept_high_budget_sum,
    COUNT(CASE WHEN e.salary > 70000 THEN 1 END) OVER (PARTITION BY e.department) as dept_high_salary_count,
    COUNT(CASE WHEN p.budget > 80000 THEN 1 END) OVER (PARTITION BY p.department) as dept_high_budget_count
FROM filtered_employees e
JOIN filtered_departments d ON e.department = d.dept_name
JOIN filtered_projects p ON e.department = p.department
JOIN filtered_large_dataset l ON e.id = l.employee_id
LEFT JOIN department_salary_aggs ds ON e.department = ds.department
LEFT JOIN category_salary_aggs cs ON d.category = cs.category
LEFT JOIN dept_budget_aggs db ON p.department = db.department
LEFT JOIN dept_transaction_aggs dt ON l.department = dt.department
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
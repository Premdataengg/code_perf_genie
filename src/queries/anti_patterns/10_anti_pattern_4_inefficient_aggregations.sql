-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Prune columns, push down filters, pre-aggregate stats to avoid repeated window aggregations
WITH filtered_employees AS (
    SELECT id, name, department, salary FROM employees WHERE salary > 50000
),
filtered_departments AS (
    SELECT dept_name, category, manager FROM departments WHERE category IN ('Tech', 'Business')
),
filtered_projects AS (
    SELECT project_id, project_name, department, budget FROM projects WHERE budget > 30000
),
filtered_large_dataset AS (
    SELECT employee_id, department as l_department, amount, transaction_id FROM large_dataset WHERE amount > 5000
),
dept_salary_stats AS (
    SELECT department, AVG(salary) as dept_avg_salary, SUM(CASE WHEN salary > 70000 THEN salary ELSE 0 END) as dept_high_salary_sum, COUNT(CASE WHEN salary > 70000 THEN 1 END) as dept_high_salary_count FROM filtered_employees GROUP BY department
),
category_salary_stats AS (
    SELECT d.category, AVG(e.salary) as category_avg_salary FROM filtered_employees e JOIN filtered_departments d ON e.department = d.dept_name GROUP BY d.category
),
dept_budget_stats AS (
    SELECT department, AVG(budget) as dept_avg_budget, SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) as dept_high_budget_sum, COUNT(CASE WHEN budget > 80000 THEN 1 END) as dept_high_budget_count FROM filtered_projects GROUP BY department
),
dept_transaction_stats AS (
    SELECT l_department, AVG(amount) as dept_avg_transaction FROM filtered_large_dataset GROUP BY l_department
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
    -- Window functions (keep only most relevant)
    ROW_NUMBER() OVER (ORDER BY e.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    DENSE_RANK() OVER (ORDER BY e.salary DESC) as salary_dense_rank,
    -- Aggregated stats via joins
    ds.dept_avg_salary,
    cs.category_avg_salary,
    dbs.dept_avg_budget,
    dts.dept_avg_transaction,
    ds.dept_high_salary_sum,
    dbs.dept_high_budget_sum,
    ds.dept_high_salary_count,
    dbs.dept_high_budget_count
FROM filtered_employees e
JOIN filtered_departments d ON e.department = d.dept_name
JOIN filtered_projects p ON e.department = p.department
JOIN filtered_large_dataset l ON e.id = l.employee_id
LEFT JOIN dept_salary_stats ds ON e.department = ds.department
LEFT JOIN category_salary_stats cs ON d.category = cs.category
LEFT JOIN dept_budget_stats dbs ON p.department = dbs.department
LEFT JOIN dept_transaction_stats dts ON l.l_department = dts.l_department
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC
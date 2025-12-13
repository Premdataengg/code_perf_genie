-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- This query violates the best practice of efficient aggregations
-- Problem: Multiple passes over data, complex window functions, and inefficient grouping
WITH 
emp_filtered AS (
    SELECT id, name, department, salary FROM employees WHERE salary > 50000
),
dept_agg AS (
    SELECT department, AVG(salary) as dept_avg_salary, COUNT(*) as dept_count, SUM(CASE WHEN salary > 70000 THEN salary ELSE 0 END) as dept_high_salary_sum, COUNT(CASE WHEN salary > 70000 THEN 1 END) as dept_high_salary_count FROM emp_filtered GROUP BY department
),
cat_agg AS (
    SELECT d.category, AVG(e.salary) as category_avg_salary FROM emp_filtered e JOIN departments d ON e.department = d.dept_name WHERE d.category IN ('Tech', 'Business') GROUP BY d.category
),
proj_agg AS (
    SELECT department, AVG(budget) as dept_avg_budget, SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) as dept_high_budget_sum, COUNT(CASE WHEN budget > 80000 THEN 1 END) as dept_high_budget_count FROM projects WHERE budget > 30000 GROUP BY department
),
emp_proj AS (
    SELECT e.*, d.dept_name, d.category, d.manager, p.project_id, p.project_name, p.budget, p.department as p_department FROM emp_filtered e JOIN departments d ON e.department = d.dept_name AND d.category IN ('Tech', 'Business') JOIN projects p ON e.department = p.department WHERE p.budget > 30000
),
emp_proj_large AS (
    SELECT ep.*, l.amount, l.transaction_id, l.department as l_department FROM emp_proj ep JOIN large_dataset l ON ep.id = l.employee_id WHERE l.amount > 5000
)
SELECT 
    epl.id as employee_id,
    epl.name as employee_name,
    epl.department as employee_dept,
    epl.salary as employee_salary,
    epl.dept_name,
    epl.category as dept_category,
    epl.manager as dept_manager,
    epl.project_id,
    epl.project_name,
    epl.budget as project_budget,
    epl.amount as transaction_amount,
    epl.transaction_id,
    ROW_NUMBER() OVER (ORDER BY epl.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY epl.department ORDER BY epl.salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY epl.category ORDER BY epl.salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY epl.p_department ORDER BY epl.budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY epl.l_department ORDER BY epl.amount DESC) as transaction_amount_rank,
    da.dept_avg_salary,
    ca.category_avg_salary,
    pa.dept_avg_budget,
    AVG(epl.amount) OVER (PARTITION BY epl.l_department) as dept_avg_transaction,
    SUM(epl.salary + epl.budget + epl.amount) OVER (PARTITION BY epl.department) as dept_total_value,
    SUM(epl.salary + epl.budget + epl.amount) OVER (PARTITION BY epl.category) as category_total_value,
    DENSE_RANK() OVER (ORDER BY epl.salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY epl.department ORDER BY epl.salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY epl.category ORDER BY epl.salary DESC) as category_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY epl.salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY epl.department ORDER BY epl.salary) as dept_salary_percentile,
    LAG(epl.salary, 1) OVER (ORDER BY epl.salary DESC) as prev_salary,
    LEAD(epl.salary, 1) OVER (ORDER BY epl.salary DESC) as next_salary,
    LAG(epl.budget, 1) OVER (PARTITION BY epl.p_department ORDER BY epl.budget DESC) as prev_budget,
    LEAD(epl.budget, 1) OVER (PARTITION BY epl.p_department ORDER BY epl.budget DESC) as next_budget,
    da.dept_high_salary_sum,
    pa.dept_high_budget_sum,
    da.dept_high_salary_count,
    pa.dept_high_budget_count
FROM emp_proj_large epl
JOIN dept_agg da ON epl.department = da.department
JOIN cat_agg ca ON epl.category = ca.category
JOIN proj_agg pa ON epl.p_department = pa.department
ORDER BY epl.salary DESC, epl.budget DESC, epl.amount DESC

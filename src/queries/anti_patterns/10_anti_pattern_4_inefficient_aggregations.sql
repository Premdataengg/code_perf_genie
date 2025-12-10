-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Precompute aggregations, prune columns, and reduce redundant window functions
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
    SELECT employee_id, amount, transaction_id, department as l_department FROM large_dataset WHERE amount > 5000
),
base_join AS (
    SELECT 
        e.id as employee_id,
        e.name as employee_name,
        e.department as employee_dept,
        e.salary as employee_salary,
        d.dept_name,
        d.category as dept_category,
        d.manager as dept_manager,
        p.project_id,
        p.project_name,
        p.budget as project_budget,
        p.department as p_department,
        l.amount as transaction_amount,
        l.transaction_id,
        l.l_department
    FROM filtered_employees e
    JOIN filtered_departments d ON e.department = d.dept_name
    JOIN filtered_projects p ON e.department = p.department
    JOIN filtered_large_dataset l ON e.id = l.employee_id
)
SELECT 
    employee_id,
    employee_name,
    employee_dept,
    employee_salary,
    dept_name,
    dept_category,
    dept_manager,
    project_id,
    project_name,
    project_budget,
    transaction_amount,
    transaction_id,
    ROW_NUMBER() OVER (ORDER BY employee_salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY employee_dept ORDER BY employee_salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY dept_category ORDER BY employee_salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY p_department ORDER BY project_budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY l_department ORDER BY transaction_amount DESC) as transaction_amount_rank,
    AVG(employee_salary) OVER (PARTITION BY employee_dept) as dept_avg_salary,
    AVG(employee_salary) OVER (PARTITION BY dept_category) as category_avg_salary,
    AVG(project_budget) OVER (PARTITION BY p_department) as dept_avg_budget,
    AVG(transaction_amount) OVER (PARTITION BY l_department) as dept_avg_transaction,
    SUM(employee_salary + project_budget + transaction_amount) OVER (PARTITION BY employee_dept) as dept_total_value,
    SUM(employee_salary + project_budget + transaction_amount) OVER (PARTITION BY dept_category) as category_total_value,
    DENSE_RANK() OVER (ORDER BY employee_salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY employee_dept ORDER BY employee_salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY dept_category ORDER BY employee_salary DESC) as category_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY employee_salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY employee_dept ORDER BY employee_salary) as dept_salary_percentile,
    LAG(employee_salary, 1) OVER (ORDER BY employee_salary DESC) as prev_salary,
    LEAD(employee_salary, 1) OVER (ORDER BY employee_salary DESC) as next_salary,
    LAG(project_budget, 1) OVER (PARTITION BY p_department ORDER BY project_budget DESC) as prev_budget,
    LEAD(project_budget, 1) OVER (PARTITION BY p_department ORDER BY project_budget DESC) as next_budget,
    SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) OVER (PARTITION BY employee_dept) as dept_high_salary_sum,
    SUM(CASE WHEN project_budget > 80000 THEN project_budget ELSE 0 END) OVER (PARTITION BY p_department) as dept_high_budget_sum,
    COUNT(CASE WHEN employee_salary > 70000 THEN 1 END) OVER (PARTITION BY employee_dept) as dept_high_salary_count,
    COUNT(CASE WHEN project_budget > 80000 THEN 1 END) OVER (PARTITION BY p_department) as dept_high_budget_count
FROM base_join
ORDER BY employee_salary DESC, project_budget DESC, transaction_amount DESC

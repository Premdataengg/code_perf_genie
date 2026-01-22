-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Reduce window function passes, pre-aggregate where possible, and prune columns
WITH filtered_employees AS (
    SELECT * FROM employees WHERE salary > 50000
),
filtered_projects AS (
    SELECT * FROM projects WHERE budget > 30000
),
filtered_large_dataset AS (
    SELECT * FROM large_dataset WHERE amount > 5000
),
filtered_departments AS (
    SELECT * FROM departments WHERE category IN ('Tech', 'Business')
),
emp_proj AS (
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
        p.department as project_department,
        l.amount as transaction_amount,
        l.transaction_id,
        l.department as transaction_department
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
    -- Only essential window functions
    ROW_NUMBER() OVER (ORDER BY employee_salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY employee_dept ORDER BY employee_salary DESC) as dept_salary_rank,
    DENSE_RANK() OVER (ORDER BY employee_salary DESC) as salary_dense_rank,
    -- Pre-aggregated department salary stats
    AVG(employee_salary) OVER (PARTITION BY employee_dept) as dept_avg_salary,
    SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) OVER (PARTITION BY employee_dept) as dept_high_salary_sum,
    COUNT(CASE WHEN employee_salary > 70000 THEN 1 END) OVER (PARTITION BY employee_dept) as dept_high_salary_count,
    -- Only one lag/lead for demonstration
    LAG(employee_salary, 1) OVER (ORDER BY employee_salary DESC) as prev_salary
FROM emp_proj
ORDER BY employee_salary DESC, project_budget DESC, transaction_amount DESC

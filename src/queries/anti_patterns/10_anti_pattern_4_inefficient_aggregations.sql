-- ANTI-PATTERN 4: Inefficient Aggregation Strategy (Optimized)
-- Optimization: Precompute aggregations, prune columns, and reduce window function complexity
WITH filtered_employees AS (
    SELECT id, name, department, salary FROM employees WHERE salary > 50000
),
filtered_departments AS (
    SELECT dept_name, category, manager FROM departments WHERE category IN ('Tech', 'Business')
),
filtered_projects AS (
    SELECT project_id, project_name, budget, department FROM projects WHERE budget > 30000
),
filtered_large_dataset AS (
    SELECT amount, transaction_id, employee_id, department FROM large_dataset WHERE amount > 5000
),
joined_data AS (
    SELECT 
        e.id as employee_id,
        e.name as employee_name,
        e.department as employee_dept,
        e.salary as employee_salary,
        d.dept_name,
        d.category,
        d.manager,
        p.project_id,
        p.project_name,
        p.budget,
        l.amount,
        l.transaction_id,
        e.salary as base_salary,
        p.budget as base_budget,
        l.amount as base_amount
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
    category as dept_category,
    manager as dept_manager,
    project_id,
    project_name,
    budget as project_budget,
    amount as transaction_amount,
    transaction_id,
    ROW_NUMBER() OVER (ORDER BY employee_salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY employee_dept ORDER BY employee_salary DESC) as dept_salary_rank,
    AVG(employee_salary) OVER (PARTITION BY employee_dept) as dept_avg_salary,
    AVG(employee_salary) OVER (PARTITION BY category) as category_avg_salary,
    AVG(budget) OVER (PARTITION BY employee_dept) as dept_avg_budget,
    AVG(amount) OVER (PARTITION BY employee_dept) as dept_avg_transaction,
    SUM(employee_salary + budget + amount) OVER (PARTITION BY employee_dept) as dept_total_value,
    DENSE_RANK() OVER (ORDER BY employee_salary DESC) as salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY employee_salary) as salary_percentile,
    LAG(employee_salary, 1) OVER (ORDER BY employee_salary DESC) as prev_salary,
    LEAD(employee_salary, 1) OVER (ORDER BY employee_salary DESC) as next_salary,
    SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) OVER (PARTITION BY employee_dept) as dept_high_salary_sum,
    COUNT(CASE WHEN employee_salary > 70000 THEN 1 END) OVER (PARTITION BY employee_dept) as dept_high_salary_count
FROM joined_data
ORDER BY employee_salary DESC, project_budget DESC, transaction_amount DESC

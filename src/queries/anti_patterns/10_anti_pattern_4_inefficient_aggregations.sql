-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- This query violates the best practice of efficient aggregations
-- Problem: Multiple passes over data, complex window functions, and inefficient grouping
WITH base AS (
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
        e.salary,
        p.budget,
        l.amount,
        e.department,
        d.category,
        p.department as p_department,
        l.department as l_department
    FROM employees e
    JOIN departments d ON e.department = d.dept_name
    JOIN projects p ON e.department = p.department
    JOIN large_dataset l ON e.id = l.employee_id
    WHERE e.salary > 50000
        AND p.budget > 30000
        AND l.amount > 5000
        AND d.category IN ('Tech', 'Business')
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
    ROW_NUMBER() OVER (ORDER BY salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY employee_dept ORDER BY salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY dept_category ORDER BY salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY p_department ORDER BY project_budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY l_department ORDER BY transaction_amount DESC) as transaction_amount_rank,
    AVG(salary) OVER (PARTITION BY employee_dept) as dept_avg_salary,
    AVG(salary) OVER (PARTITION BY dept_category) as category_avg_salary,
    AVG(project_budget) OVER (PARTITION BY p_department) as dept_avg_budget,
    AVG(transaction_amount) OVER (PARTITION BY l_department) as dept_avg_transaction,
    SUM(salary + project_budget + transaction_amount) OVER (PARTITION BY employee_dept) as dept_total_value,
    SUM(salary + project_budget + transaction_amount) OVER (PARTITION BY dept_category) as category_total_value,
    DENSE_RANK() OVER (ORDER BY salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY employee_dept ORDER BY salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY dept_category ORDER BY salary DESC) as category_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY employee_dept ORDER BY salary) as dept_salary_percentile,
    LAG(salary, 1) OVER (ORDER BY salary DESC) as prev_salary,
    LEAD(salary, 1) OVER (ORDER BY salary DESC) as next_salary,
    LAG(project_budget, 1) OVER (PARTITION BY p_department ORDER BY project_budget DESC) as prev_budget,
    LEAD(project_budget, 1) OVER (PARTITION BY p_department ORDER BY project_budget DESC) as next_budget,
    SUM(CASE WHEN salary > 70000 THEN salary ELSE 0 END) OVER (PARTITION BY employee_dept) as dept_high_salary_sum,
    SUM(CASE WHEN project_budget > 80000 THEN project_budget ELSE 0 END) OVER (PARTITION BY p_department) as dept_high_budget_sum,
    COUNT(CASE WHEN salary > 70000 THEN 1 END) OVER (PARTITION BY employee_dept) as dept_high_salary_count,
    COUNT(CASE WHEN project_budget > 80000 THEN 1 END) OVER (PARTITION BY p_department) as dept_high_budget_count
FROM base
ORDER BY employee_salary DESC, project_budget DESC, transaction_amount DESC
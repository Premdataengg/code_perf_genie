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
        l.transaction_id
    FROM employees e
    JOIN departments d ON e.department = d.dept_name
    JOIN projects p ON e.department = p.department
    JOIN large_dataset l ON e.id = l.employee_id
    WHERE e.salary > 50000
        AND p.budget > 30000
        AND l.amount > 5000
        AND d.category IN ('Tech', 'Business')
),
department_agg AS (
    SELECT 
        employee_dept,
        AVG(employee_salary) as dept_avg_salary,
        SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) as dept_high_salary_sum,
        COUNT(CASE WHEN employee_salary > 70000 THEN 1 END) as dept_high_salary_count,
        SUM(employee_salary) as dept_total_salary
    FROM base
    GROUP BY employee_dept
),
category_agg AS (
    SELECT 
        dept_category,
        AVG(employee_salary) as category_avg_salary,
        SUM(employee_salary) as category_total_salary
    FROM base
    GROUP BY dept_category
),
project_agg AS (
    SELECT 
        employee_dept as project_department,
        AVG(project_budget) as dept_avg_budget,
        SUM(CASE WHEN project_budget > 80000 THEN project_budget ELSE 0 END) as dept_high_budget_sum,
        COUNT(CASE WHEN project_budget > 80000 THEN 1 END) as dept_high_budget_count,
        SUM(project_budget) as dept_total_budget
    FROM base
    GROUP BY employee_dept
),
transaction_agg AS (
    SELECT 
        employee_dept as transaction_department,
        AVG(transaction_amount) as dept_avg_transaction,
        SUM(transaction_amount) as dept_total_transaction
    FROM base
    GROUP BY employee_dept
)
SELECT 
    b.employee_id,
    b.employee_name,
    b.employee_dept,
    b.employee_salary,
    b.dept_name,
    b.dept_category,
    b.dept_manager,
    b.project_id,
    b.project_name,
    b.project_budget,
    b.transaction_amount,
    b.transaction_id,
    ROW_NUMBER() OVER (ORDER BY b.employee_salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY b.dept_category ORDER BY b.employee_salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY b.employee_dept ORDER BY b.project_budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY b.employee_dept ORDER BY b.transaction_amount DESC) as transaction_amount_rank,
    da.dept_avg_salary,
    ca.category_avg_salary,
    pa.dept_avg_budget,
    ta.dept_avg_transaction,
    (b.employee_salary + b.project_budget + b.transaction_amount) as total_value,
    da.dept_high_salary_sum,
    pa.dept_high_budget_sum,
    da.dept_high_salary_count,
    pa.dept_high_budget_count,
    DENSE_RANK() OVER (ORDER BY b.employee_salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY b.dept_category ORDER BY b.employee_salary DESC) as category_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY b.employee_salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary) as dept_salary_percentile,
    LAG(b.employee_salary, 1) OVER (ORDER BY b.employee_salary DESC) as prev_salary,
    LEAD(b.employee_salary, 1) OVER (ORDER BY b.employee_salary DESC) as next_salary,
    LAG(b.project_budget, 1) OVER (PARTITION BY b.employee_dept ORDER BY b.project_budget DESC) as prev_budget,
    LEAD(b.project_budget, 1) OVER (PARTITION BY b.employee_dept ORDER BY b.project_budget DESC) as next_budget
FROM base b
LEFT JOIN department_agg da ON b.employee_dept = da.employee_dept
LEFT JOIN category_agg ca ON b.dept_category = ca.dept_category
LEFT JOIN project_agg pa ON b.employee_dept = pa.project_department
LEFT JOIN transaction_agg ta ON b.employee_dept = ta.transaction_department
ORDER BY b.employee_salary DESC, b.project_budget DESC, b.transaction_amount DESC
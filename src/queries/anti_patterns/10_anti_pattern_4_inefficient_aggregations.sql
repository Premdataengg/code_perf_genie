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
department_aggs AS (
    SELECT 
        employee_dept,
        AVG(employee_salary) as dept_avg_salary,
        SUM(employee_salary + project_budget + transaction_amount) as dept_total_value,
        COUNT(CASE WHEN employee_salary > 70000 THEN 1 END) as dept_high_salary_count,
        SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) as dept_high_salary_sum
    FROM base
    GROUP BY employee_dept
),
category_aggs AS (
    SELECT 
        dept_category,
        AVG(employee_salary) as category_avg_salary,
        SUM(employee_salary + project_budget + transaction_amount) as category_total_value
    FROM base
    GROUP BY dept_category
)
SELECT 
    b.*,
    ROW_NUMBER() OVER (ORDER BY b.employee_salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY b.dept_category ORDER BY b.employee_salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY b.employee_dept ORDER BY b.project_budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY b.employee_dept ORDER BY b.transaction_amount DESC) as transaction_amount_rank,
    d.dept_avg_salary,
    d.dept_total_value,
    d.dept_high_salary_count,
    d.dept_high_salary_sum,
    c.category_avg_salary,
    c.category_total_value
FROM base b
LEFT JOIN department_aggs d ON b.employee_dept = d.employee_dept
LEFT JOIN category_aggs c ON b.dept_category = c.dept_category
ORDER BY b.employee_salary DESC, b.project_budget DESC, b.transaction_amount DESC
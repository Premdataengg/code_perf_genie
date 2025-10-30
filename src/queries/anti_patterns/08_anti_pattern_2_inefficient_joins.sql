-- ANTI-PATTERN 2: Inefficient Join Strategy
-- This query violates the best practice of join optimization
-- Problem: Multiple large table joins without proper join order or broadcast hints
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
    p.start_date,
    p.end_date,
    l.amount as transaction_amount,
    l.transaction_id,
    l.date as transaction_date,
    (e.salary + p.budget + l.amount) as total_value,
    CASE 
        WHEN e.salary > p.budget THEN 'High Salary'
        WHEN p.budget > l.amount THEN 'High Budget'
        ELSE 'High Transaction'
    END as value_category
FROM (
    SELECT * FROM employees WHERE salary > 60000
) e
JOIN /*+ BROADCAST(d) */ (
    SELECT * FROM departments WHERE category IN ('Tech', 'Business')
) d ON e.department = d.dept_name
JOIN (
    SELECT * FROM projects WHERE budget > 50000 AND start_date >= '2024-01-01' AND end_date <= '2024-12-31'
) p ON e.department = p.department
JOIN (
    SELECT * FROM large_dataset WHERE amount > 10000
) l ON e.id = l.employee_id
ORDER BY total_value DESC, employee_name

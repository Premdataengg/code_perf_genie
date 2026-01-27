-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Reduce redundant window functions, precompute aggregates, minimize window passes
WITH dept_agg AS (
    SELECT 
        department,
        AVG(salary) as dept_avg_salary,
        SUM(CASE WHEN salary > 70000 THEN salary ELSE 0 END) as dept_high_salary_sum,
        COUNT(CASE WHEN salary > 70000 THEN 1 END) as dept_high_salary_count
    FROM employees
    GROUP BY department
),
category_agg AS (
    SELECT 
        d.category,
        AVG(e.salary) as category_avg_salary
    FROM employees e
    JOIN departments d ON e.department = d.dept_name
    GROUP BY d.category
),
dept_project_agg AS (
    SELECT 
        department,
        AVG(budget) as dept_avg_budget,
        SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) as dept_high_budget_sum,
        COUNT(CASE WHEN budget > 80000 THEN 1 END) as dept_high_budget_count
    FROM projects
    GROUP BY department
),
dept_transaction_agg AS (
    SELECT 
        department,
        AVG(amount) as dept_avg_transaction
    FROM large_dataset
    GROUP BY department
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
    -- Window functions: compute only once per partition
    ROW_NUMBER() OVER (ORDER BY e.salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY d.category ORDER BY e.salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY p.department ORDER BY p.budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY l.department ORDER BY l.amount DESC) as transaction_amount_rank,
    -- Precomputed aggregations
    da.dept_avg_salary,
    ca.category_avg_salary,
    dpa.dept_avg_budget,
    dta.dept_avg_transaction,
    -- Total value per department/category
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY e.department) as dept_total_value,
    SUM(e.salary + p.budget + l.amount) OVER (PARTITION BY d.category) as category_total_value,
    -- Ranking functions
    DENSE_RANK() OVER (ORDER BY e.salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY d.category ORDER BY e.salary DESC) as category_salary_dense_rank,
    -- Percentile calculations
    PERCENT_RANK() OVER (ORDER BY e.salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY e.department ORDER BY e.salary) as dept_salary_percentile,
    -- Lag and lead functions (kept as is, but only once per partition)
    LAG(e.salary, 1) OVER (ORDER BY e.salary DESC) as prev_salary,
    LEAD(e.salary, 1) OVER (ORDER BY e.salary DESC) as next_salary,
    LAG(p.budget, 1) OVER (PARTITION BY p.department ORDER BY p.budget DESC) as prev_budget,
    LEAD(p.budget, 1) OVER (PARTITION BY p.department ORDER BY p.budget DESC) as next_budget,
    -- Precomputed conditional aggregations
    da.dept_high_salary_sum,
    dpa.dept_high_budget_sum,
    da.dept_high_salary_count,
    dpa.dept_high_budget_count
FROM employees e
JOIN departments d ON e.department = d.dept_name
JOIN projects p ON e.department = p.department
JOIN large_dataset l ON e.id = l.employee_id
LEFT JOIN dept_agg da ON e.department = da.department
LEFT JOIN category_agg ca ON d.category = ca.category
LEFT JOIN dept_project_agg dpa ON p.department = dpa.department
LEFT JOIN dept_transaction_agg dta ON l.department = dta.department
WHERE e.salary > 50000
    AND p.budget > 30000
    AND l.amount > 5000
    AND d.category IN ('Tech', 'Business')
ORDER BY e.salary DESC, p.budget DESC, l.amount DESC

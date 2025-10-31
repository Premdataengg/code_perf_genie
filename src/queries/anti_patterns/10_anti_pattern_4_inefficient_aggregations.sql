-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Reduce redundant window functions and aggregate calculations
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
base AS (
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
),
dept_salary_aggs AS (
    SELECT 
        employee_dept,
        AVG(employee_salary) as dept_avg_salary,
        SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) as dept_high_salary_sum,
        COUNT(CASE WHEN employee_salary > 70000 THEN 1 END) as dept_high_salary_count,
        SUM(employee_salary) as dept_salary_sum
    FROM base
    GROUP BY employee_dept
),
category_salary_aggs AS (
    SELECT 
        dept_category,
        AVG(employee_salary) as category_avg_salary,
        SUM(employee_salary) as category_salary_sum
    FROM base
    GROUP BY dept_category
),
dept_budget_aggs AS (
    SELECT 
        project_department,
        AVG(project_budget) as dept_avg_budget,
        SUM(CASE WHEN project_budget > 80000 THEN project_budget ELSE 0 END) as dept_high_budget_sum,
        COUNT(CASE WHEN project_budget > 80000 THEN 1 END) as dept_high_budget_count,
        SUM(project_budget) as dept_budget_sum
    FROM base
    GROUP BY project_department
),
dept_transaction_aggs AS (
    SELECT 
        transaction_department,
        AVG(transaction_amount) as dept_avg_transaction,
        SUM(transaction_amount) as dept_transaction_sum
    FROM base
    GROUP BY transaction_department
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
    -- Window functions (keep only necessary and non-redundant)
    ROW_NUMBER() OVER (ORDER BY b.employee_salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY b.dept_category ORDER BY b.employee_salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY b.project_department ORDER BY b.project_budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY b.transaction_department ORDER BY b.transaction_amount DESC) as transaction_amount_rank,
    -- Aggregations joined from precomputed tables
    dsa.dept_avg_salary,
    csa.category_avg_salary,
    dba.dept_avg_budget,
    dta.dept_avg_transaction,
    -- Precomputed total values
    (dsa.dept_salary_sum + dba.dept_budget_sum + dta.dept_transaction_sum) as dept_total_value,
    (csa.category_salary_sum) as category_total_value,
    -- Ranking functions (keep only one type)
    DENSE_RANK() OVER (ORDER BY b.employee_salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY b.dept_category ORDER BY b.employee_salary DESC) as category_salary_dense_rank,
    -- Percentile calculations (keep only global and department)
    PERCENT_RANK() OVER (ORDER BY b.employee_salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary) as dept_salary_percentile,
    -- Lag and lead functions (keep only for salary)
    LAG(b.employee_salary, 1) OVER (ORDER BY b.employee_salary DESC) as prev_salary,
    LEAD(b.employee_salary, 1) OVER (ORDER BY b.employee_salary DESC) as next_salary,
    -- Precomputed conditional aggregations
    dsa.dept_high_salary_sum,
    dba.dept_high_budget_sum,
    dsa.dept_high_salary_count,
    dba.dept_high_budget_count
FROM base b
LEFT JOIN dept_salary_aggs dsa ON b.employee_dept = dsa.employee_dept
LEFT JOIN category_salary_aggs csa ON b.dept_category = csa.dept_category
LEFT JOIN dept_budget_aggs dba ON b.project_department = dba.project_department
LEFT JOIN dept_transaction_aggs dta ON b.transaction_department = dta.transaction_department
ORDER BY b.employee_salary DESC, b.project_budget DESC, b.transaction_amount DESC
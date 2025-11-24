-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Precompute aggregations, reduce redundant window functions, and use CTEs for shared results
WITH filtered_employees AS (
    SELECT id, name, department, salary
    FROM employees
    WHERE salary > 50000
),
filtered_projects AS (
    SELECT project_id, project_name, budget, department
    FROM projects
    WHERE budget > 30000
),
filtered_large_dataset AS (
    SELECT employee_id, amount, transaction_id, department
    FROM large_dataset
    WHERE amount > 5000
),
joined_data AS (
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
    JOIN departments d ON e.department = d.dept_name AND d.category IN ('Tech', 'Business')
    JOIN filtered_projects p ON e.department = p.department
    JOIN filtered_large_dataset l ON e.id = l.employee_id
),
precomputed_aggs AS (
    SELECT 
        employee_dept,
        dept_category,
        project_department,
        transaction_department,
        AVG(employee_salary) as dept_avg_salary,
        AVG(employee_salary) OVER (PARTITION BY dept_category) as category_avg_salary,
        AVG(project_budget) as dept_avg_budget,
        AVG(transaction_amount) as dept_avg_transaction
    FROM joined_data
    GROUP BY employee_dept, dept_category, project_department, transaction_department
)
SELECT 
    jd.employee_id,
    jd.employee_name,
    jd.employee_dept,
    jd.employee_salary,
    jd.dept_name,
    jd.dept_category,
    jd.dept_manager,
    jd.project_id,
    jd.project_name,
    jd.project_budget,
    jd.transaction_amount,
    jd.transaction_id,
    -- Window functions (minimized redundancy)
    ROW_NUMBER() OVER (ORDER BY jd.employee_salary DESC) as salary_rank,
    ROW_NUMBER() OVER (PARTITION BY jd.employee_dept ORDER BY jd.employee_salary DESC) as dept_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY jd.dept_category ORDER BY jd.employee_salary DESC) as category_salary_rank,
    ROW_NUMBER() OVER (PARTITION BY jd.project_department ORDER BY jd.project_budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY jd.transaction_department ORDER BY jd.transaction_amount DESC) as transaction_amount_rank,
    -- Use precomputed aggregations
    pa.dept_avg_salary,
    pa.category_avg_salary,
    pa.dept_avg_budget,
    pa.dept_avg_transaction,
    -- Complex calculations in window functions (minimized)
    SUM(jd.employee_salary + jd.project_budget + jd.transaction_amount) OVER (PARTITION BY jd.employee_dept) as dept_total_value,
    SUM(jd.employee_salary + jd.project_budget + jd.transaction_amount) OVER (PARTITION BY jd.dept_category) as category_total_value,
    -- Ranking functions
    DENSE_RANK() OVER (ORDER BY jd.employee_salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY jd.employee_dept ORDER BY jd.employee_salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY jd.dept_category ORDER BY jd.employee_salary DESC) as category_salary_dense_rank,
    -- Percentile calculations
    PERCENT_RANK() OVER (ORDER BY jd.employee_salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY jd.employee_dept ORDER BY jd.employee_salary) as dept_salary_percentile,
    -- Lag and lead functions (kept as is for correctness)
    LAG(jd.employee_salary, 1) OVER (ORDER BY jd.employee_salary DESC) as prev_salary,
    LEAD(jd.employee_salary, 1) OVER (ORDER BY jd.employee_salary DESC) as next_salary,
    LAG(jd.project_budget, 1) OVER (PARTITION BY jd.project_department ORDER BY jd.project_budget DESC) as prev_budget,
    LEAD(jd.project_budget, 1) OVER (PARTITION BY jd.project_department ORDER BY jd.project_budget DESC) as next_budget,
    -- Conditional aggregations
    SUM(CASE WHEN jd.employee_salary > 70000 THEN jd.employee_salary ELSE 0 END) OVER (PARTITION BY jd.employee_dept) as dept_high_salary_sum,
    SUM(CASE WHEN jd.project_budget > 80000 THEN jd.project_budget ELSE 0 END) OVER (PARTITION BY jd.project_department) as dept_high_budget_sum,
    COUNT(CASE WHEN jd.employee_salary > 70000 THEN 1 END) OVER (PARTITION BY jd.employee_dept) as dept_high_salary_count,
    COUNT(CASE WHEN jd.project_budget > 80000 THEN 1 END) OVER (PARTITION BY jd.project_department) as dept_high_budget_count
FROM joined_data jd
JOIN precomputed_aggs pa ON jd.employee_dept = pa.employee_dept AND jd.dept_category = pa.dept_category AND jd.project_department = pa.project_department AND jd.transaction_department = pa.transaction_department
ORDER BY jd.employee_salary DESC, jd.project_budget DESC, jd.transaction_amount DESC
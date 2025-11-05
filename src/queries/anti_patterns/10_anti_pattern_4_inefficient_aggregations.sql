-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Precompute aggregations, prune columns, and reduce redundant window functions
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
base_join AS (
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
agg_dept AS (
    SELECT 
        employee_dept,
        AVG(employee_salary) as dept_avg_salary,
        SUM(employee_salary + project_budget + transaction_amount) as dept_total_value,
        COUNT(*) as dept_count,
        SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) as dept_high_salary_sum,
        COUNT(CASE WHEN employee_salary > 70000 THEN 1 END) as dept_high_salary_count
    FROM base_join
    GROUP BY employee_dept
),
agg_category AS (
    SELECT 
        dept_category,
        AVG(employee_salary) as category_avg_salary,
        SUM(employee_salary + project_budget + transaction_amount) as category_total_value
    FROM base_join
    GROUP BY dept_category
),
agg_project AS (
    SELECT 
        project_department,
        AVG(project_budget) as dept_avg_budget,
        SUM(CASE WHEN project_budget > 80000 THEN project_budget ELSE 0 END) as dept_high_budget_sum,
        COUNT(CASE WHEN project_budget > 80000 THEN 1 END) as dept_high_budget_count
    FROM base_join
    GROUP BY project_department
),
agg_transaction AS (
    SELECT 
        transaction_department,
        AVG(transaction_amount) as dept_avg_transaction
    FROM base_join
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
    DENSE_RANK() OVER (ORDER BY b.employee_salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary DESC) as dept_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY b.employee_salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary) as dept_salary_percentile,
    LAG(b.employee_salary, 1) OVER (ORDER BY b.employee_salary DESC) as prev_salary,
    LEAD(b.employee_salary, 1) OVER (ORDER BY b.employee_salary DESC) as next_salary,
    LAG(b.project_budget, 1) OVER (PARTITION BY b.project_department ORDER BY b.project_budget DESC) as prev_budget,
    LEAD(b.project_budget, 1) OVER (PARTITION BY b.project_department ORDER BY b.project_budget DESC) as next_budget,
    -- Aggregations joined from precomputed CTEs
    ad.dept_avg_salary,
    ac.category_avg_salary,
    ap.dept_avg_budget,
    at.dept_avg_transaction,
    ad.dept_total_value,
    ac.category_total_value,
    ad.dept_high_salary_sum,
    ap.dept_high_budget_sum,
    ad.dept_high_salary_count,
    ap.dept_high_budget_count
FROM base_join b
LEFT JOIN agg_dept ad ON b.employee_dept = ad.employee_dept
LEFT JOIN agg_category ac ON b.dept_category = ac.dept_category
LEFT JOIN agg_project ap ON b.project_department = ap.project_department
LEFT JOIN agg_transaction at ON b.transaction_department = at.transaction_department
ORDER BY b.employee_salary DESC, b.project_budget DESC, b.transaction_amount DESC
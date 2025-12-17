-- ANTI-PATTERN 4: Inefficient Aggregation Strategy
-- Optimized: Precompute aggregations, prune columns, and minimize redundant window functions
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
precomputed_aggs AS (
    SELECT 
        employee_dept,
        dept_category,
        project_department,
        transaction_department,
        AVG(employee_salary) as dept_avg_salary,
        AVG(employee_salary) as category_avg_salary,
        AVG(project_budget) as dept_avg_budget,
        AVG(transaction_amount) as dept_avg_transaction,
        SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) as dept_high_salary_sum,
        SUM(CASE WHEN project_budget > 80000 THEN project_budget ELSE 0 END) as dept_high_budget_sum,
        COUNT(CASE WHEN employee_salary > 70000 THEN 1 END) as dept_high_salary_count,
        COUNT(CASE WHEN project_budget > 80000 THEN 1 END) as dept_high_budget_count
    FROM base_join
    GROUP BY employee_dept, dept_category, project_department, transaction_department
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
    ROW_NUMBER() OVER (PARTITION BY b.project_department ORDER BY b.project_budget DESC) as project_budget_rank,
    ROW_NUMBER() OVER (PARTITION BY b.transaction_department ORDER BY b.transaction_amount DESC) as transaction_amount_rank,
    a.dept_avg_salary,
    a.category_avg_salary,
    a.dept_avg_budget,
    a.dept_avg_transaction,
    (b.employee_salary + b.project_budget + b.transaction_amount) as total_value,
    DENSE_RANK() OVER (ORDER BY b.employee_salary DESC) as salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary DESC) as dept_salary_dense_rank,
    DENSE_RANK() OVER (PARTITION BY b.dept_category ORDER BY b.employee_salary DESC) as category_salary_dense_rank,
    PERCENT_RANK() OVER (ORDER BY b.employee_salary) as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY b.employee_dept ORDER BY b.employee_salary) as dept_salary_percentile,
    LAG(b.employee_salary, 1) OVER (ORDER BY b.employee_salary DESC) as prev_salary,
    LEAD(b.employee_salary, 1) OVER (ORDER BY b.employee_salary DESC) as next_salary,
    LAG(b.project_budget, 1) OVER (PARTITION BY b.project_department ORDER BY b.project_budget DESC) as prev_budget,
    LEAD(b.project_budget, 1) OVER (PARTITION BY b.project_department ORDER BY b.project_budget DESC) as next_budget,
    a.dept_high_salary_sum,
    a.dept_high_budget_sum,
    a.dept_high_salary_count,
    a.dept_high_budget_count
FROM base_join b
JOIN precomputed_aggs a
  ON b.employee_dept = a.employee_dept
 AND b.dept_category = a.dept_category
 AND b.project_department = a.project_department
 AND b.transaction_department = a.transaction_department
ORDER BY b.employee_salary DESC, b.project_budget DESC, b.transaction_amount DESC

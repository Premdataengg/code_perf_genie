-- ANTI-PATTERN 4: Inefficient Aggregation Strategy (Optimized)
-- Optimization: Precompute aggregations in CTEs, reuse window results, and prune columns
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
        d.dept_name,
        d.category,
        d.manager,
        p.project_id,
        p.project_name,
        p.budget,
        p.department as project_department,
        l.amount,
        l.transaction_id,
        l.department as transaction_department
    FROM filtered_employees e
    JOIN filtered_departments d ON e.department = d.dept_name
    JOIN filtered_projects p ON e.department = p.department
    JOIN filtered_large_dataset l ON e.id = l.employee_id
),
precomputed_aggs AS (
    SELECT 
        employee_id,
        employee_name,
        employee_dept,
        employee_salary,
        dept_name,
        category,
        manager,
        project_id,
        project_name,
        budget,
        amount,
        transaction_id,
        ROW_NUMBER() OVER (ORDER BY employee_salary DESC) as salary_rank,
        ROW_NUMBER() OVER (PARTITION BY employee_dept ORDER BY employee_salary DESC) as dept_salary_rank,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY employee_salary DESC) as category_salary_rank,
        ROW_NUMBER() OVER (PARTITION BY project_department ORDER BY budget DESC) as project_budget_rank,
        ROW_NUMBER() OVER (PARTITION BY transaction_department ORDER BY amount DESC) as transaction_amount_rank,
        AVG(employee_salary) OVER (PARTITION BY employee_dept) as dept_avg_salary,
        AVG(employee_salary) OVER (PARTITION BY category) as category_avg_salary,
        AVG(budget) OVER (PARTITION BY project_department) as dept_avg_budget,
        AVG(amount) OVER (PARTITION BY transaction_department) as dept_avg_transaction,
        SUM(employee_salary + budget + amount) OVER (PARTITION BY employee_dept) as dept_total_value,
        SUM(employee_salary + budget + amount) OVER (PARTITION BY category) as category_total_value,
        DENSE_RANK() OVER (ORDER BY employee_salary DESC) as salary_dense_rank,
        DENSE_RANK() OVER (PARTITION BY employee_dept ORDER BY employee_salary DESC) as dept_salary_dense_rank,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY employee_salary DESC) as category_salary_dense_rank,
        PERCENT_RANK() OVER (ORDER BY employee_salary) as salary_percentile,
        PERCENT_RANK() OVER (PARTITION BY employee_dept ORDER BY employee_salary) as dept_salary_percentile,
        LAG(employee_salary, 1) OVER (ORDER BY employee_salary DESC) as prev_salary,
        LEAD(employee_salary, 1) OVER (ORDER BY employee_salary DESC) as next_salary,
        LAG(budget, 1) OVER (PARTITION BY project_department ORDER BY budget DESC) as prev_budget,
        LEAD(budget, 1) OVER (PARTITION BY project_department ORDER BY budget DESC) as next_budget,
        SUM(CASE WHEN employee_salary > 70000 THEN employee_salary ELSE 0 END) OVER (PARTITION BY employee_dept) as dept_high_salary_sum,
        SUM(CASE WHEN budget > 80000 THEN budget ELSE 0 END) OVER (PARTITION BY project_department) as dept_high_budget_sum,
        COUNT(CASE WHEN employee_salary > 70000 THEN 1 END) OVER (PARTITION BY employee_dept) as dept_high_salary_count,
        COUNT(CASE WHEN budget > 80000 THEN 1 END) OVER (PARTITION BY project_department) as dept_high_budget_count
    FROM base_join
)
SELECT * FROM precomputed_aggs
ORDER BY employee_salary DESC, budget DESC, amount DESC

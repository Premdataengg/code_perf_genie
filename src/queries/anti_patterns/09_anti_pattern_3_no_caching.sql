-- ANTI-PATTERN 3: No Caching Strategy
-- This query violates the best practice of caching frequently used data
-- Problem: Repeatedly computing expensive aggregations without caching intermediate results
WITH expensive_calc AS (
    SELECT 
        department,
        AVG(salary) as avg_salary,
        SUM(salary) as total_salary,
        COUNT(*) as emp_count,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary,
        STDDEV(salary) as salary_stddev,
        VARIANCE(salary) as salary_variance,
        PERCENTILE(salary, 0.25) as salary_q1,
        PERCENTILE(salary, 0.75) as salary_q3
    FROM employees
    GROUP BY department
),
department_stats AS (
    SELECT 
        d.dept_name,
        d.category,
        d.manager,
        e.avg_salary,
        e.total_salary,
        e.emp_count,
        e.max_salary,
        e.min_salary,
        e.salary_stddev,
        e.salary_variance,
        e.salary_q1,
        e.salary_q3,
        (e.max_salary - e.min_salary) as salary_range,
        (e.salary_q3 - e.salary_q1) as salary_iqr
    FROM departments d
    JOIN expensive_calc e ON d.dept_name = e.department
),
project_analysis AS (
    SELECT 
        p.department,
        AVG(p.budget) as avg_budget,
        SUM(p.budget) as total_budget,
        COUNT(*) as project_count,
        MAX(p.budget) as max_budget,
        MIN(p.budget) as min_budget
    FROM projects p
    GROUP BY p.department
)
SELECT 
    ds.dept_name,
    ds.category,
    ds.manager,
    ds.avg_salary,
    ds.total_salary,
    ds.emp_count,
    ds.max_salary,
    ds.min_salary,
    ds.salary_stddev,
    ds.salary_variance,
    ds.salary_range,
    ds.salary_iqr,
    pa.avg_budget,
    pa.total_budget,
    pa.project_count,
    pa.max_budget,
    pa.min_budget,
    (ds.avg_salary + pa.avg_budget) as combined_avg,
    (ds.total_salary + pa.total_budget) as combined_total,
    CASE 
        WHEN ds.avg_salary > pa.avg_budget THEN 'Salary Dominant'
        WHEN pa.avg_budget > ds.avg_salary THEN 'Budget Dominant'
        ELSE 'Balanced'
    END as resource_profile
FROM department_stats ds
JOIN project_analysis pa ON ds.dept_name = pa.department
ORDER BY combined_total DESC, dept_name
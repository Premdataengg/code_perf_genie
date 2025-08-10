-- Optimized: Remove CROSS JOINs, push down filters, use CTEs, and broadcast small tables
WITH filtered_employees AS (
    SELECT * FROM employees
    WHERE salary > 50000
      AND LENGTH(name) > 5
      AND department IN ('Engineering', 'Marketing', 'Sales')
),
filtered_departments AS (
    SELECT * FROM departments
    WHERE category IN ('Tech', 'Business')
)
SELECT 
    e1.id as emp1_id,
    e1.name as emp1_name,
    e1.department as emp1_dept,
    e1.salary as emp1_salary,
    e2.id as emp2_id,
    e2.name as emp2_name,
    e2.department as emp2_dept,
    e2.salary as emp2_salary,
    e3.id as emp3_id,
    e3.name as emp3_name,
    e3.department as emp3_dept,
    e3.salary as emp3_salary,
    d1.dept_name as dept1_name,
    d1.category as dept1_category,
    d1.manager as dept1_manager,
    d2.dept_name as dept2_name,
    d2.category as dept2_category,
    d2.manager as dept2_manager,
    d3.dept_name as dept3_name,
    d3.category as dept3_category,
    d3.manager as dept3_manager,
    (e1.salary + e2.salary + e3.salary) as total_salary,
    (e1.salary * e2.salary * e3.salary) as salary_product,
    CASE 
        WHEN e1.salary > e2.salary AND e1.salary > e3.salary THEN e1.name
        WHEN e2.salary > e1.salary AND e2.salary > e3.salary THEN e2.name
        ELSE e3.name
    END as highest_paid_employee,
    CASE 
        WHEN e1.department = e2.department AND e2.department = e3.department THEN 'Same Department'
        WHEN e1.department = e2.department OR e2.department = e3.department OR e1.department = e3.department THEN 'Partial Match'
        ELSE 'Different Departments'
    END as department_comparison,
    CONCAT(e1.name, ' | ', e2.name, ' | ', e3.name) as all_names,
    CONCAT(d1.manager, ' | ', d2.manager, ' | ', d3.manager) as all_managers
FROM filtered_employees e1
JOIN filtered_employees e2 ON e1.id != e2.id
JOIN filtered_employees e3 ON e1.id != e3.id AND e2.id != e3.id
JOIN /*+ BROADCAST(d1) */ filtered_departments d1 ON e1.department = d1.dept_name
JOIN /*+ BROADCAST(d2) */ filtered_departments d2 ON e2.department = d2.dept_name
JOIN /*+ BROADCAST(d3) */ filtered_departments d3 ON e3.department = d3.dept_name
WHERE (e1.salary + e2.salary + e3.salary) > 200000
  AND (e1.salary * e2.salary * e3.salary) > 1000000000000
ORDER BY 
    (e1.salary + e2.salary + e3.salary) DESC,
    (e1.salary * e2.salary * e3.salary) DESC,
    LENGTH(e1.name) + LENGTH(e2.name) + LENGTH(e3.name) DESC,
    e1.id + e2.id + e3.id DESC
LIMIT 100
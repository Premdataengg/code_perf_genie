-- Deliberately poorly performing query with multiple anti-patterns
-- OPTIMIZED: Use CTEs to avoid repeated subqueries, push down filters, and deduplicate employee sets before join
WITH filtered_employees AS (
    SELECT * FROM employees WHERE salary > 50000 AND LENGTH(name) > 5 AND department IN ('Engineering', 'Marketing', 'Sales')
),
emp_pairs AS (
    SELECT e1.*, e2.*
    FROM filtered_employees e1
    JOIN filtered_employees e2 ON e1.id < e2.id
),
emp_triplets AS (
    SELECT e1.id as emp1_id, e1.name as emp1_name, e1.department as emp1_dept, e1.salary as emp1_salary,
           e2.id as emp2_id, e2.name as emp2_name, e2.department as emp2_dept, e2.salary as emp2_salary,
           e3.id as emp3_id, e3.name as emp3_name, e3.department as emp3_dept, e3.salary as emp3_salary
    FROM emp_pairs ep
    JOIN filtered_employees e3 ON ep.id < e3.id AND ep.e2.id < e3.id AND ep.id != e3.id AND ep.e2.id != e3.id
    -- Ensures all ids are unique and ordered, avoids cartesian explosion
    JOIN employees e1 ON ep.id = e1.id
    JOIN employees e2 ON ep.e2.id = e2.id
)
SELECT 
    et.emp1_id,
    et.emp1_name,
    et.emp1_dept,
    et.emp1_salary,
    et.emp2_id,
    et.emp2_name,
    et.emp2_dept,
    et.emp2_salary,
    et.emp3_id,
    et.emp3_name,
    et.emp3_dept,
    et.emp3_salary,
    d1.dept_name as dept1_name,
    d1.category as dept1_category,
    d1.manager as dept1_manager,
    d2.dept_name as dept2_name,
    d2.category as dept2_category,
    d2.manager as dept2_manager,
    d3.dept_name as dept3_name,
    d3.category as dept3_category,
    d3.manager as dept3_manager,
    (et.emp1_salary + et.emp2_salary + et.emp3_salary) as total_salary,
    (et.emp1_salary * et.emp2_salary * et.emp3_salary) as salary_product,
    CASE 
        WHEN et.emp1_salary > et.emp2_salary AND et.emp1_salary > et.emp3_salary THEN et.emp1_name
        WHEN et.emp2_salary > et.emp1_salary AND et.emp2_salary > et.emp3_salary THEN et.emp2_name
        ELSE et.emp3_name
    END as highest_paid_employee,
    CASE 
        WHEN et.emp1_dept = et.emp2_dept AND et.emp2_dept = et.emp3_dept THEN 'Same Department'
        WHEN et.emp1_dept = et.emp2_dept OR et.emp2_dept = et.emp3_dept OR et.emp1_dept = et.emp3_dept THEN 'Partial Match'
        ELSE 'Different Departments'
    END as department_comparison,
    CONCAT(et.emp1_name, ' | ', et.emp2_name, ' | ', et.emp3_name) as all_names,
    CONCAT(d1.manager, ' | ', d2.manager, ' | ', d3.manager) as all_managers
FROM emp_triplets et
JOIN /*+ BROADCAST(d1) */ departments d1 ON et.emp1_dept = d1.dept_name AND d1.category IN ('Tech', 'Business')
JOIN /*+ BROADCAST(d2) */ departments d2 ON et.emp2_dept = d2.dept_name AND d2.category IN ('Tech', 'Business')
JOIN /*+ BROADCAST(d3) */ departments d3 ON et.emp3_dept = d3.dept_name AND d3.category IN ('Tech', 'Business')
WHERE (et.emp1_salary + et.emp2_salary + et.emp3_salary) > 200000
    AND (et.emp1_salary * et.emp2_salary * et.emp3_salary) > 1000000000000
ORDER BY 
    (et.emp1_salary + et.emp2_salary + et.emp3_salary) DESC,
    (et.emp1_salary * et.emp2_salary * et.emp3_salary) DESC,
    LENGTH(et.emp1_name) + LENGTH(et.emp2_name) + LENGTH(et.emp3_name) DESC,
    et.emp1_id + et.emp2_id + et.emp3_id DESC
LIMIT 100

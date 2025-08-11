-- JOIN operation between tables
SELECT e.name, e.salary, e.department, d.category, d.manager
FROM employees e
JOIN /*+ BROADCAST(d) */ (SELECT dept_name, category, manager FROM departments WHERE dept_name IS NOT NULL) d ON e.department = d.dept_name
ORDER BY e.salary DESC
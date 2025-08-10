-- Optimized: Use broadcast join for small departments table
SELECT e.name, e.salary, e.department, d.category, d.manager
FROM employees e
JOIN /*+ BROADCAST(d) */ departments d ON e.department = d.dept_name
ORDER BY e.salary DESC
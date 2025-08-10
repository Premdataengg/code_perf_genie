-- Optimized: Efficient aggregation and column pruning
SELECT department,
       COUNT(*) as employee_count,
       AVG(salary) as avg_salary,
       MAX(salary) as max_salary,
       MIN(salary) as min_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC
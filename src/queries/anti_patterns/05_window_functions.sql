-- Optimized: Window function with column pruning
SELECT name, department, salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num
FROM employees
ORDER BY department, salary DESC
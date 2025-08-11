-- Window functions for ranking
SELECT name, department, salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num
FROM employees
WHERE department IS NOT NULL
ORDER BY department, salary DESC
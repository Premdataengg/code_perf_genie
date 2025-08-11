-- Filtering with WHERE clause
SELECT name, salary 
FROM employees 
WHERE department = 'Engineering' 
  AND name IS NOT NULL
ORDER BY salary DESC
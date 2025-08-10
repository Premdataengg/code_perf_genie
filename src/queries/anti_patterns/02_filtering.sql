-- Optimized: Predicate pushdown and column pruning
SELECT name, salary
FROM employees
WHERE department = 'Engineering'
ORDER BY salary DESC
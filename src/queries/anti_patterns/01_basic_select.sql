-- Optimized: Select only required columns and avoid unnecessary ORDER BY for large tables
SELECT id, name, department, salary FROM employees
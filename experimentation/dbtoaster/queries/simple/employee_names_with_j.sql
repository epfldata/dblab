--  List all the employee details

CREATE STREAM EMPLOYEE(
    employee_id     INT, 
    last_name       VARCHAR(30),
    first_name      VARCHAR(20),
    middle_name     CHAR(1),
    job_id          INT,
    manager_id      INT,
    hire_date       DATE,
    salary          FLOAT,
    commission      FLOAT,
    department_id   INT
    ) 
  FROM FILE '../../experiments/data/employee/employee.dat' LINE DELIMITED
  CSV ();

SELECT COUNT(*) AS first_names_with_j 
FROM employee
WHERE first_name LIKE 'J%';

SELECT COUNT(*) AS last_names_with_j
FROM employee
WHERE last_name LIKE 'J%';


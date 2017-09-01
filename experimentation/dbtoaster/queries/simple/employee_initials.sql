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

SELECT substring('FOO', 1, 2);
SELECT SUBSTRING(first_name, 0, 1), 
       SUBSTRING(middle_name, 0, 1),
       SUBSTRING(last_name, 0, 1)
FROM employee;
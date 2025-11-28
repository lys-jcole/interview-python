### 1. Navigate to the SQL sandbox
https://www.db-fiddle.com/

### 2. Copy below code into the Schema SQL section
```bash
-- Create the table
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    department_id INT,
    salary DECIMAL(10, 2),
    hire_date DATE
);

CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(50) NOT NULL,
    location VARCHAR(100)
);
-- Insert Data
INSERT INTO employees (employee_id, first_name, last_name, age, department_id, salary, hire_date)  VALUES (1, 'Brian', 'Hood', 30, 1, 55000.00, '2015-06-20'),(2, 'Jane', 'Smith', 28, 2, 60000.00, '2016-03-15'),(3, 'Guido', 'Scorza', 35, 1, 75000.00, '2017-08-01'),(4, 'Michael', 'Brown', 40, 3, 15000.00, '2014-11-30'),(5, 'Chris', 'Davis', 45, 4, 85000.00, '2013-05-12'),(6, 'David', 'Miller', 25, 2, 50000.00, '2019-02-20'),(7, 'Jonathan', 'Zittrain', 38, 4, 90000.00, '2016-07-09'),(8, 'Jonathan', 'Turley', 50, 3, 70000.00, '2010-04-17'),(9, 'James', 'Taylor', 33, 1, 60000.00, '2018-09-25'),(10, 'Patricia', 'Anderson', 29, 4, 80000.00, '2017-11-02'), (11, 'John', 'Wayne', 49, 5, 90000.00, '2018-4-02');
INSERT INTO departments (department_id, department_name, location) VALUES  (1, 'Sales', 'New York'), (2, 'Marketing', 'Los Angeles'), (3, 'HR', 'Chicago'), (4, 'Engineering', 'San Francisco'),(6, 'Legal', 'Denver');
```
### 3. Begin writing your queries

#### Query 1: Display all employees
**Expected Output:**
```
employee_id | first_name | last_name | age | department_id | salary    | hire_date
------------|------------|-----------|-----|---------------|-----------|------------
1           | Brian      | Hood      | 30  | 1             | 55000.00  | 2015-06-20
2           | Jane       | Smith     | 28  | 2             | 60000.00  | 2016-03-15
3           | Guido      | Scorza    | 35  | 1             | 75000.00  | 2017-08-01
4           | Michael    | Brown     | 40  | 3             | 15000.00  | 2014-11-30
5           | Chris      | Davis     | 45  | 4             | 85000.00  | 2013-05-12
6           | David      | Miller    | 25  | 2             | 50000.00  | 2019-02-20
7           | Jonathan   | Zittrain  | 38  | 4             | 90000.00  | 2016-07-09
8           | Jonathan   | Turley    | 50  | 3             | 70000.00  | 2010-04-17
9           | James      | Taylor    | 33  | 1             | 60000.00  | 2018-09-25
10          | Patricia   | Anderson  | 29  | 4             | 80000.00  | 2017-11-02
11          | John       | Wayne     | 49  | 5             | 90000.00  | 2018-04-02
```

#### Query 2: Display employee full name, as one column, and their department location
**Expected Output:**
```
full_name          | location
-------------------|---------------
Brian Hood         | New York
Jane Smith         | Los Angeles
Guido Scorza       | New York
David Miller       | Los Angeles
James Taylor       | New York
Chris Davis        | San Francisco
Jonathan Zittrain  | San Francisco
Patricia Anderson  | San Francisco
Michael Brown      | Chicago
Jonathan Turley    | Chicago
```

#### Query 3: Display employees that do not have a valid department
**Expected Output:**
```
employee_id | first_name | last_name | age | department_id | salary    | hire_date
------------|------------|-----------|-----|---------------|-----------|------------
11          | John       | Wayne     | 49  | 5             | 90000.00  | 2018-04-02
```

#### Query 4: Display departments that do not have any employees
**Expected Output:**
```
department_id | department_name | location
--------------|-----------------|----------
6             | Legal           | Denver
```

#### Query 5: Display the average salary for each department in descending order
**Expected Output:**
```
department_name | avg_salary
----------------|-------------
Engineering     | 85000.00
Sales           | 63333.33
Marketing       | 55000.00
HR              | 42500.00
```

#### Query 6: Display the average salary for each department in descending order, only if the average is above $50k
**Expected Output:**
```
department_name | avg_salary
----------------|-------------
Engineering     | 85000.00
Sales           | 63333.33
Marketing       | 55000.00
```

#### Query 7: Display employees who earn more than the average salary of their department
**Expected Output:**
```
first_name | last_name | salary    | department_name
-----------|-----------|-----------|----------------
Guido      | Scorza    | 75000.00  | Sales
Jonathan   | Turley    | 70000.00  | HR
Jonathan   | Zittrain  | 90000.00  | Engineering
Jane       | Smith     | 60000.00  | Marketing
```

#### Query 8: Display the highest paid employee for every department
**Expected Output:**
```
first_name | last_name | salary    | department_name
-----------|-----------|-----------|----------------
Jonathan   | Zittrain  | 90000.00  | Engineering
Guido      | Scorza    | 75000.00  | Sales
Jonathan   | Turley    | 70000.00  | HR
Jane       | Smith     | 60000.00  | Marketing
```
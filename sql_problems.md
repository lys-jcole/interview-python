### 1. Navigate to the SQL sandbox
https://www.db-fiddle.com/

### 2. Copy below code into the Schema SQL section
```bash
-- Create the table
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Age INT,
    DepartmentID INT,
    Salary DECIMAL(10, 2),
    HireDate DATE
);

CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(50) NOT NULL,
    Location VARCHAR(100)
);
-- Insert Data
INSERT INTO Employees (EmployeeID, FirstName, LastName, Age, DepartmentID, Salary, HireDate)  VALUES (1, 'John', 'Doe', 30, 1, 55000.00, '2015-06-20'),(2, 'Jane', 'Smith', 28, 2, 60000.00, '2016-03-15'),(3, 'Emily', 'Jones', 35, 1, 75000.00, '2017-08-01'),(4, 'Michael', 'Brown', 40, 3, 15000.00, '2014-11-30'),(5, 'Chris', 'Davis', 45, 4, 85000.00, '2013-05-12'),(6, 'David', 'Miller', 25, 2, 50000.00, '2019-02-20'),(7, 'Sarah', 'Wilson', 38, 4, 90000.00, '2016-07-09'),(8, 'Linda', 'Moore', 50, 3, 70000.00, '2010-04-17'),(9, 'James', 'Taylor', 33, 1, 60000.00, '2018-09-25'),(10, 'Patricia', 'Anderson', 29, 4, 80000.00, '2017-11-02'), (11, 'John', 'Wayne', 49, 5, 90000.00, '2018-4-02');
INSERT INTO Departments (DepartmentID, DepartmentName, Location) VALUES  (1, 'Sales', 'New York'), (2, 'Marketing', 'Los Angeles'), (3, 'HR', 'Chicago'), (4, 'Engineering', 'San Francisco'),(6, 'Legal', 'Denver');
```
### 3. Begin writing your queries
- [ ] Display all employees
- [ ] Display employee full name, as one column, and their department location
- [ ] Display employees that do not have a valid department
- [ ] Display departments that do not have any employees
- [ ] Display the average salary for each department in descending order
- [ ] Display the average salary for each department in descending order, only if the average is above $50k
- [ ] Display the highest paid employee for every department
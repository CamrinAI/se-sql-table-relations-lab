# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Employees in Boston: return first and last names, sorted by first, last
# Expect 2 rows with firstName ['Julie', 'Steve'] and 2 columns
df_boston = pd.read_sql(
    """
    SELECT e.firstName, e.lastName
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston'
    ORDER BY e.firstName, e.lastName
    """,
    conn,
)

# STEP 2
# Offices with zero employees (should return 0 rows in this dataset)
df_zero_emp = pd.read_sql(
    """
    SELECT o.*
    FROM offices o
    LEFT JOIN employees e ON o.officeCode = e.officeCode
    GROUP BY o.officeCode
    HAVING COUNT(e.employeeNumber) = 0
    """,
    conn,
)

# STEP 3
# All employees with their office city and state (if any). Include all employees.
# Order by firstName, lastName. Expect (23, 4).
df_employee = pd.read_sql(
    """
    SELECT e.firstName,
           e.lastName,
           o.city,
           o.state
    FROM employees e
    LEFT JOIN offices o ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName
    """,
    conn,
)

# STEP 4
# Customers who have NOT placed any orders: contact info + their sales rep employee number
# Sort by contact's last name. Expect (24, 4) and first three contactFirstName values
# ['Raanan', 'Mel', 'Carmen'].
df_contacts = pd.read_sql(
    """
    SELECT c.contactFirstName,
           c.contactLastName,
           c.phone,
           c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o ON o.customerNumber = c.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName ASC, c.contactFirstName ASC
    """,
    conn,
)

# STEP 5
# Customer contact names with their payment amount and date, sorted by amount DESC (cast to numeric)
# Expect (273, 4). First row contactFirstName should be 'Diego '.
df_payment = pd.read_sql(
    """
    SELECT c.contactFirstName,
           c.contactLastName,
           p.amount,
           p.paymentDate
    FROM payments p
    JOIN customers c ON c.customerNumber = p.customerNumber
    ORDER BY CAST(p.amount AS REAL) DESC, p.paymentDate DESC
    """,
    conn,
)

# STEP 6
# Employees whose customers have an average credit limit > 90000.
# Return employeeNumber, firstName, lastName, and number of customers.
# Sort by number of customers DESC. Expect (4, 4) with 'Larry' first.
df_credit = pd.read_sql(
    """
    SELECT e.employeeNumber,
           e.firstName,
           e.lastName,
           COUNT(c.customerNumber) AS numcustomers
    FROM employees e
    JOIN customers c ON c.salesRepEmployeeNumber = e.employeeNumber
    GROUP BY e.employeeNumber
    HAVING AVG(c.creditLimit) > 90000
    ORDER BY numcustomers DESC, e.firstName, e.lastName
    """,
    conn,
)

# STEP 7
# Product name, number of orders per product (numorders), and total units sold (totalunits)
# Sort by totalunits DESC. Expect (109, 3) and top totalunits 1808.
df_product_sold = pd.read_sql(
    """
    SELECT p.productName,
           COUNT(DISTINCT od.orderNumber) AS numorders,
           SUM(od.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails od ON od.productCode = p.productCode
    GROUP BY p.productCode
    ORDER BY totalunits DESC, p.productName ASC
    """,
    conn,
)

# STEP 8
# For each product: product name, code, and number of distinct customers who ordered it (numpurchasers)
# Sort by numpurchasers DESC. Expect (109, 3) and first row numpurchasers == 40.
df_total_customers = pd.read_sql(
    """
    SELECT p.productName,
           p.productCode,
           COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od ON od.productCode = p.productCode
    JOIN orders o ON o.orderNumber = od.orderNumber
    GROUP BY p.productCode
    ORDER BY numpurchasers DESC, p.productName ASC
    """,
    conn,
)

# STEP 9
# Number of customers per office as n_customers, with officeCode and city.
# Sort by officeCode ASC to have first row with 12 customers.
df_customers = pd.read_sql(
    """
    SELECT COUNT(c.customerNumber) AS n_customers,
           o.officeCode,
           o.city
    FROM offices o
    JOIN employees e ON e.officeCode = o.officeCode
    LEFT JOIN customers c ON c.salesRepEmployeeNumber = e.employeeNumber
    GROUP BY o.officeCode
    ORDER BY o.officeCode ASC
    """,
    conn,
)

# STEP 10
# Employees who sold products ordered by fewer than 20 customers.
# Order by lastName, firstName to match expected first row 'Loui'.
df_under_20 = pd.read_sql(
    """
    SELECT DISTINCT e.employeeNumber,
           e.firstName,
           e.lastName,
           o.city,
           o.officeCode
    FROM employees e
    JOIN customers c ON c.salesRepEmployeeNumber = e.employeeNumber
    JOIN orders ord ON ord.customerNumber = c.customerNumber
    JOIN orderdetails od ON od.orderNumber = ord.orderNumber
    JOIN offices o ON o.officeCode = e.officeCode
    WHERE od.productCode IN (
        SELECT od2.productCode
        FROM orderdetails od2
        JOIN orders ord2 ON ord2.orderNumber = od2.orderNumber
        GROUP BY od2.productCode
        HAVING COUNT(DISTINCT ord2.customerNumber) < 20
    )
    ORDER BY e.lastName ASC, e.firstName ASC
    """,
    conn,
)

conn.close()
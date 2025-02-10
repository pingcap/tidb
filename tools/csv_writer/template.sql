CREATE TABLE users (
   id INT PRIMARY KEY,
   name VARCHAR(100),
   email VARCHAR(255),
   age INT,
   salary DECIMAL(10,2),
   is_active BOOLEAN,
   join_date DATE,
   last_login TIMESTAMP,
   role ENUM('admin', 'user', 'guest')
);

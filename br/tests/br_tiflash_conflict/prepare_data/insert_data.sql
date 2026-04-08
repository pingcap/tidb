-- Switch to the test database
USE test;

-- Insert data into table1
INSERT INTO table1 (id, name, age, salary, join_date) VALUES
(3, 'Alice Brown', 28, 55000.00, '2021-02-20'),
(4, 'Bob White', 32, 48000.00, '2019-11-10'),
(5, 'Charlie Black', 45, 70000.00, '2018-05-30'),
(6, 'Diana Green', 29, 62000.00, '2020-08-12'),
(7, 'Evan Blue', 38, 53000.00, '2017-07-24'),
(8, 'Fiona Yellow', 24, 45000.00, '2022-03-14'),
(9, 'George Pink', 33, 59000.00, '2021-06-22'),
(10, 'Hannah Red', 27, 61000.00, '2020-12-15');

-- Insert data into table2
INSERT INTO table2 (id, description, status, created_at, updated_at) VALUES
(3, 'User logi \n', 'active', '2023-03-01 09:00:00', '2023-03-01 09:00:00'),
(4, 'Password reset \n', 'inactive', '2023-04-05 10:00:00', '2023-04-05 10:00:00'),
(5, 'Email verification \n', 'active', '2023-05-10 11:00:00', '2023-05-10 11:00:00'),
(6, 'Account suspension \n', 'inactive', '2023-06-15 12:00:00', '2023-06-15 12:00:00'),
(7, 'Profile update \n', 'active', '2023-07-20 13:00:00', '2023-07-20 13:00:00'),
(8, 'Subscription renewal \n', 'inactive', '2023-08-25 14:00:00', '2023-08-25 14:00:00'),
(9, 'Payment processing \n', 'active', '2023-09-30 15:00:00', '2023-09-30 15:00:00'),
(10, 'Order fulfillment \n', 'inactive', '2023-10-05 16:00:00', '2023-10-05 16:00:00');

-- Insert data into table3
INSERT INTO table3 (id, score, grade, remarks, is_pass) VALUES
(3, 88.0, 'B', 'Very good performance \n', TRUE),
(4, 78.5, 'C', 'Good performance \n', TRUE),
(5, 95.2, 'A', 'Outstanding performance \n', TRUE),
(6, 69.4, 'D', 'Satisfactory performance \n', TRUE),
(7, 82.3, 'B', 'Very good performance \n', TRUE),
(8, 91.0, 'A', 'Excellent performance \n', TRUE),
(9, 75.6, 'C', 'Good performance \n', TRUE),
(10, 68.9, 'D', 'Satisfactory performance \n', TRUE);
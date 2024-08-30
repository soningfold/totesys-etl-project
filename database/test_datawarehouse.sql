DROP DATABASE IF EXISTS test_totesys_datawarehouse;

CREATE DATABASE test_totesys_datawarehouse;

\c test_totesys_datawarehouse

CREATE TABLE "fact_sales_order" (
  "sales_record_id" SERIAL PRIMARY KEY,
  "sales_order_id" int NOT NULL,
  "created_date" date NOT NULL,
  "created_time" time NOT NULL,
  "last_updated_date" date NOT NULL,
  "last_updated_time" time NOT NULL,
  "sales_staff_id" int NOT NULL,
  "counterparty_id" int NOT NULL,
  "units_sold" int NOT NULL,
  "unit_price" numeric(10, 2) NOT NULL,
  "currency_id" int NOT NULL,
  "design_id" int NOT NULL,
  "agreed_payment_date" date NOT NULL,
  "agreed_delivery_date" date NOT NULL,
  "agreed_delivery_location_id" int NOT NULL
);

CREATE TABLE "dim_date" (
  "date_id" date PRIMARY KEY NOT NULL,
  "year" int NOT NULL,
  "month" int NOT NULL,
  "day" int NOT NULL,
  "day_of_week" int NOT NULL,
  "day_name" varchar NOT NULL,
  "month_name" varchar NOT NULL,
  "quarter" int NOT NULL
);

CREATE TABLE "dim_staff" (
  "staff_id" int PRIMARY KEY NOT NULL,
  "first_name" varchar NOT NULL,
  "last_name" varchar NOT NULL,
  "department_name" varchar NOT NULL,
  "location" varchar NOT NULL,
  "email_address" varchar NOT NULL
);

CREATE TABLE "dim_location" (
  "location_id" int PRIMARY KEY NOT NULL,
  "address_line_1" varchar NOT NULL,
  "address_line_2" varchar,
  "district" varchar,
  "city" varchar NOT NULL,
  "postal_code" varchar NOT NULL,
  "country" varchar NOT NULL,
  "phone" varchar NOT NULL
);

CREATE TABLE "dim_currency" (
  "currency_id" int PRIMARY KEY NOT NULL,
  "currency_code" varchar NOT NULL,
  "currency_name" varchar NOT NULL
);

CREATE TABLE "dim_design" (
  "design_id" int PRIMARY KEY NOT NULL,
  "design_name" varchar NOT NULL,
  "file_location" varchar NOT NULL,
  "file_name" varchar NOT NULL
);

CREATE TABLE "dim_counterparty" (
  "counterparty_id" int PRIMARY KEY NOT NULL,
  "counterparty_legal_name" varchar NOT NULL,
  "counterparty_legal_address_line_1" varchar NOT NULL,
  "counterparty_legal_address_line_2" varchar,
  "counterparty_legal_district" varchar,
  "counterparty_legal_city" varchar NOT NULL,
  "counterparty_legal_postal_code" varchar NOT NULL,
  "counterparty_legal_country" varchar NOT NULL,
  "counterparty_legal_phone_number" varchar NOT NULL
);

-- INSERT INTO "dim_date" 
-- ("date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter") 
-- VALUES 
-- ('2022-01-01', 2022, 1, 1, 6, 'Saturday', 'January', 1), 
-- ('2022-01-02', 2022, 1, 2, 7, 'Sunday', 'January', 1), 
-- ('2022-01-03', 2022, 1, 3, 1, 'Monday', 'January', 1), 
-- ('2022-01-04', 2022, 1, 4, 2, 'Tuesday', 'January', 1), 
-- ('2022-01-05', 2022, 1, 5, 3, 'Wednesday', 'January', 1),
-- ('2022-01-06', 2022, 1, 6, 4, 'Thursday', 'January', 1), 
-- ('2022-01-07', 2022, 1, 7, 5, 'Friday', 'January', 1), 
-- ('2022-01-08', 2022, 1, 8, 6, 'Saturday', 'January', 1), 
-- ('2022-01-09', 2022, 1, 9, 7, 'Sunday', 'January', 1), 
-- ('2022-01-10', 2022, 1, 10, 1, 'Monday', 'January', 1),
-- ('2022-01-11', 2022, 1, 11, 2, 'Tuesday', 'January', 1),
-- ('2022-01-12', 2022, 1, 12, 3, 'Wednesday', 'January', 1),
-- ('2022-01-13', 2022, 1, 13, 4, 'Thursday', 'January', 1),
-- ('2022-01-14', 2022, 1, 14, 5, 'Friday', 'January', 1),
-- ('2022-01-15', 2022, 1, 15, 6, 'Saturday', 'January', 1);


-- INSERT INTO "dim_staff" 
-- ("staff_id", "first_name", "last_name", "department_name", "location", "email_address") 
-- VALUES 
-- (1, 'John', 'Doe', 'Sales', 'London', 'john.doe@example.com'), 
-- (2, 'Jane', 'Smith', 'Marketing', 'New York', 'jane.smith@example.com'),
-- (3, 'David', 'Johnson', 'Finance', 'Tokyo', 'david.johnson@example.com'), 
-- (4, 'Sarah', 'Williams', 'HR', 'Berlin', 'sarah.williams@example.com'), 
-- (5, 'Michael', 'Brown', 'IT', 'San Francisco', 'michael.brown@example.com');

-- INSERT INTO "dim_location" 
-- ("location_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone") 
-- VALUES 
-- (1, '123 Main St', 'Apt 4B', 'Central', 'London', 'SW1A 1AA', 'United Kingdom', '+44 20 1234 5678'), 
-- (2, '456 Elm St', 'Suite 200', 'Downtown', 'New York', '10001', 'United States', '+1 212-555-1234'), 
-- (3, '789 Oak St', NULL, 'Shinjuku', 'Tokyo', '160-0022', 'Japan', '+81 3-1234-5678'), 
-- (4, '987 Maple St', NULL, 'Mitte', 'Berlin', '10115', 'Germany', '+49 30 12345678'), 
-- (5, '654 Pine St', 'Floor 10', 'Financial District', 'San Francisco', '94111', 'United States', '+1 415-555-1234');

-- INSERT INTO "dim_currency" 
-- ("currency_id", "currency_code", "currency_name") 
-- VALUES 
-- (1, 'USD', 'United States Dollar'), 
-- (2, 'GBP', 'British Pound Sterling'), 
-- (3, 'EUR', 'Euro'), 
-- (4, 'JPY', 'Japanese Yen'), 
-- (5, 'CAD', 'Canadian Dollar');

-- INSERT INTO "dim_design" 
-- ("design_id", "design_name", "file_location", "file_name") 
-- VALUES 
-- (1, 'Design 1', '/path/to/design1', 'design1.jpg'), 
-- (2, 'Design 2', '/path/to/design2', 'design2.jpg'), 
-- (3, 'Design 3', '/path/to/design3', 'design3.jpg'), 
-- (4, 'Design 4', '/path/to/design4', 'design4.jpg'), 
-- (5, 'Design 5', '/path/to/design5', 'design5.jpg');

-- INSERT INTO "dim_counterparty" 
-- ("counterparty_id", "counterparty_legal_name", "counterparty_legal_address_line_1", "counterparty_legal_address_line_2", "counterparty_legal_district", "counterparty_legal_city", "counterparty_legal_postal_code", "counterparty_legal_country", "counterparty_legal_phone_number") 
-- VALUES 
-- (1, 'Company A', '123 Main St', 'Suite 100', 'Central', 'London', 'SW1A 1AA', 'United Kingdom', '+44 20 1234 5678'), 
-- (2, 'Company B', '456 Elm St', 'Floor 5', 'Downtown', 'New York', '10001', 'United States', '+1 212-555-1234'), 
-- (3, 'Company C', '789 Oak St', NULL, 'Shinjuku', 'Tokyo', '160-0022', 'Japan', '+81 3-1234-5678'), 
-- (4, 'Company D', '987 Maple St', NULL, 'Mitte', 'Berlin', '10115', 'Germany', '+49 30 12345678'), 
-- (5, 'Company E', '654 Pine St', 'Suite 200', 'Financial District', 'San Francisco', '94111', 'United States', '+1 415-555-1234'); 

-- INSERT INTO "fact_sales_order" 
-- ("sales_order_id", "created_date", "created_time", "last_updated_date", "last_updated_time", "sales_staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "design_id", "agreed_payment_date", "agreed_delivery_date", "agreed_delivery_location_id") 
-- VALUES 
-- (1, '2022-01-01', '10:00:00', '2022-01-01', '10:00:00', 1, 1, 10, 100.00, 1, 1, '2022-01-05', '2022-01-10', 1), 
-- (2, '2022-01-02', '11:00:00', '2022-01-02', '11:00:00', 2, 2, 20, 200.00, 2, 2, '2022-01-06', '2022-01-11', 2), 
-- (3, '2022-01-03', '12:00:00', '2022-01-03', '12:00:00', 3, 3, 30, 300.00, 3, 3, '2022-01-07', '2022-01-12', 3), 
-- (4, '2022-01-04', '13:00:00', '2022-01-04', '13:00:00', 4, 4, 40, 400.00, 4, 4, '2022-01-08', '2022-01-13', 4), 
-- (5, '2022-01-05', '14:00:00', '2022-01-05', '14:00:00', 5, 5, 50, 500.00, 5, 5, '2022-01-09', '2022-01-14', 5);

-- SELECT * FROM "fact_sales_order";
-- SELECT * FROM "dim_date";
-- SELECT * FROM "dim_staff";
-- SELECT * FROM "dim_location";
-- SELECT * FROM "dim_currency";
-- SELECT * FROM "dim_design";
-- SELECT * FROM "dim_counterparty";
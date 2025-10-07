CREATE DATABASE orders;
USE orders;

-- USUARIOS-------------------------------------
CREATE TABLE users ( 
	id INT NOT NULL PRIMARY KEY,
    name VARCHAR(50) NULL,
    surname VARCHAR(50) NULL,
    phone VARCHAR(50) NULL,
    email VARCHAR(100) NULL,
    birth_date VARCHAR(20) NULL,
    country VARCHAR(50) NOT NULL,
    city VARCHAR(50) NULL,
    postal_code VARCHAR(20) NULL,
    address VARCHAR(100) NULL
    );

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/american_users.csv'
INTO TABLE users
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/european_users.csv'
INTO TABLE users
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;

-- EMPRESAS ----------------------------------------------------
CREATE TABLE company (
	company_id VARCHAR(20) NOT NULL PRIMARY KEY,
    company_name VARCHAR(50) NULL,
    phone VARCHAR(50) NULL,
    email VARCHAR(100) NULL,
    country VARCHAR(50) NOT NULL,
    website VARCHAR(150) NULL
    );

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/companies.csv'
INTO TABLE company
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;

-- TARJETAS------------------------------------------
CREATE TABLE credit_cards (
	id VARCHAR(20) NOT NULL PRIMARY KEY,
    user_id INT NULL,
    iban VARCHAR(100) NULL,
    pan VARCHAR(100) NULL,
    pin VARCHAR(5) NULL,
    cvv VARCHAR(5) NULL,
    track1 VARCHAR(100) NULL,
    track2 VARCHAR(100) NULL,
    expiring_date VARCHAR(50) NOT NULL
    );
    

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/credit_cards.csv'
INTO TABLE credit_cards
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;

-- TRANSACCIONES------------------------------------------
CREATE TABLE transactions (
	id VARCHAR(50) NOT NULL PRIMARY KEY,
    card_id VARCHAR(100) NOT NULL,
    business_id VARCHAR(50) NOT NULL,
    timestamp timestamp,
    amount DECIMAL(10,2), 
    declined TINYINT NOT NULL, 
    products_id VARCHAR(50) NOT NULL,
    user_id INT NOT NULL,
    lat FLOAT,
    longitude FLOAT,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (card_id) REFERENCES credit_cards(id),
	FOREIGN KEY (business_id) REFERENCES company(company_id)
    );

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ";"
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;

-- 1.1 Subconsulta usuarios con más de 80 transacciones
SELECT *
FROM users u 
WHERE id IN (
	SELECT t.user_id
    FROM transactions t
    WHERE declined = 0
    GROUP BY user_id
    HAVING COUNT(t.id) > 80
    );

-- 1.2 Media de 'amount' por IBAN de las tarjetas de la compañia 'Donec Ltd'
SELECT cc.iban as IBAN, ROUND(AVG(t.amount),2) as Cantidad_Media
FROM credit_cards cc
JOIN transactions t 
ON cc.id = t.card_id
WHERE t.business_id = (
	SELECT c.company_id
    FROM company c
    WHERE company_name = 'Donec Ltd'
    AND declined = 0)
GROUP BY IBAN; 

-- 2 Creación tabla del estado de las tarjetas en base a sus tres últimas transacciones
CREATE TABLE card_status as ( 
	SELECT card_id, 
	SUM(declined) as reject_count,
    CASE
		WHEN sum(declined) = 3 THEN 'Rejected'
        ELSE 'Active'
	END AS state
	FROM (
		SELECT t.card_id, t.declined,
		ROW_NUMBER() OVER(PARTITION BY t.card_id ORDER BY t.timestamp DESC) as last_mov
        FROM transactions t
        ) as mov
    WHERE last_mov <= 3
    GROUP BY card_id
    );

ALTER TABLE card_status
ADD CONSTRAINT fk_card_id FOREIGN KEY (card_id) REFERENCES credit_cards(id);

-- 2.1 Tarjetas activas
SELECT *
FROM card_status
WHERE state = 'Active';

-- 3 Tabla para unir productos y transacciones
-- PRODUCTOS------------------------------------------
CREATE TABLE products (
	id INT NOT NULL PRIMARY KEY,
    product_name VARCHAR(50),
    price VARCHAR(10),
    colour VARCHAR(20),
    weight FLOAT, 
    warehouse_id VARCHAR(10) 
    );
    
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products.csv'
INTO TABLE products
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;

-- DETALLES ----------------------------------------
CREATE TABLE details_transactions (
	id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    product_id INT NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES transactions(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
    );

INSERT INTO details_transactions (transaction_id,product_id)
SELECT t.id, j_query.product_id
FROM transactions t
JOIN JSON_TABLE(
  CONCAT('["', REGEXP_REPLACE (t.products_id, '[[:space:]]*,[[:space:]]*', '","'), '"]'),
  '$[*]' COLUMNS (product_id VARCHAR(50) PATH '$')
) AS j_query;

-- 3.1 Veces que se ha vendido cada producto
SELECT p.product_name as Producto, COUNT(dt.product_id) as Venta_productos
FROM details_transactions dt
JOIN products p 
ON p.id = dt.product_id
GROUP BY p.id, Producto
ORDER BY Venta_productos DESC;



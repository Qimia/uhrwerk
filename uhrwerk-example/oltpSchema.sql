Create SCHEMA IF NOT EXISTS qimia_oltp;

#SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS qimia_oltp.stores;
DROP TABLE IF EXISTS qimia_oltp.employees;
DROP TABLE IF EXISTS qimia_oltp.discounts;
DROP TABLE IF EXISTS qimia_oltp.sales_items;
DROP TABLE IF EXISTS qimia_oltp.stores_employees;
DROP TABLE IF EXISTS qimia_oltp.products;
DROP TABLE IF EXISTS qimia_oltp.sales;
DROP TABLE IF EXISTS qimia_oltp.suppliers;
DROP TABLE IF EXISTS qimia_oltp.currency;

CREATE TABLE IF NOT EXISTS qimia_oltp.currency (
    country VARCHAR(50) PRIMARY KEY
    ,currency_name VARCHAR(255)
    ,exchange_rate float
);

CREATE TABLE IF NOT EXISTS qimia_oltp.stores (
	store_id INT PRIMARY KEY
	,store_name VARCHAR (255)
	,address VARCHAR (255)
	,city VARCHAR (255)
	,zip_code INT
	,latitude float
	,longitude float
	,country VARCHAR (50)
	,opened_since date
	,last_redesigned date
	,selling_square_footage int
	,total_square_footage int
	#,FOREIGN KEY (country) REFERENCES qimia_oltp.currency (country)
);

CREATE TABLE IF NOT EXISTS qimia_oltp.employees (
    employee_id INT PRIMARY KEY
    ,first_name VARCHAR(255)
    ,last_name VARCHAR(255)
    ,department VARCHAR(255)
    ,salary INT
    ,address VARCHAR(255)
    ,city VARCHAR(255)
    ,zip_code INT
    ,latitude float
	,longitude float
    ,country VARCHAR(50)
    ,employed_since date
    ,supervisor INT
    #,FOREIGN KEY (country) REFERENCES qimia_oltp.currency (country)
);

CREATE TABLE IF NOT EXISTS qimia_oltp.suppliers (
    supplier_id INT PRIMARY KEY
    ,company_name VARCHAR(255)
    ,bank_account VARCHAR(255)
    ,preferred boolean
    ,address VARCHAR(255)
    ,city VARCHAR(255)
    ,zip_code INT
    ,latitude float
	,longitude float
    ,country VARCHAR(255)
    #,FOREIGN KEY (country) REFERENCES qimia_oltp.currency (country)
);

CREATE TABLE IF NOT EXISTS qimia_oltp.products (
    product_id VARCHAR(50) PRIMARY KEY
    ,brand VARCHAR(255)
    ,type VARCHAR(255)
    ,category VARCHAR(255)
    ,fit VARCHAR(255)
    ,size VARCHAR(255)
    ,sex VARCHAR(1)
    ,description VARCHAR(255)
    ,package_size INT
    ,purchase_price float
    ,selling_price float
    ,supplier INT
    #,FOREIGN KEY (supplier) REFERENCES qimia_oltp.suppliers (supplier_id)
);

CREATE TABLE IF NOT EXISTS qimia_oltp.sales (
    sales_id INT PRIMARY KEY
    ,cashier INT
    ,store INT
    ,selling_date date
    #,FOREIGN KEY (cashier) REFERENCES qimia_oltp.employees (employee_id),
    #,FOREIGN KEY (store) REFERENCES qimia_oltp.stores (store_id)
);

CREATE TABLE IF NOT EXISTS qimia_oltp.sales_items (
    sales_id INT
    ,product_id VARCHAR(50)
    ,quantity INT
    #,FOREIGN KEY (sales_id) REFERENCES qimia_oltp.sales (sales_id),
    #,FOREIGN KEY (product_id) REFERENCES qimia_oltp.products (product_id)
);

CREATE TABLE IF NOT EXISTS qimia_oltp.discounts (
    store_id INT
    ,product_id VARCHAR(50)
    ,percentage float
    ,start_date date
    ,end_date date
    #,FOREIGN KEY (store_id) REFERENCES qimia_oltp.stores (store_id),
    #,FOREIGN KEY (product_id) REFERENCES qimia_oltp.products (product_id)
);



CREATE TABLE IF NOT EXISTS qimia_oltp.stores_employees (
    store_id INT
    ,employee_id INT
    #,FOREIGN KEY (store_id) REFERENCES qimia_oltp.stores (store_id),
    #,FOREIGN KEY (employee_id) REFERENCES qimia_oltp.employees (employee_id)
);


TRUNCATE TABLE qimia_oltp.stores;
TRUNCATE TABLE qimia_oltp.employees;
TRUNCATE TABLE qimia_oltp.discounts;
TRUNCATE TABLE qimia_oltp.sales_items;
TRUNCATE TABLE qimia_oltp.stores_employees;
TRUNCATE TABLE qimia_oltp.products;
TRUNCATE TABLE qimia_oltp.sales;
TRUNCATE TABLE qimia_oltp.suppliers;
TRUNCATE TABLE qimia_oltp.currency;

#SET FOREIGN_KEY_CHECKS = 1;

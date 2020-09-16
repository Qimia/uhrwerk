create SCHEMA IF NOT EXISTS qimia_oltp;

drop table IF EXISTS qimia_oltp.stores CASCADE;
drop table IF EXISTS qimia_oltp.employees CASCADE;
drop table IF EXISTS qimia_oltp.discounts CASCADE;
drop table IF EXISTS qimia_oltp.sales_items CASCADE;
drop table IF EXISTS qimia_oltp.stores_employees CASCADE;
drop table IF EXISTS qimia_oltp.products CASCADE;
drop table IF EXISTS qimia_oltp.sales CASCADE;
drop table IF EXISTS qimia_oltp.suppliers CASCADE;
drop table IF EXISTS qimia_oltp.currency CASCADE;

create TABLE IF NOT EXISTS qimia_oltp.currency (
    country VARCHAR(50) PRIMARY KEY
    ,currency_name VARCHAR(255)
    ,exchange_rate float
);

create TABLE IF NOT EXISTS qimia_oltp.stores (
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
);

create TABLE IF NOT EXISTS qimia_oltp.employees (
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
);

create TABLE IF NOT EXISTS qimia_oltp.suppliers (
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
);

create TABLE IF NOT EXISTS qimia_oltp.products (
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
);

create TABLE IF NOT EXISTS qimia_oltp.sales (
    sales_id INT PRIMARY KEY
    ,cashier INT
    ,store INT
    ,selling_date date
    ,INDEX (selling_date)
);

create TABLE IF NOT EXISTS qimia_oltp.sales_items (
    sales_id INT
    ,product_id VARCHAR(50)
    ,quantity INT
);

create TABLE IF NOT EXISTS qimia_oltp.discounts (
    store_id INT
    ,product_id VARCHAR(50)
    ,percentage float
    ,start_date date
    ,end_date date
);

create TABLE IF NOT EXISTS qimia_oltp.stores_employees (
    store_id INT
    ,employee_id INT
);


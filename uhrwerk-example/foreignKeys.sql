ALTER TABLE qimia_oltp.stores
ADD CONSTRAINT FK_storeCountry
FOREIGN KEY (country) REFERENCES qimia_oltp.currency (country);

ALTER TABLE qimia_oltp.employees
ADD CONSTRAINT FK_employeeCountry
FOREIGN KEY (country) REFERENCES qimia_oltp.currency (country);

ALTER TABLE qimia_oltp.suppliers
ADD CONSTRAINT FK_supplierCountry
FOREIGN KEY (country) REFERENCES qimia_oltp.currency (country);

ALTER TABLE qimia_oltp.products
ADD CONSTRAINT FK_productSupplier
FOREIGN KEY (supplier) REFERENCES qimia_oltp.suppliers (supplier_id);

ALTER TABLE qimia_oltp.sales
ADD CONSTRAINT FK_salesCashier
FOREIGN KEY (cashier) REFERENCES qimia_oltp.employees (employee_id);

ALTER TABLE qimia_oltp.sales
ADD CONSTRAINT FK_salesStore
FOREIGN KEY (store) REFERENCES qimia_oltp.stores (store_id);

ALTER TABLE qimia_oltp.sales_items
ADD CONSTRAINT FK_salesHeader
FOREIGN KEY (sales_id) REFERENCES qimia_oltp.sales (sales_id);

ALTER TABLE qimia_oltp.sales_items
ADD CONSTRAINT FK_itemProduct
FOREIGN KEY (product_id) REFERENCES qimia_oltp.products (product_id);

ALTER TABLE qimia_oltp.discounts
ADD CONSTRAINT FK_discountStores
FOREIGN KEY (store_id) REFERENCES qimia_oltp.stores (store_id);

ALTER TABLE qimia_oltp.discounts
ADD CONSTRAINT FK_discountProducts
FOREIGN KEY (product_id) REFERENCES qimia_oltp.products (product_id);

ALTER TABLE qimia_oltp.stores_employees
ADD CONSTRAINT FK_seStores
FOREIGN KEY (store_id) REFERENCES qimia_oltp.stores (store_id);

ALTER TABLE qimia_oltp.stores_employees
ADD CONSTRAINT FK_seEmployees
FOREIGN KEY (employee_id) REFERENCES qimia_oltp.employees (employee_id);
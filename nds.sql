-- создаем БД
CREATE DATABASE supermarket
OWNER  postgres
ENCODING utf8;


-- создаем схему
CREATE SCHEMA nds;

-- Таблица с городами
create table nds.city (
	city_id int primary key,
	city varchar(250) not null unique -- город
	);

-- Таблица с гендерами
create table nds.gender (
	gender_id int primary key,
	gender varchar(250) not null unique -- пол
	);

-- Таблица типов клиента
create table nds.customertype (
	customertype_id int primary key,
	customertype varchar(250) not null unique -- Тип клиента (с картой магазина или без)
	);

-- таблица категорий товаров
create table nds.productline (
	productline_id int primary key,
	productline varchar(250) not null unique -- Категория товара
	);

-- таблица с типами платежей  
create table nds.paymenttype (
	paymenttype_id int primary key,
	paymenttype varchar(250) not null unique -- метод оплаты
	);

-- таблица филиалов
create table nds.branch (
    branch_id int primary key,
    city_id int not null references nds.city(city_id) on delete restrict on update cascade,
    branch varchar(250) not null unique -- название филиала
	);

--таблица товаров
create table nds.product (
	product_id int primary key,
	productline_id int not null references nds.productline(productline_id) on delete restrict on update cascade,
	unitprice decimal(20,2) not null-- цена за еденицу товара не включая налог
	);
	
-- таблица клиентов
create table nds.customer (
    customer_id int primary key,
    gender_id int not null references nds.gender(gender_id) on delete restrict on update cascade,
    customertype_id int not null references nds.customertype(customertype_id) on delete restrict on update cascade,
    rating numeric(3,1) not null-- рейтинг
    );
 
-- таблица заказов   
create table nds.order (
	order_id int primary key,
	branch_id int not null references nds.branch (branch_id) on delete restrict on update cascade,
    customer_id int not null references nds.customer (customer_id) on delete restrict on update cascade,
	product_id int not null references nds.product(product_id) on delete restrict on update cascade,
	paymenttype_id int not null references nds.paymenttype(paymenttype_id) on delete restrict on update cascade,
	invoice varchar(250) not null, -- номер счета
	quantity int not null, -- кол-во товара
	tax decimal(20,4) not null, -- налог
	dt timestamp not null-- дата и время продажи
	);

------------------------------------------------------------------------------------------------------------
-- после отработки ETL добавляем первичному ключу автоинкремент

create sequence nds.city_city_id_seq minvalue 3;
alter table nds.city alter city_id set default nextval('nds.city_city_id_seq');
alter sequence nds.city_city_id_seq owned by nds.city.city_id;

create sequence nds.gender_gender_id_seq minvalue 2;
alter table nds.gender alter gender_id set default nextval('nds.gender_gender_id_seq');
alter sequence nds.gender_gender_id_seq owned by nds.gender.gender_id;

create sequence nds.customertype_customertype_id_seq minvalue 2;
alter table nds.customertype alter customertype_id set default nextval('nds.customertype_customertype_id_seq');
alter sequence nds.customertype_customertype_id_seq owned by nds.customertype.customertype_id;

create sequence nds.productline_productline_id_seq minvalue 6;
alter table nds.productline   alter productline_id set default nextval('nds.productline_productline_id_seq');
alter sequence nds.productline_productline_id_seq owned by nds.productline.productline_id;
	
create sequence nds.paymenttype_paymenttype_id_seq minvalue 3;
alter table nds.paymenttype alter paymenttype_id set default nextval('nds.paymenttype_paymenttype_id_seq');
alter sequence nds.paymenttype_paymenttype_id_seq owned by nds.paymenttype.paymenttype_id;
	
create sequence nds.branch_branch_id_seq minvalue 3;
alter table nds.branch alter branch_id set default nextval('nds.branch_branch_id_seq');
alter sequence nds.branch_branch_id_seq owned by nds.branch.branch_id;

create sequence nds.product_product_id_seq minvalue 993;
alter table nds.product alter product_id set default nextval('nds.product_product_id_seq');
alter sequence nds.product_product_id_seq owned by nds.product.product_id;

create sequence nds.customer_customer_id_seq minvalue 241;
alter table nds.customer alter customer_id set default nextval('nds.customer_customer_id_seq');
alter sequence nds.customer_customer_id_seq owned by nds.customer.customer_id;

create sequence nds.order_order_id_seq minvalue 1000;
alter table nds."order" alter order_id set default nextval('nds.order_order_id_seq');
alter sequence nds.order_order_id_seq owned by nds."order".order_id;



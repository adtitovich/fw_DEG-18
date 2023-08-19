-- хранилище данных

-- создаем схемы
create schema dds;
create schema rejected;


--Dim_Calendar - справочник дат
create table dds.dim_calendar
as
with dates as (
    select dd::date as dt
    from generate_series
            ('2019-01-01'::timestamp
            , '2019-03-30'::timestamp
            , '1 day'::interval) dd )
select
    to_char(dt, 'YYYYMMDD')::int as id, -- суррогатный ключ
    dt as date, --дата
    to_char(dt, 'YYYY-MM-DD') as ansi_date, --дата в формате ansi 
    date_part('isodow', dt)::int as day, -- номер дня в году
    date_part('week', dt)::int as week_number, -- номер недели в году
    date_part('month', dt)::int as month, -- номер месяца в году
    date_part('isoyear', dt)::int as year, -- год
    (date_part('isodow', dt)::smallint between 1 and 5)::int as work_day, -- рабочий день
    (date_part('isodow', dt)::smallint between 6 and 7)::int as weekend, -- выходной день
    (to_char(dt, 'YYYYMMDD')::int in (
        20190101, 20190104, 20190106, 20190212, 20190302, 20190304, 20190320, 20130327
        ))::int as holiday -- официальный праздник
from dates
order by dt;

alter table dds.Dim_Calendar add primary key (id);
alter table dds.Dim_Calendar 
	alter column ansi_date set not null,
	alter column "day" set not null,
	alter column week_number set not null,
	alter column "month" set not null,
	alter column "year" set not null,
	alter column work_day set not null,
	alter column weekend set not null,
	alter column holiday set not null;


-- Dim_Branch -- справочник филиалов
create table dds.dim_branch (
	id serial not null primary key,
    branch_id int not null,
    branch varchar(250) not null, -- название филиала
    city varchar(250) not null -- город
    );
    
-- Dim_Customer -- справочник покупателей
  create table dds.dim_customer (
	id serial not null primary key,
    customer_id int not null,
    gender varchar(250) not null, -- пол
    customertype varchar(250) not null, -- Тип пользователя (с картой магазина или без)
    rating numeric(3,1) not null -- рейтинг
    );

-- Dim_Product -- справочник товаров
  create table dds.dim_product (
  	id serial not null primary key,
  	product_id int not null,
	productline varchar(250) not null, -- Категория товара
	unitprice decimal(20,2) not null -- цена за еденицу товара не включая налог
	);
	
-- Dim_Paymenttype -- справочник типов платежей  
create table dds.dim_paymenttype (
	id serial not null primary key,
	paymenttype_id int not null,
	paymenttype varchar(250) not null -- метод оплаты
	);


-- Fact_Order - покупки
create table dds.fact_order (
	date_key int not null references dds.Dim_Calendar(id), -- Дата покупки (key)
	branch_key int not null references dds.Dim_Branch(id), -- Филиал (key)
	customer_key int not null references dds.Dim_Customer(id), -- Покупатель (key)
	product_key int not null references dds.Dim_Product(id), -- Товар (key)
	paymenttype_key int not null references dds.Dim_Paymenttype(id), -- метод оплаты (key)
	dt timestamp not null, -- дата и время продажи
	invoice varchar(250) not null, -- номер счета
	unitprice decimal(20,2) not null, -- цена за еденицу товара не включая налог
	quantity int not null, -- кол-во товара
	salesprice decimal(20,2) not null, -- сумма продажи без налога
	tax decimal(20,4) not null, -- налог
	total decimal(20,4)  not null -- сумма продажи включая налог
	);

-- rejected-таблицы
create table rejected.customer (
	customer_id int,
	gender varchar(250),	
    customertype varchar(250),
    rating numeric(3,1)
    );

create table rejected.product (
	product_id int,
	productline varchar(250),
	unitprice decimal(20,2)
);

create table rejected.order (
	dt timestamp,
	invoice varchar(250),
	unitprice decimal(20,2),
	quantity int,
	salesprice decimal(20,2),
	tax decimal(20,4),
	total decimal(20,4)
);
	
CREATE EXTENSION pg_stat_statements;


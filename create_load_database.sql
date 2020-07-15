-- Load Target data

DROP TABLE IF EXISTS data_target
;

create table data_target (
	id serial primary key,
	name varchar(255) not null,
	link varchar(255) not null,
	upc varchar(225) null,
	weight varchar(225) null,
	price varchar not null,
	rating_score varchar(225) null,
	rating_count varchar(30) null
)
;

copy data_target(id, name, link, upc, weight, price, rating_score, rating_count)
from 'C:\Users\jfung\Desktop\UCB\13-ETL_Project\data_target.csv' delimiter ',' csv header
;

select *
from data_target
;


-- Load Walgreens data

DROP TABLE IF EXISTS data_walgreens
;

create table data_walgreens (
	id serial primary key,
	name varchar(255) not null,
	link varchar(255) not null,
	upc varchar(255) null,
	price float null,
	rating float null,
	reviews int null
)
;

copy data_walgreens(id, name, link, upc, price, rating, reviews)
from 'C:\Users\jfung\Desktop\UCB\13-ETL_Project\data_walgreens.csv' delimiter ',' csv header
;

select *
from data_walgreens
;


-- Load Walmart data

drop table if exists data_walmart
;

create table if not exists data_walmart (
	id serial not null,
	href varchar(255) not null,
	name varchar(255) not null,
	upc varchar null,
	currency varchar(1) null,
	price float null,
	customer_rating float null,
	number_of_reviews int null,
	seller varchar(255) null,
	ingredients varchar(5000)null,
	primary key(id)
)
;

copy data_walmart(href, name, upc, currency, price, customer_rating, number_of_reviews, seller, ingredients)
from 'C:\Users\jfung\Desktop\UCB\13-ETL_Project\data_walmart.csv' delimiter ',' csv header
;

select *
from data_walmart
;


-- Compare prices based on UPC

select dt.upc,
dt.name as "target_name",
--dwg.name as "walgreens_name",
dwm.name as "walmart_name",
cast(trim('$' from dt.price) as float) as "target_price",
--dwg.price as "walgreens_price",
dwm.price as "walmart_price",
case
	when cast(trim('$' from dt.price) as float) < dwm.price then 'Target'
	when cast(trim('$' from dt.price) as float) > dwm.price then 'Walmart'
	else 'Tie'
	end as "cheaper_retailer"
from data_target dt
--join data_walgreens dwg
--on dt.upc = dwg.upc
join data_walmart dwm
on dt.upc = dwm.upc
where dt.price != 'See lo'
limit 999999999
;
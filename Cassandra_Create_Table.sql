CREATE TABLE mykeyspace.ecommerce_log (
	user_id int,
	user_session varchar,
	event_date varchar,
	event_time timestamp,
	event_type varchar,
	product_id int,
	category_id bigint,
	category_code varchar,
	brand varchar,
	price double,
	PRIMARY KEY ((user_id, event_date), event_time)
);
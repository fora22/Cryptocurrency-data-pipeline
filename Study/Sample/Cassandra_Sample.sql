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

INSERT INTO mykeyspace.eCommerce_log 
(user_log_id, user_log_session, event_log_date_boundery, event_log_time, event_log_type, product_log_id, category_log_id, category_log_code, brand, price)
VALUES
();

CREATE TABLE mykeyspace.sample(
	user int,
	log_date varchar,
	val varchar,
	tt timestamp,
	PRIMARY KEY (user)
);

INSERT INTO mykeyspace.sample (user, log_date, val, tt)
VALUES (1, '20220330', '01', '2019-11-01 00:00:41');
INSERT INTO mykeyspace.sample (user, log_date, val, tt)
VALUES (2, '20220330', '02', '2019-11-01 00:00:44');
INSERT INTO mykeyspace.sample (user, log_date, val, tt)
VALUES (2, '20220331', '03', '2019-11-01 00:00:45');
INSERT INTO mykeyspace.sample (user, log_date, val, tt)
VALUES (3, '20220331', '04', '2019-11-01 00:00:44');

INSERT INTO mykeyspace.sample2 (user, log_date, val, tt)
VALUES (1, '20220330', '01', '2019-11-01 00:00:41');
INSERT INTO mykeyspace.sample2 (user, log_date, val, tt)
VALUES (2, '20220330', '02', '2019-11-01 00:00:44');
INSERT INTO mykeyspace.sample2 (user, log_date, val, tt)
VALUES (2, '20220331', '03', '2019-11-01 00:00:45');
INSERT INTO mykeyspace.sample2 (user, log_date, val, tt)
VALUES (3, '20220331', '04', '2019-11-01 00:00:44');
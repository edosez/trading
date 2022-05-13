-- Looking at tables

select * from directa.daily_options;
select * from directa.open_position_options opo ;
select * from directa.greeks_options go2 ;

select 
	* 
from directa.daily_options 
where 
	MOD(strike, 50) = 0 and 
	option_type  = 'P'and 
	strike>=2800 and 
	strike<=4150 and insert_date in (select max(insert_date) from directa.daily_options do );

-- Creating table merging daily option price and portfolio, setting expiration_date of interest
DROP TEMPORARY TABLE IF EXISTS tempdb.tmp_options;
create temporary table if not exists tempdb.tmp_options
select 
	a.median_price
	, a.insert_date 
	, a.delta
	, b.*
from directa.daily_options a 
inner join directa.open_position_options b 
on a.strike = b.strike and a.option_type = b.option_type 
where a.expiration_date = 'GIU22';

-- Looking at table
select * from tempdb.tmp_options ;

-- Calculating delta depending on different trading dates
select 
	insert_date as trade_date
	, SUM(delta) as delta
FROM tempdb.tmp_options GROUP BY 1;

-- Having a look at most importand cols
SELECT 
	median_price 
	, option_type 
	, insert_date
	, strike
from tempdb.tmp_options 
order by option_type, strike, insert_date; 

-- Estimating current position value using calendar data 1-3-x months ahead
select * from directa.calendar_options co ;
select * from directa.open_position_options opo ;

select SUM(gain_loss_abs) from directa.open_position_options opo ;

-- Creating table merging open positions and calendar options so to understand how much gain can provide the position at different point in times, 
-- supposing market prices will keep constant in future
DROP TEMPORARY TABLE IF EXISTS tempdb.options_estimation;
CREATE TEMPORARY TABLE IF NOT EXISTS tempdb.options_estimation 
SELECT 
	a.price as purchase_price
	, b.price as price_expiration_date
	, (a.price-b.price)*(-qty)*10 as profit 
	, a.qty
	, a.strike 
	, a.option_type 
	, a.purchase_date 
	, a.expiration_date as purchase_expiration_date
	, b.expiration_date 
	, CURRENT_DATE() AS today_date
from directa.open_position_options a
	inner join directa.calendar_options b 
		on a.strike=b.strike and a.option_type=b.option_type ;
	
SELECT * FROM tempdb.options_estimation ;
	
-- Profit by granular split
SELECT 
	expiration_date
	, strike
	, option_type
	, qty
	, purchase_price 
	, price_expiration_date 
	, sum(profit) as profit
FROM tempdb.options_estimation 
-- WHERE expiration_date = '2022-03-18'
GROUP BY 1, 2, 3, 4, 5, 6;

-- Profit by expiration_date
SELECT 
	expiration_date
	, purchase_date
	, TIMESTAMPDIFF(day, today_date, expiration_date) AS days_to_expiration 
	, sum(profit) as profit
FROM tempdb.options_estimation 
-- WHERE expiration_date = '2022-03-18'
GROUP BY 1, 2, 3;

-- Analyzing greeks
SELECT 
	SUM(IV)/SUM(CASE WHEN IV IS NOT NULL THEN 1 ELSE 0 END)
FROM directa.greeks_options go2 
where MONTH(expiration_date) = 6 and update_time in (SELECT MAX(update_time) from directa.greeks_options go3 ) ;

-- time decay
SELECT 	
	a.option_type 
	, a.strike 
	, a.median_price as last_median_price
	, a.insert_date as last_insert_date
	, b.median_price as first_median_price
	, b.insert_date as last_insert_date 
	, a.median_price - b.median_price as abs_diff_median_price
	, (a.median_price - b.median_price)/a.median_price  as perc_diff_median_price
FROM (
	SELECT
		*
	FROM directa.daily_options
	WHERE insert_date in (SELECT max(insert_date) FROM directa.daily_options WHERE expiration_date='GIU22')) as a
LEFT JOIN (
	SELECT
		*
	FROM directa.daily_options
	WHERE insert_date in (SELECT min(insert_date) FROM directa.daily_options WHERE expiration_date='GIU22')) as b
	on a.strike=b.strike and a.option_type = b.option_type;


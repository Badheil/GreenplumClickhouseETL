-- DROP FUNCTION std10_44_1.f_load_mart_total(date, interval);

CREATE OR REPLACE FUNCTION std10_44_1.f_load_mart_total(p_date date, p_interval interval DEFAULT '2 mons'::interval)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
    v_name text;
begin 

    execute 'drop table if exists std10_44_1.f_mart_total_' || to_char(p_date,'YYYYMMDD');

    execute 'create table std10_44_1.f_mart_total_' || to_char(p_date,'YYYYMMDD')|| ' with( appendonly = true,
        orientation = column,
        compresstype = zstd,
        compresslevel = 1)
    as (
--данные из объединения bills_item и bills_head
with all_bills as 
(	
	SELECT bh.plant, 
	       SUM(bi.rpa_sat) AS turnover, 
	       SUM(bi.qty) AS quantity, 
	       COUNT(DISTINCT bh.billnum) AS total_bills
	FROM std10_44_1.bills_item bi
	JOIN std10_44_1.bills_head bh on bi.billnum = bh.billnum
	WHERE bi.calday >= '''||p_date - p_interval||''' AND bi.calday < '''||p_date||''' 
	and bh.calday >= '''||p_date - p_interval||''' AND bh.calday < '''||p_date||''' 
	GROUP BY bh.plant
),

-- данные из coupons
all_coupons as
(
	select plant, sum(
			CASE 
	        WHEN p.promo_type = ''001'' THEN p.discount 
	        WHEN p.promo_type = ''002'' THEN tmp.price / 100 * p.discount
	        ELSE NULL 
	    end) AS total_discount, count(1) as total_coupons
	from std10_44_1.coupons coup
	join (select  distinct billnum, material, calday, rpa_sat / qty as price
			from std10_44_1.bills_item 
			WHERE calday >= '''||p_date - p_interval||''' AND calday < '''||p_date||'''
			) tmp
	on coup.billnum = tmp.billnum and coup.material = tmp.material
	join std10_44_1.promos p 
	on p.promo_id = coup.promo_id
	WHERE coup.calday >= '''||p_date - p_interval||''' AND coup.calday < '''||p_date||'''
	group by coup.plant
),

-- данные из traffic
all_traffic as 
(	
	select plant, sum(quantity) as total_traffic
	from std10_44_1.traffic
	WHERE date >= '''||p_date - p_interval||''' AND date <'''||p_date||'''
	group by plant
)
        
        -- data mart
       select ab.plant,
			  ab.turnover,
			  ac.total_discount,
			  ab.turnover - ac.total_discount as net_turnover,
			  ab.quantity,
			  ab.total_bills, 
			  at.total_traffic,
			  ac.total_coupons,
			  ROUND(ac.total_coupons/ab.quantity*100,1) as "Share of discounted items",
			  ROUND(ab.quantity/ab.total_bills, 2) as "Average number of items in a receipt",
			  ROUND(ab.total_bills::numeric/at.total_traffic::numeric*100, 2) as "Store conversion rate",
			  ROUND(ab.turnover / ab.total_bills, 1) as "Average check", 
			  case 
				  when at.total_traffic is null or at.total_traffic = 0 then 0
				  else ROUND(ab.turnover / at.total_traffic, 1)
			  end as "turnover per visitor"
	from all_traffic at join all_bills ab 
	ON at.plant = ab.plant
	join all_coupons ac on ac.plant=at.plant
)

DISTRIBUTED RANDOMLY;';

end;

$$
EXECUTE ON ANY;

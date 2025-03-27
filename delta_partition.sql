CREATE OR REPLACE FUNCTION std10_44_1.f_load_delta_partitions1(
  p_tablename text,
  p_ext_tablename text,
  partition_key text,
  start_date date,
  end_date date,
  dateformat text DEFAULT NULL::text
)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare 
	cnt int;
	v_cnt_tmp int;
	v_start_date date;
	v_end_date date;
	v_temp_date date;
	v_interval interval;
	
begin
	--создаем интервал для партиции
	v_interval = '1 month'::interval;
	
	v_start_date = date_trunc('month', start_date);
	v_end_date = date_trunc('month', end_date) + v_interval;	
	cnt = 0;

	-- добавляем партиции
	perform std10_44_1.f_create_date_partitions(p_tablename,v_end_date);

	-- загружаем партицию для заданного интервала
	loop
		v_temp_date = v_start_date + v_interval;
		exit when v_temp_date > v_end_date;
		v_cnt_tmp =  std10_44_1.f_load_partition(p_tablename, p_ext_tablename,v_start_date, partition_key, dateformat);
		
		cnt = cnt + v_cnt_tmp;
		v_start_date = v_temp_date;
	end loop;
return cnt;
end;

$$
EXECUTE ON ANY;

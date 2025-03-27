-- DROP FUNCTION std10_44_1.f_create_date_partitions(text, date);

CREATE OR REPLACE FUNCTION std10_44_1.f_create_date_partitions(p_tablename text, end_date date)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
  
declare

	v_end_date_txt text;
	v_end_date date;
	v_interval interval; 
	
begin

	v_interval := '1 month'::interval;

	loop
		-- находим последнюю паритцию которая есть
		select max(partitionrangeend) into v_end_date_txt
		from pg_catalog.pg_partitions
		where schemaname||'.'||tablename = p_tablename;
		
		execute 'select '||v_end_date_txt into v_end_date;
	
		raise notice '%', v_end_date;
		-- завершаем когда партиция превышает конечную дату(нет смысла добавлять ещё партицию)
		exit when v_end_date >= end_date;
		-- если последняя дата больше текущей партиции, тогда дефолтную партицию разбивают на дополнительные до конечной даты
		execute 'alter table '||p_tablename||' split default partition
				start ('''||v_end_date||''') end ('''||v_end_date+v_interval||''')';
		
	end loop;
end;

$$
EXECUTE ON ANY;

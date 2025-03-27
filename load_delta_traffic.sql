CREATE OR REPLACE FUNCTION std10_44_1.f_load_delta_traffic(
  p_tablename text,
  p_ext_tablename text,
  partition_key text,
  start_date timestamp,
  end_date timestamp,
  p_user_id text DEFAULT 'intern'::text,
  p_pass text DEFAULT 'intern'::text
)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    v_ext_table text;
    v_temp_table text; 
    v_sql text;
    v_pxf text;
    v_result int;
    v_dist_key text; 
    v_params text;
    v_load_interval interval := '1 month'::INTERVAL;
    v_start_date date;
    v_table_oid int4;
    v_cnt int8;

BEGIN

    v_ext_table := p_tablename || '_ext';
    v_temp_table := p_tablename || '_tmp';

    SELECT c.oid
    INTO v_table_oid
    FROM pg_class AS c 
    INNER JOIN pg_namespace AS n ON c.relnamespace=n.oid
    WHERE n.nspname || '.' || c.relname = p_tablename
    LIMIT 1;
    
    IF v_table_oid IS NULL THEN
        v_dist_key := 'DISTRIBUTED RANDOMLY';
    ELSE
        v_dist_key := pg_get_table_distributedby(v_table_oid); 
    END IF;    
    
    SELECT COALESCE('WITH (' || ARRAY_TO_STRING(reloptions, ', ') || ')', '')
    INTO v_params
    FROM pg_class
    WHERE oid = p_tablename::REGCLASS;
    
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;
			

    v_start_date := DATE_TRUNC('month', start_date);
	v_pxf = 'pxf://gp.traffic?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://*host-port*/postgres&USER=*login*&PASS=*password*';
    WHILE v_start_date < end_date LOOP
        -- Устанавливаем диапазон для текущей партиции
		v_sql = 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table || ';
			CREATE EXTERNAL TABLE ' || v_ext_table || ' (
		    	plant bpchar,
				date bpchar,
				time bpchar,
				frame_id bpchar,
				quantity int4
		)
		LOCATION (''' || v_pxf || ''')
		ON ALL
		FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'') 
		ENCODING ''UTF8''';
        RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;
        EXECUTE v_sql;

        -- Создаем временную таблицу
        v_sql := 'DROP TABLE IF EXISTS ' || v_temp_table || ';
                  CREATE TABLE ' || v_temp_table || ' (LIKE ' || v_ext_table || ') ' || v_params || ' ' || v_dist_key || ';';
        RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
        EXECUTE v_sql;

        -- Загружаем данные во временную таблицу
        v_sql := 'INSERT INTO ' || v_temp_table || ' SELECT * FROM ' || v_ext_table || 
                 ' WHERE TO_DATE(' || partition_key || ', ''DD.MM.YYYY'') >= ''' || v_start_date || ''' AND TO_DATE(' || partition_key || ', ''DD.MM.YYYY'') < ''' || (v_start_date + v_load_interval) || '''';
        EXECUTE v_sql;
        GET DIAGNOSTICS v_cnt = ROW_COUNT;
        RAISE NOTICE 'INSERTED ROWS: %', v_cnt;

        -- Изменяем тип колонки "date" во временной таблице
        v_sql := 'ALTER TABLE ' || v_temp_table || ' ALTER COLUMN "date" TYPE DATE USING TO_DATE("date", ''DD.MM.YYYY'')';
        EXECUTE v_sql;

        -- Обмен партиций
        v_sql := 'ALTER TABLE ' || p_tablename || ' EXCHANGE PARTITION FOR (DATE ''' || v_start_date || ''') WITH TABLE ' || v_temp_table || ' WITH VALIDATION'; 
        RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql;
        EXECUTE v_sql;

        -- Считаем количество вставленных строк
        EXECUTE 'SELECT COUNT(1) FROM ' || p_tablename || 
                ' WHERE ' || partition_key || '::date >= ''' || v_start_date || ''' 
                AND ' || partition_key || '::date < ''' || (v_start_date + v_load_interval) || ''''
        INTO v_result;

        -- Переходим к следующему месяцу
        v_start_date := v_start_date + v_load_interval;
    END LOOP;

    RETURN v_result;
END;

$$
EXECUTE ON ANY;

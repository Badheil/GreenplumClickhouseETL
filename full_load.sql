CREATE OR REPLACE FUNCTION std10_44_1.f_full_load(p_table varchar, p_file_name varchar)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
    v_table_from text;
    v_gpfdist text;
    v_sql text;
    v_cnt int8;
BEGIN
    v_table_from := p_table || '_ext';
	-- очищаем таблицу целевую и удаляем внешнюю таблицу
    EXECUTE format('TRUNCATE TABLE std10_44_1.%I', p_table);
 
    EXECUTE format('DROP EXTERNAL TABLE IF EXISTS %I', v_table_from);
	-- адрес файла из которого будем записывать

    v_gpfdist := 'gpfdist://***/' || p_file_name || '.csv';
	
    v_sql := format(
        'CREATE EXTERNAL TABLE %I (LIKE std10_44_1.%I)
        LOCATION (''%s'') ON ALL
        FORMAT ''CSV'' ( DELIMITER '','' NULL '''' ESCAPE ''"'' QUOTE ''"'' HEADER)
        ENCODING ''UTF8''',
        v_table_from, p_table, v_gpfdist
    );

    RAISE NOTICE 'External table SQL: %', v_sql;

    EXECUTE v_sql;

    EXECUTE format('INSERT INTO std10_44_1.%I SELECT * FROM std10_44_1.%I', p_table, v_table_from);

    EXECUTE format('SELECT COUNT(*) FROM std10_44_1.%I', p_table) INTO v_cnt;

    RETURN v_cnt;
END;
$$
EXECUTE ON ANY;

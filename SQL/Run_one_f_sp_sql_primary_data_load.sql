/*
  Скрипт выполнения запроса из таблицы f_sp_sql_primary_data_load
    для заливки первичных данных
*/

-- Инициализация serveroutput
set serveroutput on

-- Объявление переменных
declare

    -- Наименование запроса
    sql_nm f_sp_sql_primary_data_load.sql_name%type := '';
    
    -- Запрос
    str varchar(32767);

    -- Источники данных
    id_data_source number;
    id_data_source_aux number;
    
    -- Дата среза
    version_date date := trunc(sysdate, 'dd');
    
    -- Время старта
    start_dt timestamp with time zone;
    
    -- Кол-во обработанных строк
    v_row_counter number;
    
begin
    
    -- Инициализация DBMS_OUTPUT
    dbms_output.enable(buffer_size => null);
    
    -- Инициализация времени старта
    start_dt := systimestamp;
    
    -- Вывод в консоль
    dbms_output.put_line('Job started at '
      || to_char(start_dt, 'DD-MM-YYYY HH24:MI:SS.FF3'));
    dbms_output.new_line;
    dbms_output.put('Status: ');

    -- Получение запроса
    select sql_text, id_data_source, id_data_source_aux
        into str, id_data_source, id_data_source_aux
        from f_sp_sql_primary_data_load
        where sql_name = sql_nm;
    
    -- Тестовое выполнение запроса
    execute immediate str using id_data_source, id_data_source_aux, version_date, out v_row_counter;
    
    -- Подтверждение транзакции
    commit;
    
    -- Вывод в консоль
    dbms_output.put_line('OK');
    dbms_output.new_line;
    dbms_output.put_line('SQL%ROWCOUNT = ' || v_row_counter);
    dbms_output.new_line;
    dbms_output.put_line('Job finished at '
      || to_char(systimestamp, 'DD-MM-YYYY HH24:MI:SS.FF3'));
    dbms_output.new_line;
    dbms_output.put_line('--');
    dbms_output.new_line;
    dbms_output.put_line('Total elapsed time is '
      || (systimestamp - start_dt));
    
exception
    
    when others then begin
        
        -- Откат транзакции
        rollback;

        -- Вывод в консоль
        dbms_output.put_line('FAIL!');
        dbms_output.new_line;
        dbms_output.put_line('Error: [' || sqlerrm || ']');
        dbms_output.new_line;
        dbms_output.put_line('Job finished at '
          || to_char(systimestamp, 'DD-MM-YYYY HH24:MI:SS.FF3'));
        dbms_output.new_line;
        dbms_output.put_line('--');
        dbms_output.new_line;
        dbms_output.put_line('Total elapsed time is '
          || (systimestamp - start_dt));
    end;
    
end;
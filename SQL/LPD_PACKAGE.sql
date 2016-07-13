/*
  Пакет загрузки первичных данных посредством запуска
    SQL-запросов из таблицы f_sp_sql_primary_data_load
*/

create or replace package LPD is

    -- Тип массива для SQL-запросов
    type
        sql_array_type is table of f_sp_sql_primary_data_load%rowtype
        index by binary_integer;

    -- Массив для SQL-запросов
    sql_array sql_array_type;

    -- Дата среза
    version_date date := trunc(sysdate, 'dd');

    -- Код SQL ошибки
    err_code number;

    -- Текст SQL ошибки
    err_message varchar2(4000);
    
    -- Флаг режима отладки
    debug_mode boolean := false;
    
    -- Префикс для режима отладки
    debug_prefix varchar2(10) := '';
    
    /* Объявление процедуры записи в лог (таблицу t_error_log) */
    procedure WRITE_LOG(
        p_error_num number := null,
        p_error_message	varchar2 := null,
        p_name_object	varchar2 := null,
        p_description varchar2 := null,
        p_id_data_source number := null,
        p_operation_type number := null,
        p_row_count number := null,
        p_date_start timestamp with local time zone := null,
        p_date_finish	timestamp with local time zone := null,
        p_id_data_source_aux	number := null,
        p_iteration	number := null,
        p_version_date date := null
    );
    
    /* Объявление функции выполнения запроса */
    function EXECUTE_SQL(
        p_sql_block varchar2,
        p_sql_array_record f_sp_sql_primary_data_load%rowtype,
        p_iteration_i number
    ) return number;
    
    /* Объявление функции загрузки первичных данных */
    function LOAD_PRIMARY_DATA(
        p_start_execute_order number := 0
    ) return number;

end LPD;
/

-- Тело пакета LPD
create or replace package body LPD is

    /* Процедура записи в лог (таблицу t_error_log) */
    procedure WRITE_LOG(
        p_error_num number := null,
        p_error_message	varchar2 := null,
        p_name_object	varchar2 := null,
        p_description varchar2 := null,
        p_id_data_source number := null,
        p_operation_type number := null,
        p_row_count number := null,
        p_date_start timestamp with local time zone := null,
        p_date_finish	timestamp with local time zone := null,
        p_id_data_source_aux	number := null,
        p_iteration	number := null,
        p_version_date date := null
    ) is

    begin
        
        -- Вставка записи в таблицу t_error_log
        insert into t_error_log(error_num,
                                error_message,
                                name_object,
                                description,
                                id_data_source,
                                operation_type,
                                row_count,
                                date_start,
                                date_finish,
                                id_data_source_aux,
                                iteration,
                                version_date)
            values (p_error_num,
                    p_error_message,
                    debug_prefix || p_name_object,
                    p_description,
                    p_id_data_source,
                    p_operation_type,
                    p_row_count,
                    p_date_start,
                    p_date_finish,
                    p_id_data_source_aux,
                    p_iteration,
                    p_version_date);

        -- Подтверждение транзакции
        commit;
        
    exception
        
        when others then begin
            
            -- Откат транзакции
            rollback;
            
            -- Сохранение кода SQL ошибки
            err_code := sqlcode;

            -- Сохранение текста SQL ошибки
            err_message := sqlerrm;

            -- Вывод в консоль
            dbms_output.put_line('Error occured while inserting to t_error_log:');
            dbms_output.put_line('  Error code: [' || err_code || ']');
            dbms_output.put_line('  Error message: [' || err_message || ']');
            
        end;
    
    end WRITE_LOG;

    /* Функция выполнения запроса */
    function EXECUTE_SQL(
        p_sql_block varchar2,
        p_sql_array_record f_sp_sql_primary_data_load%rowtype,
        p_iteration_i number
    ) return number is
    
        -- Результат функции
        l_res number := 0;
    
        -- Кол-во обработанных строк
        l_row_counter number := 0;

        -- Время старта запроса
        l_sql_start_date timestamp(6) with local time zone;
    
        -- Время окончания запроса
        l_sql_finish_date timestamp(6) with local time zone;

    begin
        
        -- Вывод в консоль
        dbms_output.put('Executing '
          || p_sql_array_record.sql_name || ' (iteration #'
          || p_iteration_i || ')... ');
                    
        -- Инициализация времени старта запроса
        l_sql_start_date := systimestamp;
                    
        -- Выполнение запроса
        execute immediate p_sql_block
            using p_sql_array_record.id_data_source,
                  p_sql_array_record.id_data_source_aux,
                  version_date,
                  out l_row_counter;
                    
        -- Подтверждение транзакции
        if debug_mode then
            rollback;
        else
            commit;
        end if;
    
        -- Инициализация времени окончания запроса
        l_sql_finish_date := systimestamp;
                    
        -- Вывод в консоль
        dbms_output.put_line('OK (ROW_COUNT = ' || l_row_counter
          || ', TIME = ' || to_char(l_sql_finish_date - l_sql_start_date) || ')');
                    
        -- Установка текста err_message
        if l_row_counter = 0 then
            err_message := '0 rows inserted';
        else
            err_message := '';
        end if;
                    
        -- Запись в лог
        WRITE_LOG(p_error_message => err_message,
                  p_name_object => p_sql_array_record.sql_name,
                  p_description => 'Загрузка данных успешно завершена',
                  p_id_data_source => p_sql_array_record.id_data_source,
                  p_id_data_source_aux => p_sql_array_record.id_data_source_aux,
                  p_row_count => l_row_counter,
                  p_date_start => l_sql_start_date,
                  p_date_finish => l_sql_finish_date,
                  p_operation_type => 1,
                  p_iteration => p_iteration_i,
                  p_version_date => version_date);

        -- Возврат результата
        return l_res;
        
    exception
        
        when others then begin
            
            -- Откат транзакции
            rollback;
                
            -- Инициализация времени окончания запроса
            l_sql_finish_date := systimestamp;
                                    
            -- Сохранение текста SQL ошибки
            err_message := sqlerrm;
                                    
            -- Сохранение кода SQL ошибки
            err_code := sqlcode;
                                    
            -- Вывод в консоль
            dbms_output.put_line('FAILED!');
            dbms_output.put_line('  Error: [' || err_message || ']');
            dbms_output.put_line('  Code: [' || err_code || ']');
                                    
            -- Запись в лог
            WRITE_LOG(p_error_num => err_code,
                      p_error_message => err_message,
                      p_name_object => p_sql_array_record.sql_name,
                      p_id_data_source => p_sql_array_record.id_data_source,
                      p_id_data_source_aux => p_sql_array_record.id_data_source_aux,
                      p_date_start => l_sql_start_date,
                      p_date_finish => l_sql_finish_date,
                      p_operation_type => 1,
                      p_iteration => p_iteration_i,
                      p_version_date => version_date);
        
            -- Установка результата
            l_res := err_code;
            
            -- Возврат результата
            return l_res;
            
        end;

    end EXECUTE_SQL;
    
    /* Функция загрузки первичных данных */
    function LOAD_PRIMARY_DATA(
        p_start_execute_order number := 0
    ) return number is
        
        -- Результат функции
        l_res number := 0;
        
        -- Результат основного запроса
        l_res_prm number;
        
        -- Результат альтернативного запроса
        l_res_alt number;
        
        -- Кол-во повторений загрузки
        
        l_iteration_count number := 3;
        
        -- Номер последней итерации
        l_iteration_num number := 0;
        
        -- Счетчик выполненных запросов
        l_ok_count number := 0;
        
        -- Счетчик ошибок
        l_err_count number := 0;
        
        -- Время старта задачи
        l_start_dt timestamp with time zone;
        
        -- Индекс массива
        l_idx binary_integer := 1;
        
        -- Начальное значение порядка выполнения
        l_start_execute_order number := p_start_execute_order;
        
    begin
    
        -- Инициализация DBMS_OUTPUT
        dbms_output.enable(buffer_size => null);
        
        -- Инициализация времени старта
        l_start_dt := systimestamp;
        
        -- Вывод в консоль
        dbms_output.put_line('Job started at '
          || to_char(l_start_dt, 'DD-MM-YYYY HH24:MI:SS.FF3'));
        dbms_output.new_line;
    
        -- Запись в лог
        WRITE_LOG(p_name_object => 'LPD.LOAD_PRIMARY_DATA',
                  p_description => 'Загрузка первичных данных (начало)',
                  p_version_date => version_date);

        -- Загрузка массива для SQL-запросов
        for i in (
        
            select *
                from f_sp_sql_primary_data_load
                where is_actual = 1 and execute_order >= l_start_execute_order
                order by execute_order
        
        ) loop
            sql_array(l_idx).sql_name := i.sql_name;
            sql_array(l_idx).sql_text := i.sql_text;
            sql_array(l_idx).sql_text_alternative := i.sql_text_alternative;
            sql_array(l_idx).id_data_source := i.id_data_source;
            sql_array(l_idx).id_data_source_aux := i.id_data_source_aux;
            sql_array(l_idx).is_actual := i.is_actual;
            l_idx := l_idx + 1;
        end loop;
    
        -- Цикл повторений загрузок
        for i in 1..l_iteration_count loop
        
            -- Выход из цикла в случае пустого массива
            if sql_array.count = 0 then
                
                exit;
            
            end if;
            
            -- Проход по всем запросам
            for l_idx in sql_array.first..sql_array.last loop
            
                -- Обработка только актуального запроса
                if sql_array(l_idx).is_actual = 1 then

                    -- Выполнение запроса
                    l_res_prm := EXECUTE_SQL(sql_array(l_idx).sql_text, sql_array(l_idx), i);
                  
                    -- Разбор результата
                    if l_res_prm = 0 then
                            
                        -- Инкремент счетчика выполненных запросов
                        l_ok_count := l_ok_count + 1;
                        
                        -- Сброс флага актуальности запроса
                        sql_array(l_idx).is_actual := 0;
                            
                    elsif l_res_prm = -1 then
    
                        -- Инкремент счетчика ошибок
                        l_err_count := l_err_count + 1;
                        
                        -- Запись в лог
                        WRITE_LOG(p_name_object => sql_array(l_idx).sql_name,
                                  p_description => 'Выполнение альтернативного запроса (начало)',
                                  p_iteration => i,
                                  p_version_date => version_date);
    
                        -- Выполнение альтернативного запроса
                        l_res_alt := EXECUTE_SQL(sql_array(l_idx).sql_text_alternative, sql_array(l_idx), i);
                        
                        -- Запись в лог
                        WRITE_LOG(p_name_object => sql_array(l_idx).sql_name,
                                  p_description => 'Выполнение альтернативного запроса (конец)',
                                  p_iteration => i,
                                  p_version_date => version_date);
    
                        -- Разбор результата
                        if l_res_alt = 0 then
    
                            -- Инкремент счетчика выполненных запросов
                            l_ok_count := l_ok_count + 1;
                            
                            -- Сброс флага актуальности запроса
                            sql_array(l_idx).is_actual := 0;
    
                        else
            
                            -- Инкремент счетчика ошибок
                            l_err_count := l_err_count + 1;
            
                        end if;
                        
    
                    else
                            
                        -- Инкремент счетчика ошибок
                        l_err_count := l_err_count + 1;
                        
                    end if;
                    
                    -- Сохранение номера последней итерации
                    l_iteration_num := i;
                
                end if;
                    
            end loop;
        
        end loop;

        -- Установка результата
        l_res := l_iteration_num;
    
        -- Запись в лог
        WRITE_LOG(p_name_object => 'LPD.LOAD_PRIMARY_DATA',
                  p_description => 'Загрузка первичных данных (конец)',
                  p_version_date => version_date);

        -- Вывод в консоль
        dbms_output.new_line;
        dbms_output.put_line('Total SQL-blocks executed: ' || l_ok_count);
        dbms_output.new_line;
        dbms_output.put_line('Total errors: ' || l_err_count);
        dbms_output.new_line;
        dbms_output.put_line('Job finished at '
          || to_char(systimestamp, 'DD-MM-YYYY HH24:MI:SS.FF3'));
        dbms_output.new_line;
        dbms_output.put_line('--');
        dbms_output.new_line;
        dbms_output.put_line('Total elapsed time is '
          || (systimestamp - l_start_dt));
          
        -- Возврат результата
        return l_res;
    
    exception
        
        when others then begin
            
            -- Установка результата
            l_res := -1;
        
            -- Сохранение текста SQL ошибки
            err_message := sqlerrm;
                                    
            -- Сохранение кода SQL ошибки
            err_code := sqlcode;
                                    
            -- Запись в лог
            WRITE_LOG(p_error_num => err_code,
                      p_error_message => err_message,
                      p_name_object => 'LPD.LOAD_PRIMARY_DATA',
                      p_description => 'Загрузка первичных данных (failed!)',
                      p_operation_type => 0,
                      p_version_date => version_date);

            -- Вывод в консоль
            dbms_output.put_line('Error occurred while running script!');
            dbms_output.put_line('  Error: [' || err_message || ']');
            dbms_output.put_line('  Code: [' || err_code || ']');
            dbms_output.new_line;
            dbms_output.put_line('Total SQL-blocks executed: ' || l_ok_count);
            dbms_output.new_line;
            dbms_output.put_line('Total errors: ' || l_err_count);
            dbms_output.new_line;
            dbms_output.put_line('Job aborted at '
              || to_char(systimestamp, 'DD-MM-YYYY HH24:MI:SS.FF3'));
            dbms_output.new_line;
            dbms_output.put_line('--');
            dbms_output.new_line;
            dbms_output.put_line('Total elapsed time is '
              || (systimestamp - l_start_dt));
              
            -- Возврат результата
            return l_res;
            
        end;
        
    end LOAD_PRIMARY_DATA;

end LPD;
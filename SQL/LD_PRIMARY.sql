/* Процедура инициации загрузки первичных данных */

create or replace procedure LD_PRIMARY (
    v_version_date in date default trunc(sysdate, 'dd')
) is 

    res number;
    v_res1 number;
    v_res2 number;
    v_res4 number;
    s_date date;
    e_date date;
    v_errNumber number;
    v_errMessage varchar2(4000);

begin
    
    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_PRIMARY',
                  p_description => 'LD_PRIMARY (начало)',
                  p_version_date => v_version_date);

    s_date := systimestamp;
    
    -- Загрузка первичных данных
    res := LPD.LOAD_PRIMARY_DATA;

    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_PRIMARY',
                  p_description => 'LD_PRIMARY (проверка успешности загрузки)',
                  p_version_date => v_version_date);

    -- Проверка на успешность загрузки первичных данных
    if res = -1 then

        v_res1 := 1;
        v_res2 := 1;
        v_res4 := 1;
    
    else

        -- Расчет v_res1
        select count(*)
            into v_res1
            from t_error_log
            where trunc(date_rec, 'dd') = trunc(sysdate, 'dd')
                    and error_num is not null
                    and id_data_source = 1
                    and operation_type = 1
                    and iteration = res;
    
        -- Расчет v_res2
        select count(*)
            into v_res2
            from t_error_log
            where trunc(date_rec, 'dd') = trunc(sysdate, 'dd')
                    and error_num is not null
                    and id_data_source = 2
                    and operation_type = 1
                    and iteration = res;
    
        -- Расчет v_res4
        select count(*)
            into v_res4
            from t_error_log
            where trunc(date_rec, 'dd') = trunc(sysdate, 'dd')
                    and error_num is not null
                    and id_data_source = 4
                    and operation_type = 1
                    and iteration = res;
    
    end if;

    e_date := systimestamp;
    
    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_PRIMARY',
                  p_description => 'LD_PRIMARY (вставка в log_versions)',
                  p_version_date => v_version_date);

    -- Вставка в log_versions
    insert into LOG_VERSIONS (VERSION_DATE,
                              IS_ACTUAL,
                              d_start,
                              d_end,
                              DATA_SOURCE1,
                              DATA_SOURCE4,                                                                    
                              DATA_REPORTS,
                              DATA_SOURCE2,
                              DATA_REPORTS2,
                              DATA_REPORTS_PP) 
        values (sysdate,
                0,
                s_date,
                e_date,
                case when v_res1 = 0 then 1 else 0 end,
                case when v_res4 = 0 then 1 else 0 end,
                1,
                case when v_res2 = 0 then 1 else 0 end,
                1,
                null);        
    
    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_PRIMARY',
                  p_description => 'LD_PRIMARY (сбор статистики - начало)',
                  p_version_date => v_version_date);

    -- Сбор статистики по первичным данным
    gather_stats('FIRST');
    
    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_PRIMARY',
                  p_description => 'LD_PRIMARY (сбор статистики - конец)',
                  p_version_date => v_version_date);

    -- Подтверждение транзакции
    commit;

    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_PRIMARY',
                  p_description => 'LD_PRIMARY (конец)',
                  p_version_date => v_version_date);

exception
    
    when others then begin
        
        v_errNumber := SQLCODE;
        
        v_errMessage := SQLERRM;
        
        -- Запись в лог
        LPD.WRITE_LOG(p_error_num => v_errNumber,
                      p_error_message => v_errMessage,
                      p_description => 'LD_PRIMARY (failed!)',
                      p_name_object => 'LD_PRIMARY',
                      p_version_date => v_version_date);

    end;

end LD_PRIMARY;
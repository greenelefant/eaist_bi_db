/* Процедура инициации сборки новых кубов */

create or replace procedure LD_NEW_CUBES (
    v_version_date in date default trunc(sysdate,'dd')
) is 

    v_c1l number;
    v_partition_name varchar2(30);
    v_errNumber number;
    v_errMessage varchar2(4000);
    
begin
    
    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_NEW_CUBES',
                  p_description => 'LD_NEW_CUBES (начало)',
                  p_version_date => v_version_date);

    -- Статистика для кубов 1 типа
    for i in (
        select table_name from all_tables where owner = 'REPORTS' and table_name in 
              ('T_PURCHASE_SHEDULE', 'SP_CUSTOMER', 'SP_STATUS', 'T_PURCHASE_DETAILED', 'T_PURCHASE_DETAILED_LIMIT',
              'T_LOT', 'T_LOT_SPECIFICATION', 'T_TENDER', 'T_LOT_MEMBER', 'SP_ORGANIZATION', 'LNK_CUSTOMERS_UNITED',
              'LNK_CONTRACT_LOT', 'T_CONTRACT', 'T_MEMBER_BID', 'SP_METHOD', 'LNK_CUSTOMERS_ALL_LEVEL', 'SP_CUSTOMERS_TREE',
              'SP_SOURCE_FINANCE', 'SP_GRBS', 'SP_SECTION_BUDGET', 'SP_TARGET_CLAUSE', 'SP_TYPE_EXPENSE', 'SP_KOSGU',
              'SP_ORGANIZATION_JOINT')
    ) loop   
        
        v_partition_name := get_last_partition(i.table_name);
        
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => '"REPORTS"',
            tabname => '"'||i.table_name||'"',
            partname => '"'||v_partition_name||'"',
            estimate_percent => dbms_stats.auto_sample_size,
            granularity => 'PARTITION'
        );
        
    end loop;

    -- Собираем кубы 1 типа
    for i in (
        select table_name from F_SP_CUBE_1LVL
    ) loop        
            
        v_c1l := N_UTILITS.LOAD_CUBE_1LVL(v_version_date, i.table_name);
                    
        v_partition_name := get_last_partition(i.table_name);
            
        DBMS_STATS.GATHER_TABLE_STATS (
            ownname => '"REPORTS"',
            tabname => '"'||i.table_name||'"',
            partname => '"'||v_partition_name||'"',
            estimate_percent => dbms_stats.auto_sample_size,
            granularity => 'PARTITION'
        );            
            
        -- Запись в лог
        LPD.WRITE_LOG(p_description => 'Сбор статистики окончен',
                      p_name_object => i.table_name,
                      p_version_date => v_version_date);

    end loop;

    -- Собираем кубы 2 типа
    for i in (
        select table_name from F_SP_CUBE_2LVL where table_name like 'CUBE_OPEN%' order by id
    ) loop
        
        N_UTILITS3.LOAD_CUBE_2LVL(v_version_date, i.table_name);
    
    end loop;    

    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_NEW_CUBES',
                  p_description => 'LD_NEW_CUBES (конец)',
                  p_version_date => v_version_date);

exception
    
    when others then begin
        
        v_errNumber := SQLCODE;
        
        v_errMessage := SQLERRM;
        
        -- Запись в лог
        LPD.WRITE_LOG(p_error_num => v_errNumber,
                      p_error_message => v_errMessage,
                      p_description => 'LD_NEW_CUBES (failed!)',
                      p_name_object => 'LD_NEW_CUBES',
                      p_version_date => v_version_date);

    end;

end LD_NEW_CUBES;
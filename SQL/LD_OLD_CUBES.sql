/* Процедура инициации сборки старых кубов */

create or replace procedure LD_OLD_CUBES (
    v_version_date in date default trunc(sysdate, 'dd')
) is 

    v_res_r1 number;
    v_res_r2 number;
    v_errNumber number;
    v_errMessage varchar2(4000);

begin
    
    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_OLD_CUBES',
                  p_description => 'LD_OLD_CUBES (начало)',
                  p_version_date => v_version_date);
    
    v_res_r2 := cube_agr_data2(2, v_version_date);
    v_res_r1 := cube_agr_data(1, v_version_date);
    
    insert_cube_portal_vender(1, v_version_date);
    insert_cube_portal_vender(2, v_version_date);  
    
    -- Запись в лог
    LPD.WRITE_LOG(p_name_object => 'LD_OLD_CUBES',
                  p_description => 'LD_OLD_CUBES (конец)',
                  p_version_date => v_version_date);

exception
    
    when others then begin
        
        v_errNumber := SQLCODE;
        
        v_errMessage := SQLERRM;
        
        -- Запись в лог
        LPD.WRITE_LOG(p_error_num => v_errNumber,
                      p_error_message => v_errMessage,
                      p_description => 'LD_OLD_CUBES (failed!)',
                      p_name_object => 'LD_OLD_CUBES',
                      p_version_date => v_version_date);
        
    end;

end LD_OLD_CUBES;
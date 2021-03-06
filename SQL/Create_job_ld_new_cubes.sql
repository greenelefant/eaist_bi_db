/* Процедура создания задания сборки новых кубов */

create or replace procedure CREATE_JOB_LD_NEW_CUBES
is 

begin
    DBMS_SCHEDULER.CREATE_JOB (
        job_name           =>  'JOB_LD_NEW_CUBES',
        job_type           =>  'PLSQL_BLOCK',
        job_action         =>  q'#BEGIN
                                      LPD.WRITE_LOG(p_name_object => 'JOB_LD_NEW_CUBES',
                                                    p_description => 'JOB_LD_NEW_CUBES (начало)');
                                        
                                      LD_NEW_CUBES;
                                        
                                      LPD.WRITE_LOG(p_name_object => 'JOB_LD_NEW_CUBES',
                                                    p_description => 'JOB_LD_NEW_CUBES (конец)');
                                  END;#',
        start_date         =>  to_timestamp_tz(to_char(sysdate + 1/1440, 'dd.mm.yyyy hh24:mi:ss') || ' +3:00', 'dd.mm.yyyy hh24:mi:ss tzh:tzm'),
        enabled            =>  true,
        auto_drop          =>  true);
end CREATE_JOB_LD_NEW_CUBES;

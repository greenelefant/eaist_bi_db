/* Процедура создания задания сборки старых кубов */

create or replace procedure CREATE_JOB_LD_OLD_CUBES
is 

begin
    DBMS_SCHEDULER.CREATE_JOB (
        job_name           =>  'JOB_LD_OLD_CUBES',
        job_type           =>  'PLSQL_BLOCK',
        job_action         =>  q'#BEGIN
                                      LPD.WRITE_LOG(p_name_object => 'JOB_LD_OLD_CUBES',
                                                    p_description => 'JOB_LD_OLD_CUBES (начало)');
                                        
                                      LD_OLD_CUBES;
                                      
                                      LPD.WRITE_LOG(p_name_object => 'JOB_LD_OLD_CUBES',
                                                    p_description => 'JOB_LD_OLD_CUBES (конец)');
                                  END;#',
        start_date         =>  to_timestamp_tz(to_char(sysdate + 1/1440, 'dd.mm.yyyy hh24:mi:ss') || ' +3:00', 'dd.mm.yyyy hh24:mi:ss tzh:tzm'),
        enabled            =>  true,
        auto_drop          =>  true);
end CREATE_JOB_LD_OLD_CUBES;

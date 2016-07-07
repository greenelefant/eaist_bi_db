/* Процедура создания задания загрузки первичных данных */

create or replace procedure CREATE_JOB_LD_PRIMARY
is 

begin
    DBMS_SCHEDULER.CREATE_JOB (
        job_name           =>  'JOB_LD_PRIMARY',
        job_type           =>  'PLSQL_BLOCK',
        job_action         =>  q'#BEGIN
                                      LPD.WRITE_LOG(p_name_object => 'JOB_LD_PRIMARY',
                                                    p_description => 'JOB_LD_PRIMARY (начало)');
                                        
                                      LD_PRIMARY;
                                        
                                      LPD.WRITE_LOG(p_name_object => 'JOB_LD_PRIMARY',
                                                    p_description => 'JOB_LD_PRIMARY (конец)');
                                  END;#',
        start_date         =>  to_timestamp_tz(to_char(sysdate + 1/1440, 'dd.mm.yyyy hh24:mi:ss') || ' +3:00', 'dd.mm.yyyy hh24:mi:ss tzh:tzm'),
        enabled            =>  true,
        auto_drop          =>  true);
end CREATE_JOB_LD_PRIMARY;

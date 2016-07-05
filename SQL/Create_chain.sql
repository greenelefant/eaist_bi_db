-- Выдача системной привилегии администратора на работу с ресурсными планами схеме REPORTS (запускать от DBA)
begin
    dbms_resource_manager_privs.grant_system_privilege(
        grantee_name => 'REPORTS',
        privilege_name => 'ADMINISTER_RESOURCE_MANAGER',
        admin_option => false);
end;
/

-- Выдача привилегий для работы с цепочками схеме REPORTS (запускать от DBA)
begin
    dbms_rule_adm.grant_system_privilege(dbms_rule_adm.create_rule_obj,
        'REPORTS');

    dbms_rule_adm.grant_system_privilege (dbms_rule_adm.create_rule_set_obj,
        'REPORTS');

    dbms_rule_adm.grant_system_privilege (dbms_rule_adm.create_evaluation_context_obj,
        'REPORTS');
end;
/

-- Очистка рабочей области
begin
    dbms_resource_manager.clear_pending_area();
end;
/

-- Создание рабочей области
begin
    dbms_resource_manager.create_pending_area();
end;
/

-- Создание ресурсного плана
begin
    dbms_resource_manager.create_plan('LD_DAILY_PLAN',
        'План ежесуточной загрузки данных');
end;
/

-- Удаление ресурсного плана
/*
begin
    dbms_resource_manager.delete_plan('LD_DAILY_PLAN');
end;
/
*/

-- Создание групп потребителей
begin
    dbms_resource_manager.create_consumer_group('LD_REPORTS_GROUP',
        'Группа потребителей для схемы REPORTS');
end;
/

-- Удаление групп потребителей
/*
begin
    dbms_resource_manager.delete_consumer_group('LD_REPORTS_GROUP');
end;
/
*/

-- Привязка групп потребителей к планам
begin
    dbms_resource_manager.create_plan_directive('LD_DAILY_PLAN',
        'LD_REPORTS_GROUP',
        'Определение ресурсов для ежесуточной загрузки данных',
        mgmt_p1 => 75);

    dbms_resource_manager.create_plan_directive('LD_DAILY_PLAN',
        'SYS_GROUP',
        'Определение ресурсов для администрирования при ежесуточной загрузке данных',
        mgmt_p1 => 20);
     
    dbms_resource_manager.create_plan_directive('LD_DAILY_PLAN',
        'OTHER_GROUPS',
        'Определение ресурсов для прочих задач при ежесуточной загрузке данных',
        mgmt_p1 => 5);
end;

-- Удаление групп потребителей из плана
/*
begin
    dbms_resource_manager.delete_plan_directive('LD_DAILY_PLAN',
        'LD_REPORTS_GROUP');

    dbms_resource_manager.delete_plan_directive('LD_DAILY_PLAN',
        'SYS_GROUP');

    dbms_resource_manager.delete_plan_directive('LD_DAILY_PLAN',
        'OTHER_GROUPS');
end;
/
*/

-- Проверка валидности рабочей области
begin
    dbms_resource_manager.validate_pending_area();
end;
/

-- Подтверждение создания рабочей области
begin
    dbms_resource_manager.submit_pending_area();
end;
/

-- Включение права вхождения в группу потребителей
begin
    dbms_resource_manager_privs.grant_switch_consumer_group('REPORTS',
        'LD_REPORTS_GROUP', false);

    dbms_resource_manager_privs.grant_switch_consumer_group('SYSTEM',
        'SYS_GROUP', false);

    dbms_resource_manager_privs.grant_switch_consumer_group('SYS',
        'SYS_GROUP', false);

    dbms_resource_manager_privs.grant_switch_consumer_group('BISYSTEM',
        'SYS_GROUP', false);
end;
/

-- Включение пользователей в группу потребителей
begin
    dbms_resource_manager.set_initial_consumer_group('REPORTS',
        'LD_REPORTS_GROUP');
    dbms_resource_manager.set_initial_consumer_group('SYSTEM',
        'SYS_GROUP');
    dbms_resource_manager.set_initial_consumer_group('SYS',
        'SYS_GROUP');
    dbms_resource_manager.set_initial_consumer_group('BISYSTEM',
        'SYS_GROUP');
end;

-- Инициализация программ
begin
    dbms_scheduler.create_program(
        program_name => 'LD_PROGRAM_PRIMARY_DATA',
        program_type => 'PLSQL_BLOCK',
        program_action => q'#
            BEGIN
                LPD.WRITE_LOG(p_name_object => 'LD_PROGRAM_PRIMARY_DATA',
                    p_description => 'LD_PROGRAM_PRIMARY_DATA (начало)');
                                      
                LD_PRIMARY;
                
                LPD.WRITE_LOG(p_name_object => 'LD_PROGRAM_PRIMARY_DATA',
                    p_description => 'LD_PROGRAM_PRIMARY_DATA (конец)');
            END;#',
        enabled => true
    );

    dbms_scheduler.create_program(
        program_name => 'LD_PROGRAM_OLD_CUBES',
        program_type => 'PLSQL_BLOCK',
        program_action => q'#
            BEGIN
                LPD.WRITE_LOG(p_name_object => 'LD_PROGRAM_OLD_CUBES',
                    p_description => 'LD_PROGRAM_OLD_CUBES (начало)');
                                      
                LD_OLD_CUBES;
                
                LPD.WRITE_LOG(p_name_object => 'LD_PROGRAM_OLD_CUBES',
                    p_description => 'LD_PROGRAM_OLD_CUBES (конец)');
            END;#',
        enabled => true
    );

    dbms_scheduler.create_program(
        program_name => 'LD_PROGRAM_NEW_CUBES',
        program_type => 'PLSQL_BLOCK',
        program_action => q'#
            BEGIN
                LPD.WRITE_LOG(p_name_object => 'LD_PROGRAM_NEW_CUBES',
                    p_description => 'LD_PROGRAM_NEW_CUBES (начало)');
                                      
                LD_NEW_CUBES;
                
                LPD.WRITE_LOG(p_name_object => 'LD_PROGRAM_NEW_CUBES',
                    p_description => 'LD_PROGRAM_NEW_CUBES (конец)');
            END;#',
        enabled => true
    );
end;
/

-- Удаление программ
/*
execute dbms_scheduler.drop_program('LD_PROGRAM_PRIMARY_DATA');
execute dbms_scheduler.drop_program('LD_PROGRAM_OLD_CUBES');
execute dbms_scheduler.drop_program('LD_PROGRAM_NEW_CUBES');
*/

-- Инициализация цепочки
begin
    dbms_scheduler.create_chain(
        chain_name => 'LD_CHAIN',
        rule_set_name => null,
        evaluation_interval => null,
        comments => 'Цепочка загрузки данных');
end;
/

-- Удаление цепочки
/*
begin
    dbms_scheduler.drop_chain (
        chain_name => 'LD_CHAIN',
        force => true);
end;
/
*/

-- Инициализация шагов цепочки
begin
    dbms_scheduler.define_chain_step(
        chain_name => 'LD_CHAIN',
        step_name => 'LD_STEP_PRIMARY_DATA',
        program_name => 'LD_PROGRAM_PRIMARY_DATA');

    dbms_scheduler.define_chain_step (
        chain_name => 'LD_CHAIN',
        step_name => 'LD_STEP_OLD_CUBES',
        program_name => 'LD_PROGRAM_OLD_CUBES');

    dbms_scheduler.define_chain_step (
        chain_name => 'LD_CHAIN',
        step_name => 'LD_STEP_NEW_CUBES',
        program_name => 'LD_PROGRAM_NEW_CUBES');
end;
/

-- Удаление шагов цепочки
/*
begin
    dbms_scheduler.drop_chain_step (
        chain_name => 'LD_CHAIN',
        step_name => 'LD_STEP_PRIMARY_DATA',
        force => true);

    dbms_scheduler.drop_chain_step (
        chain_name => 'LD_CHAIN',
        step_name => 'LD_STEP_OLD_CUBES',
        force => true);

    dbms_scheduler.drop_chain_step (
        chain_name => 'LD_CHAIN',
        step_name => 'LD_STEP_NEW_CUBES',
        force => true);
end;
/
*/

-- Инициализация правил цепочки
begin
    dbms_scheduler.define_chain_rule (
        chain_name => 'LD_CHAIN',
        condition => 'TRUE',
        action => 'START LD_STEP_PRIMARY_DATA',
        rule_name => 'LD_RULE_PRIMARY_DATA',
        comments => 'Старт загрузки первичных данных');

    dbms_scheduler.define_chain_rule (
        chain_name => 'LD_CHAIN',
        condition => 'LD_STEP_PRIMARY_DATA COMPLETED',
        action => 'START LD_STEP_OLD_CUBES',
        rule_name => 'LD_RULE_OLD_CUBES',
        comments => 'Старт загрузки старых кубов');

    dbms_scheduler.define_chain_rule (
        chain_name => 'LD_CHAIN',
        condition => 'LD_STEP_OLD_CUBES COMPLETED',
        action => 'START LD_STEP_NEW_CUBES',
        rule_name => 'LD_RULE_NEW_CUBES',
        comments => 'Старт загрузки новых кубов');

    dbms_scheduler.define_chain_rule (
        chain_name => 'LD_CHAIN',
        condition => 'LD_STEP_NEW_CUBES COMPLETED',
        action => 'END',
        rule_name => 'LD_RULE_END',
        comments => 'Окончание цепочки');
end;
/

-- Удаление правил цепочки
/*
begin
    dbms_scheduler.drop_chain_rule (
        chain_name => 'LD_CHAIN',
        rule_name => 'LD_RULE_PRIMARY_DATA',
        force => true);

    dbms_scheduler.drop_chain_rule (
        chain_name => 'LD_CHAIN',
        rule_name => 'LD_RULE_OLD_CUBES',
        force => true);

    dbms_scheduler.drop_chain_rule (
        chain_name => 'LD_CHAIN',
        rule_name => 'LD_RULE_NEW_CUBES',
        force => true);

    dbms_scheduler.drop_chain_rule (
        chain_name => 'LD_CHAIN',
        rule_name => 'LD_RULE_END',
        force => true);
end;
/
*/

-- Активация цепочки
begin
    dbms_scheduler.enable('LD_CHAIN');
end;
/

-- Инициализация расписания
begin    
    dbms_scheduler.create_schedule(
        schedule_name => 'LD_DAILY_SCHEDULE',
        start_date => to_timestamp_tz('01.01.2000 0:05:00 +3:00', 'dd.mm.yyyy hh24:mi:ss tzh:tzm'),
        repeat_interval => 'FREQ = DAILY'
    );
end;
/

-- Удаление расписания
/*
execute dbms_scheduler.drop_schedule('LD_DAILY_SCHEDULE');
*/

-- Инициализация рабочего окна    
begin    
    dbms_scheduler.create_window(
        window_name => 'LD_DAILY_WINDOW',
        resource_plan => 'LD_DAILY_PLAN',
        schedule_name => 'LD_DAILY_SCHEDULE',
        duration => interval '16' hour,
        window_priority => 'high'
    );
end;

-- Удаление рабочего окна
/*
execute dbms_scheduler.drop_window('LD_DAILY_WINDOW');
*/

-- Инициализация задачи
begin
    dbms_scheduler.create_job(
        job_name => 'JOB_LD',
        job_type => 'CHAIN',
        job_action => 'LD_CHAIN',
        schedule_name => 'LD_DAILY_SCHEDULE',
        enabled => true,
        auto_drop => false);
end;
/

-- Удаление задачи
/*
execute dbms_scheduler.drop_job('JOB_LD');
*/
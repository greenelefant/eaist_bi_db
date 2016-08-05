/*
  Скрипт вставки запросов в таблицу f_sp_sql_primary_data_load
    для заливки первичных данных
*/

-- Инициализация serveroutput
set serveroutput on

-- Очистка таблицы f_sp_sql_primary_data_load
delete
    from f_sp_sql_primary_data_load;

-- Объявление переменных
declare
    
    -- Тип массива запросов
    type rec_array_type is table of f_sp_sql_primary_data_load%rowtype
        index by binary_integer;
    
    -- Массив запросов
    rec_array rec_array_type;
    
    -- Индекс
    idx number := 0;
    
    -- Начальная строка
    start_str varchar2(4096) := 'DECLARE
    
    -- Текст запроса для вложенных execute immediate
    ST VARCHAR(32767);
    
    -- Переменные для привязки входных параметров
    V_ID_DATA_SOURCE NUMBER;
    V_ID_DATA_SOURCE_AUX NUMBER;
    V_VERSION_DATE DATE;
    
BEGIN

    -- Привязка входных параметров к переменным
    V_ID_DATA_SOURCE := :P_ID_DATA_SOURCE;
    V_ID_DATA_SOURCE_AUX := :P_ID_DATA_SOURCE_AUX;
    V_VERSION_DATE := :P_VERSION_DATE;
    
    -- ОСНОВНОЙ ТЕКСТ ЗАПРОСА
';
    
begin
    
    -- Формирование запросов
    
    -- SP_COMPLEX [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_COMPLEX';
    rec_array(idx).sql_name := 'SP_COMPLEX [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Справочник комплексов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into reports.SP_COMPLEX(id,complex_name,id_data_source,VERSION_DATE) select id, complex_name, V_ID_DATA_SOURCE, V_VERSION_DATE from complex@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_DEPARTMENT [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_DEPARTMENT';
    rec_array(idx).sql_name := 'SP_DEPARTMENT [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Справочник ведомств';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into reports.SP_DEPARTMENT(ID                    
                                                            ,GRBS                
                                                            ,ID_COMPLEX          
                                                            ,ID_ORGANIZATION          
                                                            ,ID_DATA_SOURCE      
                                                            ,VERSION_DATE) 
            select id,grbs,complex_id,enterprise_entity_id, V_ID_DATA_SOURCE, V_VERSION_DATE from department@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_ORGANIZATION [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION';
    rec_array(idx).sql_name := 'SP_ORGANIZATION [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Cправочник организаций';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into REPORTS.SP_ORGANIZATION(ID,ID_PARENT,INN,KPP, OPEN_DATE_D,ORGANIZATION_TYPE,FULL_NAME,SHORT_NAME,ID_DATA_SOURCE,VERSION_DATE,IS_CUSTOMER,IS_SUPPLIER,ENTITY_ID,FORMATTED_NAME, CONNECT_LEVEL) 
		select o.*,level from (
                            SELECT curr.ID,
                              curr.PARENT_ID,
                              curr.INN,
                              curr.KPP,
                              curr.DATE_START,
                              curr.COMPANY_TYPE,
                              curr.FULL_NAME,
                              curr.NAME,
                              V_ID_DATA_SOURCE,
                              V_VERSION_DATE,
                              case when company_type=3 then 1 else 0 end as IS_CUSTOMER,
                              case when company_type in (1,2) then 1 else 0 end as IS_SUPPLIER,
                              curr.entity_id,
                              FORMATE_NAME(FULL_NAME) as FORMATTED_NAME
                             FROM enterprise@tkdbn1 curr,
                                  (  SELECT entity_id, MAX (t.date_start) max_date, COUNT (*) cnt
                                       FROM enterprise@tkdbn1 t
                                   GROUP BY entity_id) mv
                              WHERE curr.entity_id = mv.entity_id AND curr.date_start = mv.max_date) o
          connect by prior o.entity_id=o.parent_id
          start with o.parent_id is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_ORGANIZATION - UPD [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION';
    rec_array(idx).sql_name := 'SP_ORGANIZATION - UPD [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Блок update';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        update sp_organization set id=entity_id where ID_DATA_SOURCE=V_ID_DATA_SOURCE and VERSION_DATE= V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Справочник заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CUSTOMER (ID,ID_PARENT,FULL_NAME,SHORT_NAME,CONNECT_LEVEL,INN,KPP, OPEN_DATE_D,IS_SMP,SPZ_CODE, ID_DATA_SOURCE,VERSION_DATE,S_KEY_SORT, FORMATTED_NAME )
            select ID,ID_PARENT,FULL_NAME,SHORT_NAME,CONNECT_LEVEL,INN,KPP, OPEN_DATE_D,IS_SMP,SPZ_CODE, ID_DATA_SOURCE,VERSION_DATE,S_KEY_SORT,
            FORMATE_NAME(FULL_NAME) from
              (  select cust.*,rownum s_key_sort from 
                 (
                    select 0 ID,null ID_PARENT,'Москва' FULL_NAME,'Москва' SHORT_NAME,1 CONNECT_LEVEL,null INN,null KPP, null OPEN_DATE_D, null IS_SMP,null SPZ_CODE,V_ID_DATA_SOURCE ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE, '0' key_sort from dual
                     union all
                    select id,0,regexp_replace(complex_name,'^[^[:alpha:]]{1,}',''),regexp_replace(complex_name,'^[^[:alpha:]]{1,}',''),2,null, null, null,null,null,V_ID_DATA_SOURCE ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE,to_char(id) key_sort
                        from sp_complex where ID_DATA_SOURCE=V_ID_DATA_SOURCE and VERSION_DATE=V_VERSION_DATE
                     union all
                    select v.id,v.id_complex,v.grbs,v.grbs,3,org.inn,org.kpp, open_date_d, null,null,V_ID_DATA_SOURCE ID_DATA_SOURCE,V_VERSION_DATE,v.grbs key_sort
                         from (select distinct id,grbs,id_complex from sp_department where ID_DATA_SOURCE=V_ID_DATA_SOURCE and VERSION_DATE=V_VERSION_DATE) v
                         join sp_organization org on v.id=org.id and org.id_data_source=V_ID_DATA_SOURCE and ORG.VERSION_DATE=V_VERSION_DATE
                     union all
                    select uchr.id,uchr.vedom_id,uchr.FULL_NAME,uchr.SHORT_NAME,4 conn_level,uchr.INN,uchr.KPP, open_date_d, uchr.IS_SMP,uchr.SPZ_CODE,V_ID_DATA_SOURCE ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE,uchr.FULL_NAME key_sort
                         from (select o.id,d.id vedom_id,d.id_complex,o.FULL_NAME,o.SHORT_NAME,o.INN,o.KPP, o.open_date_d,o.IS_SMP,o.SPZ_CODE,o.ID_DATA_SOURCE,o.VERSION_DATE from (select * from sp_department where id!=id_organization) d
                         join sp_organization o on d.id_organization=o.id and d.version_date=V_VERSION_DATE and O.VERSION_DATE=V_VERSION_DATE and D.ID_DATA_SOURCE=V_ID_DATA_SOURCE and o.ID_DATA_SOURCE=V_ID_DATA_SOURCE) uchr
                ) cust
                connect by prior cust.id=cust.id_parent
                start with cust.id=0
                order siblings by key_sort  );

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER - GRBS [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER - GRBS [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Простановка кодов ГРБС';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        update sp_customer c set c.grbs_code=(select grbs_code from lnk_grbs_code_customer where id_customer=c.id and eaist=1)
            where id_data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_ORGANIZATION [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION';
    rec_array(idx).sql_name := 'SP_ORGANIZATION [EAIST2]';
    rec_array(idx).description := 'Организации';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_ORGANIZATION (ID,
                                              ID_PARENT,
                                              FULL_NAME,
                                              SHORT_NAME,
                                              SPZ_CODE,
                                              INN,
                                              IS_SMP,
                                              IS_CUSTOMER,
                                              IS_SUPPLIER,
                                              ID_DATA_SOURCE,
                                              VERSION_DATE,
                                              ENTITY_ID,
                                              KPP,
                                              WEBSITE,
                                              ORGANIZATION_TYPE,
                                              CONNECT_LEVEL,
                                              OPEN_DATE,
                                              CLOSE_DATE,
                                              FORMATTED_NAME,
                                              OPEN_DATE_D,
                                              CLOSE_DATE_D,
                                              status,
                                              parent_grbs,
                                              address,
                                              --okopf,
                                              ogrn,
                                              phone,
                                              email)
                SELECT p.ID,
                       p.PARENT_ORGANIZATION,
                       p.FULL_NAME,
                       p.SHORT_NAME,
                       p.SPZ_CODE,
                       p.INN,
                       p.IS_SMP,
                       p.IS_CUSTOMER,
                       p.IS_SUPPLIER,
                       V_ID_DATA_SOURCE,
                       V_VERSION_DATE,
                       p.ENTITY_ID,
                       p.KPP,
                       p.WEB,
                       CASE
                          WHEN UNK IS NOT NULL THEN 2
                          WHEN UNK IS NULL AND IS_FL = 0 THEN 2
                          WHEN UNK IS NULL AND IS_FL = 1 THEN 1
                       END
                          ORGANIZATION_TYPE, --УНК бюджетополучателя, если оно заполнено, значит участник-бюджетополучатель
                       LEVEL,
                       ext_open,
                       ext_closed,
                       FORMATE_NAME (FULL_NAME) AS FORMATTED_NAME,
                       CASE
                          WHEN ext_open IS NULL
                          THEN
                             NULL
                          WHEN REGEXP_LIKE (
                                  ext_open,
                                  '[[:digit:]]{2}.[[:digit:]]{2}.[[:digit:]]{4}')
                          THEN
                             TO_DATE (ext_open, 'dd.mm.yyyy hh24:mi:ss')
                          WHEN REGEXP_LIKE (
                                  ext_open,
                                  '[[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2} 00:00:00')
                          THEN
                             TO_DATE (ext_open, 'yyyy-mm-dd hh24:mi:ss')
                       END
                          OPEN_DATE_D,
                       CASE
                          WHEN ext_closed IS NULL
                          THEN
                             NULL
                          WHEN REGEXP_LIKE (
                                  ext_closed,
                                  '[[:digit:]]{2}.[[:digit:]]{2}.[[:digit:]]{4}')
                          THEN
                             TO_DATE (ext_closed, 'dd.mm.yyyy hh24:mi:ss')
                          WHEN REGEXP_LIKE (
                                  ext_closed,
                                  '[[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2} 00:00:00')
                          THEN
                             TO_DATE (ext_closed, 'yyyy-mm-dd hh24:mi:ss')
                       END
                          CLOSE_DATE_D,
                       p.status,
                       parent_grbs,
                       address,
                       okopf,
                       ogrn,
                       phone,
                       email
                  FROM N_PARTICIPANT@EAIST_MOS_NSI p
            --where (PARENT_ORGANIZATION in (select ID from N_PARTICIPANT) or PARENT_ORGANIZATION is null)
            CONNECT BY     PRIOR p.id = p.parent_organization
                       AND p.DELETED_DATE IS NULL
            START WITH     parent_organization IS NULL
                       AND p.DELETED_DATE IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_ORGANIZATION - UPDATE_IS_COMPLEX [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION';
    rec_array(idx).sql_name := 'SP_ORGANIZATION - UPDATE_IS_COMPLEX [EAIST2]';
    rec_array(idx).description := 'Обновление is_complex';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         UPDATE REPORTS.SP_ORGANIZATION
            SET IS_COMPLEX = 1
          WHERE     ID_PARENT = 1             --and FULL_NAME like 'Комплекс%'
                AND ID_DATA_SOURCE = V_ID_DATA_SOURCE
                AND VERSION_DATE = V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION';
    rec_array(idx).sql_name := 'SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]';
    rec_array(idx).description := 'Корректировка дерева заказчиков (убрать, когда исправят в источнике)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         --корректировка дерева заказчиков (выпилить когда исправят в источнике)
         update sp_organization set id_parent=4687590 where id=4682553 AND is_customer=1 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE=V_VERSION_DATE; 
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT,description,ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=4682553',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
         
          update sp_organization set id_parent=1216 where id=6468 AND is_customer=1 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE=V_VERSION_DATE; 
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT,description,ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=6468',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
              
         UPDATE SP_ORGANIZATION SET ID_PARENT=4687586, CLOSE_DATE_D = TO_DATE('15.02.2011', 'dd.mm.yyyy'), CLOSE_DATE = '2011-15-02 00:00:00' WHERE ID = 4509 AND ID_DATA_SOURCE = V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT,description,ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=4509',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
              
         UPDATE REPORTS.SP_ORGANIZATION SET ID_PARENT=2911 WHERE ID = 6847136 AND ID_DATA_SOURCE = V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT,description,ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=6847136',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
              
         UPDATE REPORTS.SP_ORGANIZATION SET ID_PARENT=1441 WHERE ID = 4702486 AND ID_DATA_SOURCE = V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT,description,ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=4702486',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
              
         UPDATE REPORTS.SP_ORGANIZATION SET ID_PARENT=2911 WHERE ID = 6847048 AND ID_DATA_SOURCE = V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT,description,ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=6847048',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);       
                 
         UPDATE SP_ORGANIZATION SET id_parent=4138 WHERE id=7412 AND is_customer=1 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE=V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT,description,ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=7412',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
      
         UPDATE SP_ORGANIZATION SET id_parent = 4687586 WHERE id=5796 AND is_customer=1 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE=V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=5796',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
      
         UPDATE SP_ORGANIZATION SET id_parent=4687590 WHERE id=258 AND is_customer=1 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=258',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
              
         UPDATE SP_ORGANIZATION SET id_parent=4140 WHERE id=1252 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=1252',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);   
              
         UPDATE SP_ORGANIZATION SET id_parent=6742, connect_level=5 WHERE id=6996506 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=6996506',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);  
              
         UPDATE SP_ORGANIZATION SET id_parent=2377, connect_level=4 WHERE id=5292 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=5292',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);     
              
         UPDATE SP_ORGANIZATION SET id_parent=4687590, connect_level=3 WHERE id=7158 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=7158',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);    
      
         UPDATE SP_ORGANIZATION SET id_parent=1216, connect_level=4 WHERE id=7007877 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=7007877',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);  
              
         UPDATE SP_ORGANIZATION SET id_parent=4687586, connect_level=3 WHERE id=136 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=136',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);     
              
         UPDATE SP_ORGANIZATION SET id_parent=4666, connect_level=4 WHERE id=7159 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=7159',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);

         UPDATE SP_ORGANIZATION SET id_parent=5440, connect_level=4 WHERE id=6048 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=6048',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
              
         UPDATE SP_ORGANIZATION SET id_parent=1216, connect_level=4 WHERE id=5640619 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=5640619',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);

         UPDATE SP_ORGANIZATION SET kpp=771001001 WHERE id=6443 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=6443',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);
              
         UPDATE SP_ORGANIZATION SET id_parent=4687590, connect_level=3 WHERE id=4103 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=4103',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);  
                    
         UPDATE SP_ORGANIZATION SET id_parent=4687590, connect_level=3 WHERE id=4682554 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=4682554',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);   
              
         UPDATE SP_ORGANIZATION SET id_parent=4103, connect_level=4 WHERE id=7188392 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION - BRANCH_ADJUSTMENT [EAIST2]','Корректировка ветки id=7188392',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);              
         
         UPDATE SP_ORGANIZATION SET connect_level=4 WHERE id=4612785 AND ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE;
         INSERT INTO REPORTS.T_ERROR_LOG (NAME_OBJECT, description, ID_DATA_SOURCE, operation_type, version_date)
              VALUES ('SP_ORGANIZATION','корректировка ветки id=4612785',V_ID_DATA_SOURCE, 1, V_VERSION_DATE);              
        
    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_ORGANIZATION - AFTER_BRANCH_ADJUSTMENT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION';
    rec_array(idx).sql_name := 'SP_ORGANIZATION - AFTER_BRANCH_ADJUSTMENT [EAIST2]';
    rec_array(idx).description := 'Обновление после корректировки веток';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        merge into sp_organization so
        using (select so.id, so.version_date, so.id_data_source, so.connect_level+1 connect_level
        from sp_organization so
        inner join sp_organization sop on so.id_parent=sop.id and so.id_data_source=sop.id_data_source and so.version_date=sop.version_date and so.connect_level=sop.connect_level 
        and so.is_customer=1 and so.id_data_source=V_ID_DATA_SOURCE and so.version_date=V_VERSION_DATE) t
        on (so.id=t.id and so.version_date=t.version_date and so.id_data_source=t.id_data_source)
        when matched then update set
        so.connect_level=t.connect_level;        

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER [EAIST2]';
    rec_array(idx).description := 'Справочник заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO SP_CUSTOMER (ID,
                                  ID_PARENT,
                                  FULL_NAME,
                                  SHORT_NAME,
                                  CONNECT_LEVEL,
                                  INN,
                                  KPP,
                                  IS_SMP,
                                  SPZ_CODE,
                                  ID_DATA_SOURCE,
                                  VERSION_DATE,
                                  S_KEY_SORT,
                                  FORMATTED_NAME,
                                  OPEN_DATE,
                                  CLOSE_DATE,
                                  OPEN_DATE_D,
                                  CLOSE_DATE_D,
                                  status)
                       SELECT c.id,
                              c.ID_PARENT,
                              c.full_name,
                              c.short_name,
                              LEVEL,
                              c.INN,
                              c.kpp,
                              c.IS_SMP,
                              c.SPZ_CODE,
                              V_ID_DATA_SOURCE,
                              V_VERSION_DATE,
                              ROWNUM,
                              FORMATE_NAME (FULL_NAME) FORMATTED_NAME,
                              OPEN_DATE,
                              CLOSE_DATE,
                              OPEN_DATE_D,
                              CLOSE_DATE_D,
                              c.status
                         FROM SP_ORGANIZATION c
                   CONNECT BY     PRIOR id = ID_PARENT
                              AND is_customer = 1
                              AND ID_DATA_SOURCE = V_ID_DATA_SOURCE
                              AND VERSION_DATE = V_VERSION_DATE
                   START WITH     id = 1
                              AND is_customer = 1
                              AND ID_DATA_SOURCE = V_ID_DATA_SOURCE
                              AND VERSION_DATE = V_VERSION_DATE
            ORDER SIBLINGS BY c.full_name;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER - UPDATE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER - UPDATE [EAIST2]';
    rec_array(idx).description := 'Блок обновления';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
             UPDATE SP_CUSTOMER
                SET id = 0
              WHERE     id = 1
                    AND ID_DATA_SOURCE = V_ID_DATA_SOURCE
                    AND VERSION_DATE = V_VERSION_DATE;
    
             UPDATE SP_CUSTOMER
                SET id_parent = 0
              WHERE     id_parent = 1
                    AND ID_DATA_SOURCE = V_ID_DATA_SOURCE
                    AND VERSION_DATE = V_VERSION_DATE; --правительство Москвы id=0
    
             --Проставляем коды ГРБС
             UPDATE sp_customer c
                SET c.grbs_code =
                       (SELECT grbs_code
                          FROM lnk_grbs_code_customer
                         WHERE id_customer = c.id AND eaist = V_ID_DATA_SOURCE)
              WHERE id_data_source = 2 AND version_date = V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER - UPDATE_MERGE [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER - UPDATE_MERGE [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Простановка фиктивных ИНН и КПП';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).id_data_source_aux := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         --Для соответствия комплексов (2 уровень заказчиков) е1 с е2 ставим фиктивные инн и кпп
      
         --"Правительство Москвы" 
         UPDATE sp_customer
            SET inn = '0001', kpp = '0001'
          WHERE id = 0 AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE;
      
         UPDATE sp_customer
            SET inn = '0001', kpp = '0001'
          WHERE id = 0 AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE_AUX;
      
         --Комплекс градостроительной политики и строительства
         UPDATE sp_customer
            SET inn = '1001', kpp = '1001'
          WHERE id = 1 AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE;
      
         UPDATE sp_customer
            SET inn = '1001', kpp = '1001'
          WHERE     id = 4687588
                AND version_date = V_VERSION_DATE
                AND id_data_source = V_ID_DATA_SOURCE_AUX;
      
         --Комплекс жилищно-коммунального хозяйства и благоустройства
         UPDATE sp_customer
            SET inn = '1003', kpp = '1003'
          WHERE id = 3 AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE;
      
         UPDATE sp_customer
            SET inn = '1003', kpp = '1003'
          WHERE     id = 4687583
                AND version_date = V_VERSION_DATE
                AND id_data_source = V_ID_DATA_SOURCE_AUX;
                
         --Комплекс экономической политики и имущественно-земельных отношений (e2 id=4687589) был образован путем объединения
         -- Комплекса экономиечской политики и развития (e1 id=2) и Комплекса имущественно -земельных отношений (e1 id=4), id=2 передал полномочия id=4
         UPDATE sp_customer
            SET inn = '1004', kpp = '1004'
          WHERE id = 4 AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE;
      
         UPDATE sp_customer
            SET inn = '1004', kpp = '1004'
          WHERE     id = 4687589
                AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE_AUX;
      
         --Органы исполнительной власти, не входящие в состав комплексов городского управления
         UPDATE sp_customer
            SET inn = '1005', kpp = '1005'
          WHERE id = 5 AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE;
      
         UPDATE sp_customer
            SET inn = '1005', kpp = '1005'
          WHERE     id = 4687586
                AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE_AUX;
      
         --Префектуры административных округов города Москвы
         UPDATE sp_customer
            SET inn = '1006', kpp = '1006'
          WHERE id = 6 AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE;
      
         UPDATE sp_customer
            SET inn = '1006', kpp = '1006'
          WHERE     id = 4687587
                AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE_AUX;
      
         --Комплекс социального развития
         UPDATE sp_customer
            SET inn = '1007', kpp = '1007'
          WHERE id = 7 AND version_date = V_VERSION_DATE AND id_data_source = V_ID_DATA_SOURCE;
      
         UPDATE sp_customer
            SET inn = '1007', kpp = '1007'
          WHERE     id = 4687590
                AND version_date = V_VERSION_DATE
                AND id_data_source = V_ID_DATA_SOURCE_AUX;      
                
        --ставим инн и кпп ведомствам е1 из е2 для сводного справочника
              merge into sp_customer a using (select id,inn,kpp from sp_customer where id_data_source=V_ID_DATA_SOURCE_AUX and version_date=V_VERSION_DATE and id=4666) b
              on (1=1)
              when matched then
              update set a.inn=b.inn,a.kpp=b.kpp where id_data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE and id=69801;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_ORGANIZATION_JOINT [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION_JOINT';
    rec_array(idx).sql_name := 'SP_ORGANIZATION_JOINT [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Сводный справочник заказчиков и поставщиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).id_data_source_aux := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO SP_ORGANIZATION_JOINT
            select 
                o1.id ID_EAIST1,
                O1.ID_PARENT id_parent_eaist1,
                o1.formatted_name formatted_name_eaist1,
                O1.CONNECT_LEVEL connect_level_eaist1,
                o1.grbs_code grbs_code_eaist1,
                get_grbs (o1.id, 1)
                  "ОИВ, с которым связан, еаист1",
                CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                o2.id ID_EAIST2,
                O2.ID_PARENT id_parent_eaist2,
                o2.formatted_name formatted_name_eaist2,
                O2.CONNECT_LEVEL connect_level_eaist2,
                o2.grbs_code grbs_code_eaist2,
                get_grbs (o2.id, 2)
                  "ОИВ, с которым связан, еаист2",
                CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                NVL (o2.inn, o1.inn) INN,
                NVL (o2.kpp, o1.kpp) KPP,
                o2.OPEN_DATE_D OPEN_DATE,
                o2.CLOSE_DATE_D CLOSE_DATE,
                1 is_customer,
                0 is_supplier,
                V_VERSION_DATE VERSION_DATE 
            from
            (select * from lnk_grbs_code_customer where eaist=1) sp1
            join (select * from lnk_grbs_code_customer where eaist=2) sp2
            on sp1.grbs_code=sp2.grbs_code
            join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE AND version_date = V_VERSION_DATE) o1
            on o1.id=sp1.id_customer
            join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE_AUX AND version_date = V_VERSION_DATE and status NOT IN (3)) o2
            on o2.id=sp2.id_customer
            union all
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   o1.grbs_code grbs_code_eaist1,
                   get_grbs (o1.id, 1)
                      "ОИВ, с которым связан, еаист1",
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   o2.grbs_code grbs_code_eaist2,
                   get_grbs (o2.id, 2)
                      "ОИВ, с которым связан, еаист2",
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   1 is_customer,
                   0 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE) o1
                   JOIN
                      (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
                   
                   LEFT JOIN (select sp1.id_customer id_customer1, sp2.id_customer id_customer2 from
                        (select * from lnk_grbs_code_customer where eaist=1) sp1
                        join (select * from lnk_grbs_code_customer where eaist=2) sp2
                        on sp1.grbs_code=sp2.grbs_code
                        join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE_AUX AND version_date = V_VERSION_DATE and status NOT IN (3)) o2
                        on o2.id=sp2.id_customer) grbs_lnk1
                   ON o1.id=grbs_lnk1.id_customer1
                   
                   LEFT JOIN (select sp1.id_customer id_customer1, sp2.id_customer id_customer2 from
                        (select * from lnk_grbs_code_customer where eaist=1) sp1
                        join (select * from lnk_grbs_code_customer where eaist=2) sp2
                        on sp1.grbs_code=sp2.grbs_code
                        join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE AND version_date = V_VERSION_DATE) o1
                        on o1.id=sp1.id_customer) grbs_lnk2
                   ON o2.id=grbs_lnk2.id_customer2
              WHERE grbs_lnk1.id_customer1 is null and grbs_lnk2.id_customer2 is null
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   o1.grbs_code grbs_code_eaist1,
                   get_grbs (o1.id, 1)
                      "ОИВ, с которым связан, еаист1",
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   o2.grbs_code grbs_code_eaist2,
                   get_grbs (o2.id, 2)
                      "ОИВ, с которым связан, еаист2",
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   1 is_customer,
                   0 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE) o1
                   LEFT JOIN
                      (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
                   LEFT JOIN (select sp1.id_customer id_customer1, sp2.id_customer id_customer2 from
                        (select * from lnk_grbs_code_customer where eaist=1) sp1
                        join (select * from lnk_grbs_code_customer where eaist=2) sp2
                        on sp1.grbs_code=sp2.grbs_code
                        join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE_AUX AND version_date = V_VERSION_DATE and status NOT IN (3)) o2
                        on o2.id=sp2.id_customer) grbs_lnk
                   ON o1.id=grbs_lnk.id_customer1
             WHERE O2.ID IS NULL and grbs_lnk.id_customer1 is null
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   o1.grbs_code grbs_code_eaist1,
                   get_grbs (o1.id, 1)
                      "ОИВ, с которым связан, еаист1",
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   o2.grbs_code grbs_code_eaist2,
                   get_grbs (o2.id, 2)
                      "ОИВ, с которым связан, еаист2",
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   1 is_customer,
                   0 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE) o1
                   RIGHT JOIN
                      (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
                   LEFT JOIN (select sp1.id_customer id_customer1, sp2.id_customer id_customer2 from
                        (select * from lnk_grbs_code_customer where eaist=1) sp1
                        join (select * from lnk_grbs_code_customer where eaist=2) sp2
                        on sp1.grbs_code=sp2.grbs_code
                        join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE AND version_date = V_VERSION_DATE) o1
                        on o1.id=sp1.id_customer) grbs_lnk
                   ON o2.id=grbs_lnk.id_customer2
             WHERE O1.ID IS NULL and grbs_lnk.id_customer2 is null
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   NULL,
                   NULL,
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   NULL,
                   NULL,
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   0 is_customer,
                   1 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1) o1
                   JOIN
                      (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   NULL,
                   NULL,
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   NULL,
                   NULL,
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   0 is_customer,
                   1 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1) o1
                   LEFT JOIN
                      (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
             WHERE O2.ID IS NULL
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   NULL,
                   NULL,
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   NULL,
                   NULL,
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o2.inn, o2.inn) INN,
                   NVL (o2.kpp, o2.kpp) KPP,
                   o1.OPEN_DATE_D OPEN_DATE,
                   o1.CLOSE_DATE_D CLOSE_DATE,
                   0 is_customer,
                   1 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1) o1
                   RIGHT JOIN
                      (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
             WHERE O1.ID IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SUPPLIERS_JOINT [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SUPPLIERS_JOINT';
    rec_array(idx).sql_name := 'SP_SUPPLIERS_JOINT [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Объединенный справочник поставщиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).id_data_source_aux := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO SP_SUPPLIERS_JOINT (RN, ID,ID_SOURCE, ID_PARENT, ID_PARENT_SOURCE, ID_LEVEL, FORMATTED_NAME, IS_AGGREGATOR, VERSION_DATE)
        VALUES (1,0,V_ID_DATA_SOURCE_AUX,null,null,1,'Все поставщики',null,V_VERSION_DATE);

        INSERT INTO SP_SUPPLIERS_JOINT (RN, ID,ID_SOURCE, ID_PARENT, ID_PARENT_SOURCE, ID_LEVEL, FORMATTED_NAME, IS_AGGREGATOR, VERSION_DATE)
        SELECT rownum+1 rn, org.*
        FROM
          (SELECT DISTINCT id_eaist2, V_ID_DATA_SOURCE_AUX ID_SOURCE, 0 as id_parent_eaist2, V_ID_DATA_SOURCE_AUX as ID_PARENT_SOURCE, 2 as connect_level_eaist2, formatted_name_eaist2, null IS_AGGREGATOR, version_date  
          FROM SP_ORGANIZATION_JOINT 
          WHERE VERSION_DATE=V_VERSION_DATE
          AND id_eaist2 IS NOT NULL AND is_supplier = 1
            UNION ALL
          SELECT id_eaist1, V_ID_DATA_SOURCE ID_SOURCE, 0 as id_parent_eaist1, V_ID_DATA_SOURCE_AUX as ID_PARENT_SOURCE, 2 connect_level_eaist1, formatted_name_eaist1, null IS_AGGREGATOR, version_date  
          FROM SP_ORGANIZATION_JOINT 
          WHERE VERSION_DATE=V_VERSION_DATE
          AND id_eaist2 IS NULL AND id_eaist1 IS NOT NULL AND is_supplier = 1
          ) org;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_SUPPLIERS_UNITED
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_SUPPLIERS_UNITED';
    rec_array(idx).sql_name := 'LNK_SUPPLIERS_UNITED';
    rec_array(idx).description := 'Связь между поставщиками и объединенными поставщиками';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).id_data_source_aux := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO LNK_SUPPLIERS_UNITED (SUPPLIER_ID,ID_DATA_SOURCE,UNITED_SUPPLIER_ID,UNITED_SOURCE_ID,UNITED_SUPPLIER_CID,VERSION_DATE, FORMATTED_NAME)
        
        SELECT id, id_data_source, id_eaist2, id_data_source_j, id_eaist2||'_'||id_data_source_j, V_VERSION_DATE, formatted_name 
        FROM 
        (SELECT c.id, c.id_data_source, oj.id_eaist2, oj.id_data_source  id_data_source_j, c.formatted_name
        FROM sp_organization c
        INNER JOIN (SELECT id_eaist1, max(id_eaist2) id_eaist2, V_ID_DATA_SOURCE_AUX id_data_source FROM sp_organization_joint 
                    WHERE version_date=V_VERSION_DATE and is_supplier=1 and id_eaist1 IS NOT NULL and id_eaist2 IS NOT NULL
                    group by id_eaist1) oj
        ON c.id=oj.id_eaist1 
        WHERE c.id_data_source=V_ID_DATA_SOURCE and c.version_date=V_VERSION_DATE
        
        UNION ALL
        
        SELECT c.id, c.id_data_source, oj.id_eaist1, oj.id_data_source  id_data_source_j, c.formatted_name
        FROM sp_organization c
        INNER JOIN (select  id_eaist1, id_eaist2, V_ID_DATA_SOURCE id_data_source from sp_organization_joint WHERE version_date=V_VERSION_DATE and is_supplier=1 and id_eaist1 IS NOT NULL and id_eaist2 is null) oj
        on c.id=oj.id_eaist1 
        WHERE c.id_data_source=V_ID_DATA_SOURCE and c.version_date=V_VERSION_DATE
        
        UNION ALL
        
        SELECT c.id, c.id_data_source, c.id id_eaist2, c.id_data_source id_data_source_j, c.formatted_name
        FROM sp_organization c
        WHERE c.id_data_source=V_ID_DATA_SOURCE_AUX and c.version_date=V_VERSION_DATE and c.is_supplier=1
        
        UNION ALL
        
        SELECT c.id, c.id_data_source, oj.id_eaist2, oj.id_data_source   id_data_source_j, c.formatted_name
        FROM sp_organization c
        INNER JOIN (SELECT id_eaist1, max(id_eaist2) id_eaist2, V_ID_DATA_SOURCE_AUX id_data_source FROM sp_organization_joint 
                    WHERE version_date=V_VERSION_DATE and is_supplier=1 and id_eaist1 IS NOT NULL and id_eaist2 IS NOT NULL
                    group by id_eaist1) oj
        on c.id=oj.id_eaist1 
        WHERE c.id_data_source=4 and c.version_date=V_VERSION_DATE
        
        UNION ALL
        
        SELECT c.id, c.id_data_source, oj.id_eaist1, oj.id_data_source  id_data_source_j, c.formatted_name
        FROM sp_organization c
        INNER JOIN (select  id_eaist1, id_eaist2, V_ID_DATA_SOURCE id_data_source from sp_organization_joint WHERE version_date=V_VERSION_DATE and is_supplier=1 and id_eaist1 IS NOT NULL and id_eaist2 is null) oj
        on c.id=oj.id_eaist1 
        WHERE c.id_data_source=4 and c.version_date=V_VERSION_DATE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CUSTOMERS_UNITED [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CUSTOMERS_UNITED';
    rec_array(idx).sql_name := 'LNK_CUSTOMERS_UNITED [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Связь между заказчиками и объединенными заказчиками';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).id_data_source_aux := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO LNK_CUSTOMERS_UNITED (CUSTOMER_ID,ID_DATA_SOURCE,UNITED_CUSTOMER_ID,UNITED_SOURCE_ID,UNITED_CUSTOMER_CID,VERSION_DATE, FORMATTED_NAME)
        SELECT id, id_data_source, id_eaist2, id_data_source_j, id_eaist2||'_'||id_data_source_j, V_VERSION_DATE, formatted_name 
        FROM 
        (SELECT c.id, c.id_data_source, oj.id_eaist2, oj.id_data_source  id_data_source_j, c.formatted_name
        FROM sp_customer c
        INNER JOIN (SELECT id_eaist1, id_eaist2, V_ID_DATA_SOURCE_AUX id_data_source FROM sp_organization_joint WHERE version_date=V_VERSION_DATE and is_customer=1 and id_eaist1 IS NOT NULL and id_eaist2 IS NOT NULL) oj
        ON c.id=oj.id_eaist1 
        WHERE c.id_data_source=V_ID_DATA_SOURCE and c.version_date=V_VERSION_DATE
        UNION ALL
        SELECT c.id, c.id_data_source, oj.id_eaist1, oj.id_data_source  id_data_source_j, c.formatted_name
        FROM sp_customer c
        INNER JOIN (select  id_eaist1, id_eaist2, V_ID_DATA_SOURCE id_data_source from sp_organization_joint WHERE version_date=V_VERSION_DATE and is_customer=1 and id_eaist1 IS NOT NULL and id_eaist2 is null) oj
        on c.id=oj.id_eaist1 
        WHERE c.id_data_source=V_ID_DATA_SOURCE and c.version_date=V_VERSION_DATE
        UNION ALL
        SELECT c.id, c.id_data_source, c.id id_eaist2, c.id_data_source id_data_source_j, c.formatted_name
        FROM sp_customer c
        WHERE c.id_data_source=V_ID_DATA_SOURCE_AUX and c.version_date=V_VERSION_DATE
        UNION ALL
        SELECT c.id, c.id_data_source, oj.id_eaist2, oj.id_data_source   id_data_source_j, c.formatted_name
        FROM sp_customer c
        INNER JOIN (select id_eaist1, id_eaist2, V_ID_DATA_SOURCE_AUX id_data_source from sp_organization_joint WHERE version_date=V_VERSION_DATE and is_customer=1 and id_eaist1 IS NOT NULL and id_eaist2 IS NOT NULL) oj
        on c.id=oj.id_eaist1 
        WHERE c.id_data_source=4 and c.version_date=V_VERSION_DATE
        UNION ALL
        SELECT c.id, c.id_data_source, oj.id_eaist1, oj.id_data_source  id_data_source_j, c.formatted_name
        FROM sp_customer c
        INNER JOIN (select  id_eaist1, id_eaist2, V_ID_DATA_SOURCE id_data_source from sp_organization_joint WHERE version_date=V_VERSION_DATE and is_customer=1 and id_eaist1 IS NOT NULL and id_eaist2 is null) oj
        on c.id=oj.id_eaist1 
        WHERE c.id_data_source=4 and c.version_date=V_VERSION_DATE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- GENERATE_TREE_CUSTOMERS [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'GENERATE_TREE_CUSTOMERS';
    rec_array(idx).sql_name := 'GENERATE_TREE_CUSTOMERS [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Построение дерева';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        SINGLE_CUSTOMERS_SOURCES.GENERATE_TREE_CUSTOMERS(V_VERSION_DATE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMERS_TREE [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMERS_TREE';
    rec_array(idx).sql_name := 'SP_CUSTOMERS_TREE [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Сводное дерево заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          insert into SP_CUSTOMERS_TREE (RN,ID,  ID_SOURCE,  ID_PARENT,ID_PARENT_SOURCE,ID_LEVEL,FORMATTED_NAME,IS_AGGREGATOR,VERSION_DATE, KEY_SORT)
                      SELECT ROWNUM rn,
                             id,
                             id_source,
                             id_parent,
                             id_parent_source,
                             id_level,
                             formatted_name,
                             is_aggregator,
                             V_VERSION_DATE,
                             s_key_sort
                        FROM (    SELECT t.id,
                                         t.id_source,
                                         t.id_parent,
                                         t.id_parent_source,
                                         LEVEL + 1 AS id_level,
                                         formatted_name,
                                         s_key_sort,
                                         is_aggregator,
                                         id_agg,
                                         parent_agg
                                    FROM (SELECT r.id,
                                                 r.id_source,
                                                 r.id_parent,
                                                 r.id_parent_source,
                                                 c.formatted_name,
                                                 c.s_key_sort,
                                                 r.id || '_' || r.id_source AS id_agg,
                                                 r.id_parent || '_' || r.id_parent_source
                                                    AS parent_agg,
                                                 is_aggregator
                                            FROM    LNK_CUSTOMERS_ALL_SOURCES r
                                                 LEFT JOIN
                                                    sp_customer c
                                                 ON c.version_date = V_VERSION_DATE
                                                    AND c.id_data_source = r.id_source
                                                    AND c.id = r.id
                                           WHERE r.version_date = V_VERSION_DATE ) t
                              CONNECT BY NOCYCLE PRIOR id_agg = parent_agg
                              START WITH parent_agg = '0_2')
                  CONNECT BY PRIOR id_agg = parent_agg
                  START WITH parent_agg = '0_2'
           ORDER SIBLINGS BY CASE
                                WHEN id_level IN (2, 3)
                                THEN
                                   LPAD (s_key_sort, 16, '0')
                                ELSE
                                   formatted_name
                             END;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CUSTOMERS_ALL_LEVEL [LOAD_ORG_JOINT]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CUSTOMERS_ALL_LEVEL';
    rec_array(idx).sql_name := 'LNK_CUSTOMERS_ALL_LEVEL [LOAD_ORG_JOINT]';
    rec_array(idx).description := 'Таблица связей объединенных заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into LNK_CUSTOMERS_ALL_LEVEL (ID,ID_PARENT,CONNECT_LEVEL,VERSION_DATE,ID_DATA_SOURCE,CONNECT_LEVEL_PARENT)
        select id, ID_FIRST, id_level, version_date, id_source, ID_LEVEL_FIRST 
        from
          (SELECT id,
                 trim(',' FROM sys_connect_by_path(CASE WHEN LEVEL = 1 THEN id END, ',')) ID_FIRST,
                 id_level, version_date, id_source,
                 trim(',' FROM sys_connect_by_path(CASE WHEN LEVEL = 1 THEN id_level END, ',')) ID_LEVEL_FIRST
          from (
          select id||'_'||id_source id, id_source, id_parent||'_'||id_parent_source id_parent, id_level, version_date, id_parent_source 
          from SP_CUSTOMERS_TREE where version_date=V_VERSION_DATE
          union
          select '0_'||V_ID_DATA_SOURCE, V_ID_DATA_SOURCE, null, 1, V_VERSION_DATE, V_ID_DATA_SOURCE from dual
          ) soj
          connect BY PRIOR id =id_parent )
        where id<>ID_FIRST;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_OKEI [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_OKEI';
    rec_array(idx).sql_name := 'SP_OKEI [EAIST2]';
    rec_array(idx).description := 'Общероссийский классификатор единиц измерения';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_OKEI (ID,
                                      CODE,
                                      NAME,
                                      DESIGNATION_CODE_INTERNATIONAL,
                                      DESIGNATION_CODE_NATIONAL,
                                      DESIGNATION_INTERNATIONAL,
                                      DESIGNATION_NATIONAL,
                                      OKEI_GROUP,
                                      OKEI_SECTION,
                                      FACTOR,
                                      PARENT_ID,
                                      ID_DATA_SOURCE,
                                      VERSION_DATE)
            SELECT ID,
                   CODE,
                   NAME,
                   DESIGNATION_CODE_INTERNATIONAL,
                   DESIGNATION_CODE_NATIONAL,
                   DESIGNATION_INTERNATIONAL,
                   DESIGNATION_NATIONAL,
                   OKEI_GROUP,
                   OKEI_SECTION,
                   FACTOR,
                   PARENT_ID,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM N_OKEI@EAIST_MOS_NSI
             WHERE deleted_date IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';
      
    -- SP_PERIODICITY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_PERIODICITY';
    rec_array(idx).sql_name := 'SP_PERIODICITY [EAIST2]';
    rec_array(idx).description := 'Периодичность';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_PERIODICITY (ID,
                                             ENTITY_ID,
                                             CODE,
                                             NAME,
                                             VERSION_USER_ID,
                                             ID_DATA_SOURCE,
                                             VERSION_DATE)
            SELECT ID,
                   ENTITY_ID,
                   CODE,
                   NAME,
                   VERSION_USER_ID,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM N_PERIODICITY@EAIST_MOS_NSI where deleted_date is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_METHOD [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_METHOD';
    rec_array(idx).sql_name := 'SP_METHOD [EAIST2]';
    rec_array(idx).description := 'Способ размещения';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         /*INSERT INTO REPORTS.SP_METHOD (ID,
                                        NAME,
                                        CODE,
                                        ID_DATA_SOURCE,
                                        VERSION_DATE)
            SELECT ID,
                   NAME,
                   CODE,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM N_METHOD_OF_SUPPLIER@EAIST_MOS_NSI
             WHERE DELETED_DATE IS NULL AND ID = ENTITY_ID;*/
        insert into sp_method (id, name, id_data_source, version_date)
        select 1,'открытый конкурс', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 2,'конкурс с ограниченным участием', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 3,'двухэтапный конкурс', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 4,'электронный аукцион', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 5,'запрос котировок', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 6,'запрос предложений', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 7,'закупки у единственного поставщика', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 8,'закрытый конкурс', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 9,'закрытый конкурс с ограниченным участием', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 10,'закрытый двухэтапный конкурс', V_ID_DATA_SOURCE, V_VERSION_DATE from dual
        union
        select 11,'закрытый аукцион', V_ID_DATA_SOURCE, V_VERSION_DATE from dual;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_EXAMINATION_TYPE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_EXAMINATION_TYPE';
    rec_array(idx).sql_name := 'SP_EXAMINATION_TYPE [EAIST2]';
    rec_array(idx).description := 'Типы экспертизы';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_EXAMINATION_TYPE (ID,CODE,NAME,ID_DATA_SOURCE,VERSION_DATE)              
            select 1 ID,'EXPERT' CODE,'Экспертиза НМЦ' NAME,V_ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE from dual
            union all
            select 2 ID,'GRBS' CODE,'РГ ГРБС' NAME,V_ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE from dual
            union all
            select 3 ID,'MRG' CODE,'МРГ' NAME,V_ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE from dual;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KOSGU [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KOSGU';
    rec_array(idx).sql_name := 'SP_KOSGU [EAIST2]';
    rec_array(idx).description := 'КОСГУ';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_KOSGU (ID,
                                       NAME,
                                       CODE,
                                       ID_DATA_SOURCE,
                                       VERSION_DATE,
                                       ENTITY_ID)
            SELECT ID,
                   NAME,
                   CODE,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   ENTITY_ID
              FROM REPORTS.SP_KOSGU
             WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                   AND VERSION_DATE = (SELECT MAX (VERSION_DATE)
                                         FROM REPORTS.SP_KOSGU
                                        WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SECTION_BUDGET [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SECTION_BUDGET';
    rec_array(idx).sql_name := 'SP_SECTION_BUDGET [EAIST2]';
    rec_array(idx).description := 'Раздел/подраздел бюджета';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_SECTION_BUDGET (ID,
                                                NAME,
                                                DESCRIPTION,
                                                CODE,
                                                ID_DATA_SOURCE,
                                                VERSION_DATE)
            SELECT ID,
                   NAME,
                   DESCRIPTION,
                   CODE,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM SP_SECTION_BUDGET
             WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                   AND VERSION_DATE = (SELECT MAX (VERSION_DATE)
                                         FROM REPORTS.SP_SECTION_BUDGET
                                        WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_TARGET_CLAUSE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_TARGET_CLAUSE';
    rec_array(idx).sql_name := 'SP_TARGET_CLAUSE [EAIST2]';
    rec_array(idx).description := 'Целевая статья';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_TARGET_CLAUSE (ID,
                                               NAME,
                                               CODE,
                                               ID_DATA_SOURCE,
                                               VERSION_DATE)
            SELECT ID,
                   NAME,
                   CODE,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM SP_TARGET_CLAUSE
             WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                   AND VERSION_DATE = (SELECT MAX (VERSION_DATE)
                                         FROM REPORTS.SP_TARGET_CLAUSE
                                        WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_TYPE_EXPENSE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_TYPE_EXPENSE';
    rec_array(idx).sql_name := 'SP_TYPE_EXPENSE [EAIST2]';
    rec_array(idx).description := 'Вид расходов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_TYPE_EXPENSE (ID,
                                              NAME,
                                              CODE,
                                              ID_DATA_SOURCE,
                                              VERSION_DATE)
            SELECT ID,
                   NAME,
                   CODE,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM SP_TYPE_EXPENSE
             WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                   AND VERSION_DATE = (SELECT MAX (VERSION_DATE)
                                         FROM REPORTS.SP_TYPE_EXPENSE
                                        WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;
	
END;#';

    -- T_CONTRACT_DOCUMENT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_DOCUMENT';
    rec_array(idx).sql_name := 'T_CONTRACT_DOCUMENT [EAIST2]';
    rec_array(idx).description := 'Документы по контрактам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into t_contract_document (id, document_date, document_number, category_id, contract_id, document_id, entity_id, is_from_library, id_data_source, version_date)
        select id, document_date, document_number, category_id, contract_id, document_id, entity_id, is_from_library, V_ID_DATA_SOURCE, V_VERSION_DATE from contract_document@eaist_mos_rc;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;	

END;#';

    -- SP_GRBS [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_GRBS';
    rec_array(idx).sql_name := 'SP_GRBS [EAIST2]';
    rec_array(idx).description := 'ГРБС';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_GRBS (ID,
                                      CODE,
                                      NAME,
                                      DESCRIPTION,
                                      ID_DATA_SOURCE,
                                      VERSION_DATE)
            SELECT ID,
                   CODE,
                   NAME,
                   DESCRIPTION,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM SP_GRBS
             WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                   AND VERSION_DATE = (SELECT MAX (VERSION_DATE)
                                         FROM REPORTS.SP_GRBS
                                        WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SOURCE_FINANCE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SOURCE_FINANCE';
    rec_array(idx).sql_name := 'SP_SOURCE_FINANCE [EAIST2]';
    rec_array(idx).description := 'Источник финансирования';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_SOURCE_FINANCE (ID,
                                                CODE,
                                                NAME,
                                                ID_DATA_SOURCE,
                                                VERSION_DATE)
              select 1 ID,'LIMIT' CODE,'Бюджет города Москвы' NAME,V_ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE from dual 
              union all
              select 2 ID,'PFHD' CODE,'ПФХД' NAME,V_ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE from dual 
              union all
              select 3 ID,'OWN_FUNDS' CODE,'Собственные средства' NAME,V_ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE from dual 
              union all
              select 4 ID,'FEDERAL_FUNDS' CODE,'Федеральные средства' NAME,V_ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE from dual;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_STATUS_CATEGORY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_STATUS_CATEGORY';
    rec_array(idx).sql_name := 'SP_STATUS_CATEGORY [EAIST2]';
    rec_array(idx).description := 'Справочник категорий статусов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (1, 'contract', 'Статусы для договоров', 'Договор', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (2, 'purchase', 'Статусы объектов закупки (ОЗ)', 'ОЗ', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (3, 'lot', 'Статусы для лотов', 'Лот', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (4, 'plan_purchase', 'Статусы для плана закупок', 'План закупок', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (5, 'doz', 'Статусы детализированных объектов закупки', 'ДОЗ', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (8, 'contractСlaim', 'Статусы для претензий по договорам', 'Претензии по договору', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (10, 'contractstage', 'Статусы для стадий по договорам', 'Стадии по договору', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (12, '', 'Статусы бюджетных обязательств (БО)', 'БО', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (14, 'PlanSchedule', 'Статусы планов-графиков детализированных объектов закупки', 'План-график', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (15, 'BidStatus', 'Статусы заявок', 'Статусы заявок', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (16, '', 'Статусы процедур закупок', 'Статусы процедур закупок', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (17, '', 'Состояние процедур закупок', 'Состояние процедур закупок', V_ID_DATA_SOURCE, V_VERSION_DATE);
        INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID, CODE, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE) VALUES (18, '', 'Cостояние участников торгов', 'Cостояние участников торгов', V_ID_DATA_SOURCE, V_VERSION_DATE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_STATUS [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_STATUS';
    rec_array(idx).sql_name := 'SP_STATUS [EAIST2]';
    rec_array(idx).description := 'Статусы';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          -- Статусы по контрактам id_status_category=1 и претензиям id_status_category=8 и стадиям
          INSERT INTO REPORTS.SP_STATUS (ID,NAME,DESCRIPTION,CODE,ID_CATEGORY,ID_DATA_SOURCE,VERSION_DATE)
              select ID,NAME,DESCRIPTION,CODE,CATEGORY_ID ,V_ID_DATA_SOURCE,V_VERSION_DATE from STATE@EAIST_MOS_RC where category_id in (1, 8, 10);--2 статуса с нулловой категорией
          -- Статусы по БО id_status_category=12
          INSERT INTO REPORTS.SP_STATUS (ID,NAME,DESCRIPTION,CODE,ID_CATEGORY,ID_DATA_SOURCE,VERSION_DATE)
              select ID,NAME,DESCRIPTION,CODE,CATEGORY_ID ,V_ID_DATA_SOURCE,V_VERSION_DATE from STATE@EAIST_MOS_RC where category_id=12;
          -- Статусы лотов id_category=3
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (0,'DF_APPROVED','На согласовании ДФ',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (1,'FORMATION','Формирование',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (2,'SIGNED','Утвержден',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (3,'GRBS_APPROVED','На согласовании ГРБС',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (4,'INCLUDE_IN_PLAN_SCHEDULE','Включён в план-график',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (5,'PUBLISHED','Опубликовано в плане-график',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (6,'CANCELED','Отменён',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (7,'REMOVED','Удалён',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (8,'CORRECTION','К корректировке',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (9,'COORDINATION_OF_APPLICATION','Согласование заявки',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (10,'MAKING_MODIFICATIONS','Внесение изменений',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (11,'DID_NOT_TAKE_PLACE','Не состоялся',3,V_ID_DATA_SOURCE,V_VERSION_DATE);
          -- Статусы ОЗ id_category=2
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (1,'FORMATION','Формирование',2,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (2,'SIGNED','Утвержден',2,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (3,'DF_APPROVED','На согласовании ДФ',2,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (4,'GRBS_APPROVED','На согласовании ГРБС',2,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (5,'INCLUDE_IN_PURCHASE_PLAN','Включён в план закупок',2,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (6,'PUBLISHED','Опубликован',2,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (7,'CANCELED','Отменён',2,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (8,'REMOVED','Удалён',2,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (9,'CORRECTION','К корректировке',2,V_ID_DATA_SOURCE,V_VERSION_DATE);
          -- Статусы для ДОЗ id_category=5
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (0,'DF_APPROVED','На согласовании ДФ',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (1,'FORMATION','Формирование',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (2,'SIGNED','Утвержден',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (3,'GRBS_APPROVED','На согласовании ГРБС',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (4,'INCLUDE_IN_PLAN_SCHEDULE','Включён в план-график',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (5,'INCLUDE_IN_LOT','Включён в лот',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (6,'PUBLISHED','Опубликован',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (7,'CANCELED','Отменён',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (8,'REMOVED','Удалён',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (9,'CORRECTION','К корректировке',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (10,'DID_NOT_TAKE_PLACE','Не состоялся',5,V_ID_DATA_SOURCE,V_VERSION_DATE);
          -- Статусы для ПЗ id_category=4
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (1,'DF_APPROVED','На согласовании ДФ',4,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (2,'GRBS_APPROVED','На согласовании ГРБС',4,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (3,'APPROVED','Согласовано',4,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (4,'CORRECTION','К корректировке',4,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (5,'SIGNED','Утвержден',4,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (6,'SENT_FOR_PUBLICATION','Отправлен на публикацию',4,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (7,'PUBLICATION_ERROR','Ошибка публикации',4,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (8,'PUBLISHED','Опубликован',4,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (9,'REMOVED','Удалён',4,V_ID_DATA_SOURCE,V_VERSION_DATE);
          -- Статусы для ПГ id_category=14
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (0,'DF_APPROVED','На согласовании ДФ',14,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (1,'GRBS_APPROVED','На согласовании ГРБС',14,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (2,'APPROVED','Согласовано',14,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (3,'CORRECTION','К корректировке',14,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (4,'SIGNED','Утвержден',14,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (5,'SENT_FOR_PUBLICATION','Отправлен на публикацию',14,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (6,'PUBLICATION_ERROR','Ошибка публикации',14,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (7,'PUBLISHED','Опубликован',14,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (8,'REMOVED','Удалён',14,V_ID_DATA_SOURCE,V_VERSION_DATE);
          -- Статусы для заявок id_category=15
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (0,'NEW','Новая',15,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (1,'DRAFT','Черновик',15,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (2,'REGISTERED','Зарегистрировано',15,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (3,'CANCELLED','Отменена',15,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (4,'DENIED','Не допущен',15,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (5,'ACCORDANCE','Допущен',15,V_ID_DATA_SOURCE,V_VERSION_DATE);
          -- Статусы для процедур id_category=16
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (1,'STATUS_EDITING','Редактирование',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (2,'STATUS_SIGNED','Подписана ЭП',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (3,'STATUS_READY_FOR_PUBLICITING','Подготовлена к публикации',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (4,'STATUS_REQUEST_DENIED','Заявка на размещение заказа отклонена',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (5,'STATUS_REQUEST_APPROVED','Заявка на размещение заказа согласована',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (6,'STATUS_TIMING_IS_SET','Установлены сроки размещения',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (7,'STATUS_ANNOUNCEMENT_PUBLISHED','Извещение опубликовано',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (8,'STATUS_SENT_PUBLIC_DISCUSSION','Отправлено на общественное обсуждение',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (9,'STATUS_CANCELED','Отменена',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (10,'STATUS_GRBS_APPROVED','Согласована РГ ГРБС',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (11,'STATUS_REQUEST_SENT','Подана заявка на размещение (На рассмотрении ТК)',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (12,'STATUS_GRBS_PENDING','На рассмотрении РГ ГРБС',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (13,'STATUS_GRBS_ISSUED_NOTE','Выданы замечания ГРБС',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (14,'STATUS_GRBS_CANCELED','Отклонено РГ ГРБС',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (15,'STATUS_SENT_NMC_EXAMINATION','Отправлена на экспертизу НМЦ',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (16,'STATUS_NMC_EXAMINATION','Пройдена экспертиза НМЦ',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (17,'STATUS_DKP_INFO_REQUEST','Необходимы разъяснения для ДКП',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (18,'STATUS_TRADE_COMPLETED','Торги завершены',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (19,'STATUS_WAITING_PUBLISHING','Ожидание публикации',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (20,'STATUS_DKP_ISSUED_NOTE','Выданы предписания ДКП',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (21,'STATUS_DKP_NOTE_EXECUTED','Предписания ДКП исполнены',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (22,'REMOVED','Удалён',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (23,'STATUS_TIMING','Назначение сроков',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (24,'STATUS_PUBLISHING_ERROR','Ошибка публикации',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (25,'STATUS_MAKING_MODIFICATIONS','Внесение изменений',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (26,'STATUS_GRBS_NOTE_EXECUTED','Замечания ГРБС исполнены',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (27,'STATUS_PUBLISH_APPROVING_GRBS_REQUEST_SENT','Согласование публикации - отправлен запрос на публикацию для согласования ГРБС',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (28,'STATUS_PUBLISH_APPROVED_GRBS_REQUEST','согласована ГРБС для публикации (На публикации)',16,V_ID_DATA_SOURCE,V_VERSION_DATE);
          -- Состояние процедур id_category=17
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (1,'STATE_PREPARING_FOR_PUBLISHING','',17,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (2,'STATE_SENT_FOR_PUBLISHING','',17,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (3,'STATE_PUBLISHING_APPROVED','',17,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (4,'STATE_PUBLISHED','',17,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (5,'STATE_APPROVED','',17,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (6,'STATE_TRADE_СOMPLETED','Торги завершены',17,V_ID_DATA_SOURCE,V_VERSION_DATE);
          -- Cостояние участников торгов id_category=18
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (0,'NEW','Новая',18,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (1,'DRAFT','',18,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (2,'REGISTERED','',18,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (3,'CANCELLED','Отменена',18,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (4,'DENIED','Не допущен',18,V_ID_DATA_SOURCE,V_VERSION_DATE);
          INSERT INTO REPORTS.SP_STATUS (ID,CODE,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) VALUES (5,'ACCORDANCE','Допущен',18,V_ID_DATA_SOURCE,V_VERSION_DATE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_MER_CODE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_MER_CODE';
    rec_array(idx).sql_name := 'SP_MER_CODE [EAIST2]';
    rec_array(idx).description := 'Справочник МЭР (Минэкономразвитие)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_MER_CODE (ID,
                                      CODE,
                                      NAME,
                                      OKEI_NAME,
                                      MER_TYPE,
                                      ID_PARENT,
                                      ID_DATA_SOURCE,
                                      VERSION_DATE)
            SELECT mk.ID,
                   mk.CODE,
                   mk.NAME,                   
                   mk.OKEI_NAME,
                   mk.MER_TYPE,
                   mk.PARENT_ID,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM N_MER_CODE@EAIST_MOS_NSI  mk
              WHERE deleted_date IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_MER_CODE - PARENT_ID [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_MER_CODE';
    rec_array(idx).sql_name := 'SP_MER_CODE - PARENT_ID [EAIST2]';
    rec_array(idx).description := 'Обновление PARENT_ID';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      --Обновление PARENT_ID
      merge into sp_mer_code mer
      using (select pmer.id parent_id, umer.id 
              from sp_mer_code umer
              inner join sp_mer_code pmer on umer.version_date=pmer.version_date and umer.id_data_source=pmer.id_data_source and umer.id_parent is null
              and pmer.id_parent is not null and 
              substr(pmer.code,1,instr(pmer.code,'.',1,regexp_count(pmer.code,'\.'))-1)=substr(umer.code,1,instr(umer.code,'.',1,regexp_count(umer.code,'\.'))-1)
              and substr(pmer.code,instr(pmer.code,'.',1,regexp_count(pmer.code,'\.'))+1,length(pmer.code)-instr(pmer.code,'.',1,regexp_count(pmer.code,'\.'))+1) like '0%0'
              and substr(umer.code,instr(umer.code,'.',1,regexp_count(umer.code,'\.'))+1,length(umer.code)-instr(umer.code,'.',1,regexp_count(umer.code,'\.'))+1) not like '0%0'
              and lower(pmer.code) not like '%разд%' and lower(umer.code) not like '%разд%' and umer.code like '%.%'
              where umer.version_date=V_VERSION_DATE) pmer
      on (mer.id_data_source=V_ID_DATA_SOURCE and mer.version_date=V_VERSION_DATE and mer.id=pmer.id)
      when matched then update set
      mer.id_parent=pmer.parent_id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KPGZ [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KPGZ';
    rec_array(idx).sql_name := 'SP_KPGZ [EAIST2]';
    rec_array(idx).description := 'Классификатор предметов государственного заказа (КПГЗ)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_KPGZ (ID,
                                      CODE,
                                      DESCRIPTION,
                                      NAME,
                                      ID_PARENT,
                                      ID_DATA_SOURCE,
                                      VERSION_DATE,
                                      ID_OKPD,
                                      ID_MER,
                                      DELETED_DATE)
            SELECT nk.ID,
                   nk.CODE,
                   nk.DESCRIPTION,
                   nk.NAME,
                   nk.PARENT_ID,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   o.id OKPD,
                   nk.MER_CODE_ID,
                   NULL
              FROM    (SELECT ID,
                              CODE,
                              DESCRIPTION,
                              NAME,
                              PARENT_ID,
                              OKPD_ID,
                              MER_CODE_ID
                         FROM N_KPGZ@EAIST_MOS_NSI
                        WHERE DELETED_DATE IS NULL) nk
                   LEFT JOIN
                      n_okpd@EAIST_MOS_NSI o
                   ON nk.OKPD_ID = o.id
            UNION ALL
            SELECT nkn.ID,
                   nkn.CODE,
                   nkn.DESCRIPTION,
                   nkn.NAME,
                   NULL,--nkn.PARENT_ID,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   o.id OKPD,
                   nkn.MER_CODE_ID,
                   nkn.DELETED_DATE
            FROM (SELECT max(ID) ID,
                                        ENTITY_ID
                                   FROM N_KPGZ@EAIST_MOS_NSI
                                  WHERE DELETED_DATE IS NOT NULL
                                  GROUP BY ENTITY_ID) nk
            INNER JOIN (SELECT ID,
                                        CODE,
                                        DESCRIPTION,
                                        NAME,
                                        PARENT_ID,
                                        OKPD_ID,
                                        MER_CODE_ID,
                                        ENTITY_ID,
                                        DELETED_DATE
                                   FROM N_KPGZ@EAIST_MOS_NSI) nkn on nk.id=nkn.id and nk.entity_id=nkn.entity_id
            LEFT JOIN (SELECT ENTITY_ID, ID FROM N_KPGZ@EAIST_MOS_NSI
                       WHERE DELETED_DATE IS NULL) dnk on nk.ENTITY_ID=dnk.ENTITY_ID
            LEFT JOIN n_okpd@EAIST_MOS_NSI o ON nkn.OKPD_ID = o.id
            WHERE dnk.ID is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KPGZ - DELETED_HIERARCHY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KPGZ';
    rec_array(idx).sql_name := 'SP_KPGZ - DELETED_HIERARCHY [EAIST2]';
    rec_array(idx).description := 'КПГЗ - иерархия удаленных';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        merge into sp_kpgz sk
        using (select code, min(id) id from sp_kpgz where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE and deleted_date is null
               group by code
                union all
               select code, min(id) id from sp_kpgz where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE and deleted_date is not null
               and code not in (select code from sp_kpgz where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE and deleted_date is null)
               group by code) skp
        on ( substr(sk.code,1,length(skp.code))=skp.code and length(sk.code)-length(skp.code) between 3 and 4 and sk.code like skp.code||'.%'
            and sk.id_data_source=V_ID_DATA_SOURCE and sk.version_date=V_VERSION_DATE and sk.code not like '__' and sk.deleted_date is not null)
        when matched then update set
        sk.ID_PARENT=skp.id
        where sk.ID_PARENT is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KPGZ - 2,3 LVL [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KPGZ';
    rec_array(idx).sql_name := 'SP_KPGZ - 2,3 LVL [EAIST2]';
    rec_array(idx).description := 'Простановка КПГЗ 2 и 3 уровня';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
           MERGE INTO SP_KPGZ sk
           USING (
              SELECT sk.id, sk.id_data_source, sk.version_date, sk2.id AS parent_id
              FROM sp_KPGZ sk
              INNER JOIN (select id, code from sp_kpgz where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE and INSTR(code,'.',1,2)=0 and deleted_date is null
                          union all
                          select min(id) id, code from sp_kpgz where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE and INSTR(code,'.',1,2)=0 and deleted_date is not null
                          and code not in (select code from sp_kpgz where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE and INSTR(code,'.',1,2)=0 and deleted_date is null)
                          group by code ) sk2 ON 
              substr(sk.code,1,INSTR(sk.code,'.',1,2)-1)=substr(sk2.code,1,INSTR(sk.code,'.',1,2)-1) AND INSTR(sk2.code,'.',1,2)=0 
              AND sk.version_date=V_VERSION_DATE AND sk.id_data_source=V_ID_DATA_SOURCE) t
           ON (sk.id=t.id AND sk.id_data_source=t.id_data_source AND sk.version_date=t.version_date AND sk.version_date=V_VERSION_DATE
           AND sk.id_data_source=V_ID_DATA_SOURCE)
           WHEN MATCHED THEN UPDATE SET
              sk.ID_PARENT_LEVEL2=t.parent_id;
              
            MERGE INTO SP_KPGZ sk
            USING (
              SELECT sk.id, sk.id_data_source, sk.version_date, sk3.id AS parent_id
              FROM sp_KPGZ sk
              INNER JOIN (select id, code from sp_kpgz where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE and INSTR(code,'.',1,3)=0  and deleted_date is null
                          union all
                          select min(id) id, code from sp_kpgz where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE and INSTR(code,'.',1,3)=0  and deleted_date is not null
                          and code not in (select code from sp_kpgz where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE and  INSTR(code,'.',1,3)=0  and deleted_date is null)
                          group by code ) sk3 ON 
              substr(sk.code,1,INSTR(sk.code,'.',1,3)-1)=substr(sk3.code,1,INSTR(sk.code,'.',1,3)-1) AND INSTR(sk3.code,'.',1,3)=0 
              AND sk.version_date=V_VERSION_DATE AND sk.id_data_source=V_ID_DATA_SOURCE) t 
            ON (sk.id=t.id AND sk.id_data_source=t.id_data_source AND sk.version_date=t.version_date AND sk.version_date=V_VERSION_DATE
            AND sk.id_data_source=V_ID_DATA_SOURCE)
            WHEN MATCHED THEN UPDATE SET
              sk.ID_PARENT_LEVEL3=t.parent_id; 

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_OKDP_INNOVATION [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_OKDP_INNOVATION';
    rec_array(idx).sql_name := 'SP_OKDP_INNOVATION [EAIST2]';
    rec_array(idx).description := 'Cправочник инновационной продукции ОКПД';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO SP_OKDP_INNOVATION (ID,
                                         NAME,
                                         ID_DATA_SOURCE,
                                         VERSION_DATE)
            SELECT ID,
                   NAME,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM n_okpd@EAIST_MOS_NSI
             WHERE INNOVATION_PRODUCT = 1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_OKDP [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_OKDP';
    rec_array(idx).sql_name := 'SP_OKDP [EAIST2]';
    rec_array(idx).description := 'Справочник  ОКПД';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into sp_okpd (id, code, name, description, parent_id, auction_product, innovation_product, rus, uis, id_Data_source, version_date)
         select id, code, name, description, parent_id, auction_product, innovation_product, rus, uis, V_ID_DATA_SOURCE, V_VERSION_DATE
         FROM n_okpd@EAIST_MOS_NSI where deleted_date is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SPGZ [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SPGZ';
    rec_array(idx).sql_name := 'SP_SPGZ [EAIST2]';
    rec_array(idx).description := 'Спецификация предметов государственного заказа (СПГЗ)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_SPGZ (ID,
                                      NAME,
                                      KPGZ_ID,
                                      ID_DATA_SOURCE,
                                      VERSION_DATE,
                                      entity_id,
                                      INNOVATION_PRODUCT)
            SELECT ID,
                   NAME,
                   KPGZ_ID,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   entity_id,
                   INNOVATION_PRODUCT
              FROM N_SPGZ@EAIST_MOS_NSI
             WHERE DELETED_DATE IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER_RATING [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER_RATING';
    rec_array(idx).sql_name := 'SP_CUSTOMER_RATING [EAIST2]';
    rec_array(idx).description := 'Справочник рейтингов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_CUSTOMER_RATING (ID,
                                                 CUSTOMER_ID,
                                                 PERIOD,
                                                 PERIOD_TYPE,
                                                 PERIOD_TEXT,
                                                 COUNT_OF_CONTRACT_DETECTED,
                                                 COUNT_OF_CUSTOMER_RESPONS,
                                                 DETECTED_GLAV_CONTROL,
                                                 YEAR,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
            SELECT ID,
                   CUSTOMER_ID,
                   CASE
                      WHEN period LIKE 'Год' THEN 'y'
                      WHEN period LIKE '1-е полугодие' THEN 'h'
                   END
                      PERIOD,
                   CASE
                      WHEN period LIKE 'Год' THEN 0
                      WHEN period LIKE '1-е полугодие' THEN 1
                   END
                      PERIOD_TYPE,
                   PERIOD PERIOD_TEXT,
                   COUNT_OF_CONTRACT_DETECTED,
                   COUNT_OF_CUSTOMER_RESPONS,
                   DETECTED_GLAV_CONTROL,
                   YEAR,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM N_CUSTOMER_RATING@EAIST_MOS_NSI
             WHERE DELETED_DATE IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CONNECTION_ORGANIZATION [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CONNECTION_ORGANIZATION';
    rec_array(idx).sql_name := 'LNK_CONNECTION_ORGANIZATION [EAIST2]';
    rec_array(idx).description := 'Рассчетные связи родителей организаций с детьми всех уровней';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        for j in (
          select * from sp_customer where ID_DATA_SOURCE = V_ID_DATA_SOURCE and version_date = V_VERSION_DATE
        ) 
        loop
          insert into LNK_CONNECTION_ORGANIZATION (id,id_child,ID_DATA_SOURCE,parent_level,child_LEVEL,VERSION_DATE)
            SELECT  j.id, id as child_id, ID_DATA_SOURCE,j.connect_level parent_level,level as child_LEVEL, VERSION_DATE
            FROM   sp_customer o    
            START WITH    id = j.id and ID_DATA_SOURCE=j.ID_DATA_SOURCE and version_date=j.version_date
            CONNECT BY     id_parent=PRIOR id and ID_DATA_SOURCE=prior ID_DATA_SOURCE and version_date=prior version_date;
        end loop;
        delete from  LNK_CONNECTION_ORGANIZATION where id=id_child;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_FINANCIAL_LIMIT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_FINANCIAL_LIMIT';
    rec_array(idx).sql_name := 'T_FINANCIAL_LIMIT [EAIST2]';
    rec_array(idx).description := 'Финансирование';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO T_FINANCIAL_LIMIT (ID,
                                        YEAR,
                                        PLANNED_AMOUNT,
                                        PRICE,
                                        CUSTOMER_ID,
                                        GRBS,
                                        FKR,
                                        CSR,
                                        KVR,
                                        KOSGU,
                                        TARGET_SUBSIDY_CODE,
                                        KBK,
                                        FINANCIAL_SOURCE_TYPE,
                                        REST,
                                        ID_DATA_SOURCE,
                                        VERSION_DATE)
            SELECT ID,
                   YEAR,
                   PLANNED_AMOUNT,
                   PRICE,
                   CUSTOMER_ID,
                   GRBS,
                   FKR,
                   CSR,
                   KVR,
                   KOSGU,
                   TARGET_SUBSIDY_CODE,
                   KBK,
                   CASE
                      WHEN FINANCIAL_SOURCE_TYPE = 'LIMIT' THEN 1
                      WHEN FINANCIAL_SOURCE_TYPE = 'PFHD' THEN 2
                      WHEN FINANCIAL_SOURCE_TYPE = 'OWN_FUNDS' THEN 3
                      WHEN FINANCIAL_SOURCE_TYPE = 'FEDERAL_FUNDS' THEN 4
                   END
                      FINANCIAL_SOURCE_TYPE,
                   REST,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM N_FINANCIAL_LIMIT@EAIST_MOS_NSI
             WHERE deleted_date IS NULL and not (FINANCIAL_SOURCE_TYPE='PFHD' and substr(code,1,1)='0');

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_TENDER [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_TENDER';
    rec_array(idx).sql_name := 'T_TENDER [EAIST2]';
    rec_array(idx).description := 'Закупки\заказы\тендер';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_TENDER (ID,
                                               PUBLICATION_DATE,
                                               ID_TENDER_TYPE,
                                               SUBJECT,
                                               TENDER_LEVEL,
                                               IS_UNION_TRADE,
                                               CREATE_PROTOCOL_DATE,
                                               REGISTRY_NUMBER,
                                               --ID_TORG_TYPE,
                                               IS_44FZ,
                                               ID_ENTERPRISE_ENTITY,
                                               PUBLISH_OOS_DATE,
                                               LAST_PROTOCOL_DATE_FROM_OOS,
                                               ID_CUSTOMER_EAIST2,
                                               UNSUCCESSFUL_PURCHASE,
                                               id_status,
                                               id_state,
                                               is_active,
                                               OOS_URL,
                                               ENTITY_ID,
                                               ID_DATA_SOURCE,
                                               VERSION_DATE,
                                               LAST_UPDATE_DATE,
                                               PUBLISH_STATUS_DATE,
                                               CHANGE_COUNT,
                                               CANCEL_DATE,
                                               REQUEST_END_DATE,
                                               FIRST_PUBLSIH_DATE,
                                               LAST_PUBLISH_DATE,
											   PREV_ENTITY_ID)

SELECT pv.ID as Id1,
                           pd.PROCEDURE_DATE AS PUBLICATION_DATE,
                           pe.METHOD_OF_SUPPLIER_ID AS ID_TENDER_TYPE, --свой справчник в коде
                           pv.PROCUREMENT_SUBJECT,
                           CASE
                              WHEN pv.ACCOMODATION_LEVEL = 'Первый' THEN 1
                              WHEN pv.ACCOMODATION_LEVEL = 'Второй' THEN 2
                              ELSE NULL
                           END
                              TENDER_LEVEL,
                           PL.JOINT_AUCTION AS IS_UNION_TRADE,
                           CASE 
                            WHEN pe.METHOD_OF_SUPPLIER_ID=4 THEN
                              CASE WHEN PV.STATUS_ID=18 or have_win.procedure_entity_id is not null THEN oos.protocol_date ELSE oos.lim_protocol_date END
                            ELSE
                              CASE 
                                WHEN (pe.METHOD_OF_SUPPLIER_ID=1 and not_ea_oos.session_type in (2,3)) or
                                      (pe.METHOD_OF_SUPPLIER_ID=2 and not_ea_oos.session_type in (5)) or
                                      (pe.METHOD_OF_SUPPLIER_ID=5 and not_ea_oos.session_type in (12)) or
                                      (pe.METHOD_OF_SUPPLIER_ID=6 and not_ea_oos.session_type in (15)) or
                                      have_win.procedure_entity_id is not null
                                      THEN not_ea_oos.protocol_created_date
                                else null
                              END
                           END AS CREATE_PROTOCOL_DATE,--nvl(oos.PROTOCOL_DATE, not_ea_oos.protocol_created_date) AS CREATE_PROTOCOL_DATE,                          
                           pe.REG_NUMBER,
                           --pv.TRADE_OBJECT_TYPE -- string value as ID_TORG_TYPE,                   
                           0 AS IS_44FZ,
                           pv.ORGANIZER_ID AS ID_ENTERPRISE_ENTITY,
                           CASE 
                            WHEN pe.METHOD_OF_SUPPLIER_ID=4 THEN
                              CASE WHEN PV.STATUS_ID=18 THEN oos.publish_date ELSE oos.lim_publish_date END
                            ELSE
                              null
                           END AS PUBLISH_OOS_DATE,
                           CASE 
                            WHEN pe.METHOD_OF_SUPPLIER_ID=4 THEN
                              CASE WHEN PV.STATUS_ID=18 THEN oos.protocol_date ELSE oos.lim_protocol_date END
                            ELSE
                              not_ea_oos.protocol_created_date
                           END AS LAST_PROTOCOL_DATE_FROM_OOS,
                           pe.CUSTOMER_ID,
                           pv.UNSUCCESSFUL_PURCHASE,
                           pv.status_id,
                           pv.state_id,
                           CASE WHEN pv.status_id = 9 THEN 0 ELSE 1 END is_active,
                           pe.oos_url,
                           pe.id,
                           V_ID_DATA_SOURCE,
                           V_VERSION_DATE,
                           pv.CREATED_DATE as LAST_UPDATE_DATE,
                           pub_pv.CREATED_DATE as PUBLISH_STATUS_DATE,
                           changeCount.CHANGE_COUNT,
                           cancel_pv.created_Date CANCEL_DATE,
                           pre.PROCEDURE_DATE as REQUEST_END_DATE,
                           dates.first_pub_date,
                           dates.last_pub_date,
						   pe.published_id
                    FROM (select id,entity_id, created_date, PROCUREMENT_SUBJECT,ACCOMODATION_LEVEL,UNSUCCESSFUL_PURCHASE,status_id,state_id, ORGANIZER_ID from D_PROCEDURE_VERSION@EAIST_MOS_SHARD where deleted_date IS NULL) pv
                    --Получаем дату создания версии когда статус перешел в опубликовано              
                    --Устраняем дубли в D_PROCEDURE_VERSION
                    JOIN (SELECT entity_id, max(created_date) created_date FROM D_PROCEDURE_VERSION@eaist_mos_shard WHERE deleted_date is null group by entity_id) max_pv
                    on max_pv.entity_id=pv.entity_id and max_pv.created_date=pv.created_date
                    --Устраняем дубли в D_PROCEDURE_ENTITY
                    JOIN (select all_p.* from D_PROCEDURE_ENTITY@EAIST_MOS_SHARD all_p
                            join (select reg_number,nvl(ent_id18,nvl(ent_id7, ent_id)) id
                                  from(
                                  select reg_number,
                                                      max(case when status_id=18 and max_ver_status=version then id else null end) ent_id18,
                                                      max(case when status_id=7 and max_ver_status=version then id else null end) ent_id7,
                                                      max(case when version=max_ver then id else null end) ent_id                    
                                                      
                                                      
                                                      from (select pe.*, pv.status_id, max(pe.version) over (partition by reg_number) max_ver, max(pe.version) over (partition by reg_number, status_id) max_ver_status 
                                                      from D_PROCEDURE_ENTITY@eaist_mos_shard pe
                                                      join (select id, entity_id, status_id, deleted_date from D_PROCEDURE_VERSION@eaist_mos_shard) pv 
                                                      on pe.id=pv.entity_id 
                                                      where (pe.published_id is null or pv.status_id in (7,18)) and pe.deleted_date is null and pv.deleted_date is null) dat
                                                      group by reg_number)) filtr_p   
                          on all_p.id=filtr_p.id)pe 
                          ON pe.ID = pv.entity_id
                    
                    
                          
                    --Получаем общее число изменений по процедуре
                    LEFT JOIN (SELECT distinct entity_id, min(created_date) created_date FROM D_PROCEDURE_VERSION@eaist_mos_shard WHERE status_id=7 group by entity_id) pub_pv
                    on pub_pv.entity_id=pv.entity_id --and pub_pv.created_date=pv.created_date
                     --Получаем дату создания версии когда статус перешел в отменено
                    LEFT JOIN (SELECT distinct entity_id, min(created_date) created_date FROM D_PROCEDURE_VERSION@eaist_mos_shard WHERE status_id=9 group by entity_id) cancel_pv
                    on cancel_pv.entity_id=pv.entity_id --and pub_pv.created_date=pv.created_date
                    
                    LEFT JOIN (select count(*) change_count, pe.reg_number from D_PROCEDURE_ENTITY@EAIST_MOS_SHARD pe
                              JOIN D_PROCEDURE_VERSION@EAIST_MOS_SHARD pv ON pe.id=pv.entity_id
                              GROUP BY pe.reg_number) changeCount ON changeCount.reg_number=pe.reg_number
                          
                    LEFT JOIN D_PROCEDURE_DATE@EAIST_MOS_SHARD pd ON pd.PROCEDURE_ID = pv.ID AND pd.DATE_TYPE = 'publicationDate' AND pd.deleted_date IS NULL
                    LEFT JOIN D_PROCEDURE_DATE@EAIST_MOS_SHARD pre ON pre.PROCEDURE_ID = pv.ID AND pre.DATE_TYPE = 'receivingEndDateTime' AND pre.deleted_date IS NULL
                    LEFT JOIN (select oos_nolim.PROCEDURE_ENTITY_ID, oos_nolim.PUBLISH_DATE, oos_nolim.PROTOCOL_DATE, oos_lim.PUBLISH_DATE LIM_PUBLISH_DATE, oos_lim.PROTOCOL_DATE LIM_PROTOCOL_DATE from 
                              (SELECT DISTINCT PUBLISH_DATE, PROTOCOL_DATE, PROCEDURE_ENTITY_ID FROM
                              (SELECT  PUBLISH_DATE,
                                        PROTOCOL_DATE,
                                        PROCEDURE_ENTITY_ID,
                                        max(PROTOCOL_DATE)  OVER (PARTITION BY PROCEDURE_ENTITY_ID) MAX_PROTOCOL_DATE,
                                        max(PUBLISH_DATE)  OVER (PARTITION BY PROCEDURE_ENTITY_ID) MAX_PUBLISH_DATE
                                       FROM D_OOS_FTP_PROTOCOL@EAIST_MOS_SHARD) where PROTOCOL_DATE=MAX_PROTOCOL_DATE and PUBLISH_DATE=MAX_PUBLISH_DATE) oos_nolim
                              left join
                              (SELECT DISTINCT PUBLISH_DATE, PROTOCOL_DATE, PROCEDURE_ENTITY_ID FROM
                              (SELECT  PUBLISH_DATE,
                                        PROTOCOL_DATE,
                                        PROCEDURE_ENTITY_ID,
                                        max(CASE WHEN PROTOCOL_TYPE in ('PPI', 'PPN') THEN PROTOCOL_DATE ELSE NULL END)  OVER (PARTITION BY PROCEDURE_ENTITY_ID) MAX_LIM_PROTOCOL_DATE,
                                        max(CASE WHEN PROTOCOL_TYPE in ('PPI', 'PPN') THEN PUBLISH_DATE ELSE NULL END)  OVER (PARTITION BY PROCEDURE_ENTITY_ID) MAX_LIM_PUBLISH_DATE
                                       FROM D_OOS_FTP_PROTOCOL@EAIST_MOS_SHARD) where PROTOCOL_DATE=MAX_LIM_PROTOCOL_DATE and PUBLISH_DATE=MAX_LIM_PUBLISH_DATE) oos_lim
                              on oos_nolim.PROCEDURE_ENTITY_ID=oos_lim.PROCEDURE_ENTITY_ID) oos
                    ON oos.PROCEDURE_ENTITY_ID = pe.ID
                    LEFT JOIN (select distinct nvl(protocol_created_date, created_Date) protocol_created_date, procedure_id, session_type from (
                        select com.*, 
                                max(protocol_number) over (partition by procedure_Id) max_protocol,
                                max(version) over (partition by procedure_Id, protocol_number) max_version,
                                max(protocol_created_date) over (partition by procedure_Id, protocol_number, version) max_protocol_date
                        from d_commission_session@eaist_mos_shard com  where session_type not in (1,18) and deleted_date is null
                        ) where protocol_number= max_protocol and version=max_version and (max_protocol_date=protocol_created_date or protocol_created_date is null)) not_ea_oos
                    ON not_ea_oos.procedure_id=pe.id
                    LEFT JOIN (SELECT distinct lv.joint_auction, pl.PROCEDURE_ENTITY_ID FROM D_PROCEDURE_LOT_ENTRY@EAIST_MOS_SHARD pl
                                        JOIN (select id , joint_auction from d_lot_version@eaist_mos_shard where deleted_date IS NULL ) lv 
                                        ON pl.lot_id = lv.id AND pl.is_actual = 1 and lv.joint_auction=1) pl
                               on pl.PROCEDURE_ENTITY_ID = pe.ID
                    LEFT JOIN (select distinct entity_id,
                                min(case when ver.status_id=7 then ver.created_date else null end) over (partition by ent.reg_number) first_pub_date,
                                max(case when ver.status_id=7 then ver.created_date else null end) over (partition by ver.entity_id) last_pub_date
                                FROM (select entity_id, status_id, createD_date from D_PROCEDURE_VERSION@EAIST_MOS_SHARD) ver
                                JOIN D_PROCEDURE_ENTITY@EAIST_MOS_SHARD ent
                                ON ver.entity_id=ent.id) dates on dates.entity_id=pe.id
                    LEFT JOIN (select distinct procedure_entity_id from (
                              select procedure_Id procedure_entity_id, place from d_ea_winner@eaist_mos_shard
                              union
                              SELECT                           
                                procedure_entity_id,                          
                                row_number() over (partition by LOT_ID order by rank desc) PLACE                          
                                FROM
                                (
                                SELECT DISTINCT 
                                        b.lot_id,
                                       nvl(ple.procedure_entity_id, ple1.procedure_entity_id) procedure_entity_id,
                                       b.FINAL_RATING AS RANK
                        
                                  FROM D_BID@EAIST_MOS_SHARD b
                                  JOIN D_LOT_VERSION@EAIST_MOS_SHARD LV on  LV.ENTITY_ID=B.LOT_ID
                                  left JOIN D_PROCEDURE_LOT_ENTRY@EAIST_MOS_SHARD ple ON lv.ID = ple.LOT_ID AND is_actual = 1
                                  left join 
                                  (select lle.lot_id, procedure_entity_id, lot_num 
                                      from (select id, lot_id, root_lot_id, max(id) over (partition by lot_id) max_id from d_lot_lot_entry@eaist_mos_shard  where is_actual = 1) lle --ищем связь совместных лотов с главными совместными лотами
                                      left join d_procedure_lot_entry@eaist_mos_shard ple1 on lle.id=lle.max_id and ple1.lot_id = lle.root_lot_id and ple1.is_actual = 1 --ищем связь главных совместных лотов с процедурами
                                    where PROCEDURE_ID IN (SELECT ID FROM D_PROCEDURE_VERSION@EAIST_MOS_SHARD WHERE deleted_date IS NULL)) ple1  
                                  on lv.id = ple1.lot_id )
                                  ) where place=1) have_win on have_win.procedure_entity_id=pe.id        
                                
                    WHERE EXISTS (SELECT * FROM D_PROCEDURE_LOT_ENTRY@EAIST_MOS_SHARD pl
                      JOIN (SELECT l.ID, l.entity_id FROM D_LOT_VERSION@EAIST_MOS_SHARD l WHERE deleted_date IS NULL
                              union all
                             (SELECT l.ID,
                                   l.ENTITY_ID
                              FROM D_LOT_VERSION@EAIST_MOS_SHARD l
                              join (select distinct max(id)  over (partition by entity_id) maxid, entity_id from D_LOT_VERSION@EAIST_MOS_SHARD) maxl
                              on l.entity_id=maxl.entity_id and l.id=maxl.maxid)) lv ON pl.lot_id = lv.id AND pl.is_actual = 1
                        WHERE pl.PROCEDURE_ENTITY_ID = pe.ID);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT';
    rec_array(idx).sql_name := 'T_LOT [EAIST2]';
    rec_array(idx).description := 'Лот';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_LOT (ID,
                                    CONCLUSION_REASON,
                                    COST,
                                    DESCRIPTION,
                                    LOT_REGISTRY_NUMBER_PLAN,
                                    LOT_REGISTRY_NUMBER,
                                    LOT_NUMBER,
                                    MAXIMUM_CONTRACT_COST,
                                    NAME,
                                    ID_METHODOFSUPPLIER,
                                    TENDER_ID,
                                    IS_UNIT,
                                    IS_SMALL,
                                    IS_MULTI_WINNER,
                                    ID_PURCHASE_SCHEDULE,
                                    customer_id,
                                    id_entity,
                                    WAS_PUBLISHED_OLD,
                                    id_status,
                                    PURCHASE_START_DATE--PLAN_PUBLICATION_DATE--,FACT_PUBLICATION_DATE --из тендеров
                                    ,
                                    is_active,
                                    APPROVED_DATE,
                                    ID_DATA_SOURCE,
                                    VERSION_DATE,
                                    IS_AUCTION_TO_INCREASE,
                                    JOINT_AUCTION,
                                    first_publish_date,
                                    last_publish_date,
                                    approve_before_pub_date,
                                    lot_created_date,
                                    ORDER_PERCENT_AMOUNT,
                                    GUARANTEE_PERCENT_AMOUNT,
                                    last_change_publish_date,
                                    first_approve_date,
                                    last_change_approve_date,
									prev_entity_id)
			SELECT lv.ID,
							   lv.REASON_CONCLUSION_CONTRACT,
							   lv.CONTRACT_NMC AS COST,
							   NULL AS DESCRIPTION,
							   le.REGISTRY_NUMBER,
							   p.REGISTRY_NUMBER TENDER_REG_NUMBER,
							   lv.LOT_NUM,
							   li.NMC AS MAXIMUM_CONTRACT_COST,
							   lv.LOT_NAME,
							   lv.METHOD_OF_SUPPLIER_ID,
							   lv.TENDER_ID,
							   lv.bidding_on_unit_production AS IS_UNIT,
							   CASE
								  WHEN lv.SMP_TYPE = 'none' THEN 0
								  WHEN lv.SMP_TYPE = 'entirely' THEN 1
								  WHEN lv.SMP_TYPE = 'partly' THEN 2
								  ELSE NULL
							   END
								  IS_SMALL,
							   lv.CAN_MULTIPLE AS IS_MULTI_WINNER,
							   PSLE.PLAN_SCHEDULE_ID,
							   LE.CUSTOMER_ID,
							   le.id AS liid,
							   le.WAS_PUBLISHED,
							   lv.STATUS_ID,
							   LV.START_DATE,--это дата плановой публикации процедуры закупки, в которую войдет данный лот
							   CASE WHEN lv.STATUS_ID IN (11, 6) THEN 0 ELSE 1 END
								  is_active,
							   dates.last_approve_date,--lsh.status_date, 
							   V_ID_DATA_SOURCE,
							   V_VERSION_DATE,
							   CASE
								WHEN (lv.METHOD_OF_SUPPLIER_ID=4) AND (lv.CONTRACT_NMC=0) THEN 1
								else 0
							   END IS_AUCTION_TO_INCREASE,
							   lv.joint_auction,
							   dates.first_pub_date,
							   dates.last_pub_date,
							   dates.approve_before_pub_date,
							   dates.created_date,
							   lv.ORDER_PERCENT_AMOUNT,
							   lv.GUARANTEE_PERCENT_AMOUNT,
							   dates.last_change_pub_date,
							   dates.first_approve_date,
							   dates.last_change_approve_date,
							   le.published_id
			FROM (select  lv.*,  nvl(pl.PROCEDURE_ID,ple1.PROCEDURE_ID) AS TENDER_ID, nvl(pl.LOT_NUM,ple1.LOT_NUM) AS LOT_NUM
				  FROM (select 
											nvl(lv.ID, maxlv.ID) ID,
											nvl(lv.ENTITY_ID, maxlv.ENTITY_ID) ENTITY_ID,
											nvl(lv.REASON_CONCLUSION_CONTRACT, maxlv.REASON_CONCLUSION_CONTRACT) REASON_CONCLUSION_CONTRACT,
											nvl(lv.CONTRACT_NMC, maxlv.CONTRACT_NMC) CONTRACT_NMC,
											nvl(lv.LOT_NAME, maxlv.LOT_NAME) LOT_NAME,
											nvl(lv.METHOD_OF_SUPPLIER_ID, maxlv.METHOD_OF_SUPPLIER_ID) METHOD_OF_SUPPLIER_ID,
											nvl(lv.SMP_TYPE, maxlv.SMP_TYPE) SMP_TYPE,
											nvl(lv.CAN_MULTIPLE, maxlv.CAN_MULTIPLE) CAN_MULTIPLE,
											nvl(lv.STATUS_ID, maxlv.STATUS_ID) STATUS_ID,
											nvl(lv.START_DATE, maxlv.START_DATE) START_DATE,
											nvl(lv.bidding_on_unit_production, maxlv.bidding_on_unit_production) bidding_on_unit_production,
											nvl(lv.joint_auction, maxlv.joint_auction) joint_auction,
											nvl(lv.ORDER_PERCENT_AMOUNT, maxlv.ORDER_PERCENT_AMOUNT) ORDER_PERCENT_AMOUNT,
											nvl(lv.GUARANTEE_PERCENT_AMOUNT, maxlv.GUARANTEE_PERCENT_AMOUNT) GUARANTEE_PERCENT_AMOUNT
											from 
											(SELECT l.ID,
																			l.ENTITY_ID,
																			l.REASON_CONCLUSION_CONTRACT,
																			l.CONTRACT_NMC,
																			l.LOT_NAME,
																			l.METHOD_OF_SUPPLIER_ID,
																			l.SMP_TYPE,
																			l.CAN_MULTIPLE,
																			l.STATUS_ID,
																			L.START_DATE,
																			l.bidding_on_unit_production,
																			l.joint_auction,
																			l.ORDER_PERCENT_AMOUNT,
																			l.GUARANTEE_PERCENT_AMOUNT
																	   FROM D_LOT_VERSION@EAIST_MOS_SHARD l
																	  WHERE deleted_date IS NULL) lv 
											FULL JOIN                                                   
											(SELECT l.ID,
													l.ENTITY_ID,
													l.REASON_CONCLUSION_CONTRACT,
													l.CONTRACT_NMC,
													l.LOT_NAME,
													l.METHOD_OF_SUPPLIER_ID,
													l.SMP_TYPE,
													l.CAN_MULTIPLE,
													l.STATUS_ID,
													L.START_DATE,
													l.bidding_on_unit_production,
													l.joint_auction,
													l.ORDER_PERCENT_AMOUNT,
													l.GUARANTEE_PERCENT_AMOUNT
											   FROM D_LOT_VERSION@EAIST_MOS_SHARD l
											   join
												(select distinct max(id)  over (partition by entity_id) maxid, entity_id from D_LOT_VERSION@EAIST_MOS_SHARD) maxl
												on l.entity_id=maxl.entity_id and l.id=maxl.maxid) maxlv
											 ON lv.entity_id=maxlv.entity_id) lv 
				  left join 
										(select lle.lot_id, procedure_id, lot_num from (select id, lot_id, root_lot_id, max(id) over (partition by lot_id) max_id from d_lot_lot_entry@eaist_mos_shard  where is_actual = 1) lle --ищем связь совместных лотов с главными совместными лотами
										left join d_procedure_lot_entry@eaist_mos_shard ple1 on lle.id=lle.max_id and ple1.lot_id = lle.root_lot_id and ple1.is_actual = 1 --ищем связь главных совместных лотов с процедурами
										where PROCEDURE_ID IN (SELECT ID FROM D_PROCEDURE_VERSION@EAIST_MOS_SHARD WHERE deleted_date IS NULL))ple1  on lv.id = ple1.lot_id                           
				  LEFT JOIN (select apl.* from (select * from D_PROCEDURE_LOT_ENTRY@EAIST_MOS_SHARD where IS_ACTUAL = 1
												and PROCEDURE_ID IN (SELECT ID
																   FROM D_PROCEDURE_VERSION@EAIST_MOS_SHARD
																  WHERE deleted_date IS NULL) ) apl
											  JOIN --Исключаем лоты соединенные с несколькими процедурами
											  (select lot_id from D_PROCEDURE_LOT_ENTRY@EAIST_MOS_SHARD where IS_ACTUAL = 1
												and PROCEDURE_ID IN (SELECT ID
																	 FROM D_PROCEDURE_VERSION@EAIST_MOS_SHARD
																	WHERE deleted_date IS NULL) 
												group by lot_id having count(lot_id)=1 order by lot_id) spl
											  on apl.lot_id=spl.lot_id)  pl ON  lv.ID = pl.LOT_ID) lv
			JOIN D_LOT_ENTITY@EAIST_MOS_SHARD le ON lv.ENTITY_ID=le.ID
			LEFT JOIN (select * from T_TENDER WHERE ID_DATA_SOURCE=2 and VERSION_DATE=TRUNC(SYSDATE)) p on p.ID=lv.TENDER_ID
			LEFT JOIN (select distinct 
										  entity_id,
										  first_pub_date,
										  last_change_pub_date,
										  first_approve_date,
										  last_change_approve_date,
										  last_pub_date,
										  last_approve_date,
										  created_date,
										  max(case when (approved_date<first_pub_date) or first_pub_date is null then approved_date else null end) over (partition by entity_id) approve_before_pub_date
										  from (                
										  select distinct ver.entity_id,
														  ver.created_date,
																  min(case when stat.status_id=5 then status_date else null end) over (partition by ent.REGISTRY_NUMBER) first_pub_date,
																  max(case when stat.status_id=5 and ver.id=ver.max_id then status_date else null end) over (partition by ver.entity_id) last_change_pub_date,
																  max(case when stat.status_id=5 then status_date else null end) over (partition by ver.entity_id) last_pub_date,
																  min(case when stat.status_id=2 then status_date else null end) over (partition by ent.REGISTRY_NUMBER) first_approve_date,
																  max(case when stat.status_id=2 and ver.id=ver.max_id then status_date else null end) over (partition by ver.entity_id) last_change_approve_date,
																  max(case when stat.status_id=2 then status_date else null end) over (partition by ver.entity_id) last_approve_date,
																  case when stat.status_id=2 then status_date else null end approved_date
																  FROM (select v.id, v.entity_id, max(id) over (partition by entity_id) max_id, min(created_date) over (partition by entity_id) created_date  from D_LOT_VERSION@EAIST_MOS_SHARD v) ver
																  JOIN D_LOT_STATUS_HISTORY@EAIST_MOS_SHARD stat
																  ON ver.id=stat.version_id
                                  JOIN D_LOT_ENTITY@EAIST_MOS_SHARD ent
                                  ON ver.entity_Id=ent.id
																  )) dates ON dates.entity_id=le.id
			LEFT JOIN D_LOT_INDEX@EAIST_MOS_SHARD li ON le.ID = li.ID
			LEFT JOIN (  SELECT MAX (PLAN_SCHEDULE_ID) PLAN_SCHEDULE_ID,
												   LOT_ID
											  FROM D_PLAN_SCHEDULE_LOT_ENTRY@EAIST_MOS_SHARD
											 WHERE is_actual = 1 and plan_schedule_id in (select id from d_plan_schedule_version@eaist_mos_shard where deleted_date is null)
										  GROUP BY LOT_ID) psle
								  ON lv.id = psle.LOT_ID                      
			where (le.published_id is null or p.id_status in (7,18) or p.id_status is null);


    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT - WAS_PUBLISHED [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT';
    rec_array(idx).sql_name := 'T_LOT - WAS_PUBLISHED [EAIST2]';
    rec_array(idx).description := 'Обновление was_published';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        --Обновление WAS_PUBLISHED
      merge into t_lot trg using
      (select id, id_Data_source, V_VERSION_DATE version_date,
      max((case when (id_status=5) or (was_published=1) then 1 else 0 end)) was_published
      from t_lot where version_date<=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE group by id, id_data_source) src
      ON (trg.id=src.id and trg.id_Data_source=src.id_Data_source and trg.version_date=src.version_date)
      when matched then
      update set trg.was_published=src.was_published;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT - PUBLICATION DATES [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT';
    rec_array(idx).sql_name := 'T_LOT - PUBLICATION DATES [EAIST2]';
    rec_array(idx).description := 'Обновление publication_date';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        --Обновление publication dates из T_TENDER 
        merge into t_lot trg using
          (select l.id, t.publication_date, t.publish_oos_date 
          from (select * from t_lot where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE) l
          left join (select * from t_tender where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE) t 
          on l.tender_id=t.id 
          where publication_date is not null or publish_oos_date is not null) src
        on (trg.id=src.id)
        when matched then
        update set trg.plan_publication_date = src.publication_date, trg.FACT_PUBLICATION_DATE=src.publish_oos_date;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT_PRICE_CUTTING [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT_PRICE_CUTTING';
    rec_array(idx).sql_name := 'T_LOT_PRICE_CUTTING [EAIST2]';
    rec_array(idx).description := 'Снижение НМЦ лота';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       INSERT INTO REPORTS.T_LOT_PRICE_CUTTING (ID, EXAMINATION_TYPE,SUM_BEFORE, SUM_AFTER,LOT_ID,PRICE_CUTTING_DATE,SAVINGS,STATUS,ID_DATA_SOURCE,VERSION_DATE)
         select lp.ID, EXAMINATION_TYPE,nmc SUM_BEFORE, nmc-savings SUM_AFTER,LOT_ID,answer_date PRICE_CUTTING_DATE, savings, status_id,V_ID_DATA_SOURCE,V_VERSION_DATE from d_lot_nmc@eaist_mos_shard lp
         join t_lot l on lp.LOT_ID=l.ID
         where  ID_DATA_SOURCE=V_ID_DATA_SOURCE and VERSION_DATE=V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT_MEMBER [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT_MEMBER';
    rec_array(idx).sql_name := 'T_LOT_MEMBER [EAIST2]';
    rec_array(idx).description := 'Участник, подавший заявку';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_LOT_MEMBER (ID,
                                           ID_LOT,
                                           ID_SUPPLIER,
                                           REQUEST_NUMBER,
                                           RANK,
                                           STATE,
                                           REGISTRATION_NUMBER,
                                           PLACE,
                                           METHOD_OF_SUPPLIER_ID,
                                           ID_DATA_SOURCE,
                                           VERSION_DATE)
            SELECT 
            DAT.b_id,
            LOT_ID,
            ID_SUPPLIER,
            REQUEST_NUMBER,
            RANK,
            STATE,
            REGISTRATION_NUMBER,
            row_number() over (partition by LOT_ID order by rank desc) PLACE,
            ID_METHODOFSUPPLIER,
            V_ID_DATA_SOURCE,
            V_VERSION_DATE
            FROM
            (
            SELECT DISTINCT b.ID||ID_METHODOFSUPPLIER B_ID,
                   lv.id LOT_ID,--b.LOT_ID AS ID_LOT,
                   b.SUPPLIER_ID AS ID_SUPPLIER,
                   MAX(br.ID) OVER (PARTITION BY B.ID) AS REQUEST_NUMBER,
                   b.FINAL_RATING AS RANK,
                   b.bid_status AS STATE, 
                   b.REGISTRATION_NUMBER,
                   lv.ID_METHODOFSUPPLIER,
                   V_ID_DATA_SOURCE
                   ,V_VERSION_DATE
    
              FROM D_BID@EAIST_MOS_SHARD b
                   --JOIN  D_LOT_ENTITY@EAIST_MOS_SHARD le ON b.LOT_ID = le.ID
                   --JOIN (SELECT ID, ENTITY_ID FROM D_LOT_VERSION@EAIST_MOS_SHARD WHERE deleted_date IS NULL) lv ON lv.entity_id = le.ID
                   JOIN t_lot LV on  LV.ID_ENTITY=B.LOT_ID AND LV.ID_DATA_SOURCE=V_ID_DATA_SOURCE AND LV.VERSION_DATE=V_VERSION_DATE-1
                  JOIN D_PROCEDURE_LOT_ENTRY@EAIST_MOS_SHARD ple ON lv.ID = ple.LOT_ID AND is_actual = 1 
                   LEFT JOIN (SELECT b.*,ROW_NUMBER () OVER (PARTITION BY b.PROCEDURE_VERSION_ID ORDER BY b.id DESC) rn FROM D_BID_REGISTRY@EAIST_MOS_SHARD b) br 
                   --Номер заявки по журналу учета заявок - для одной процедуры их несколько - баг данных, берем по max(id)
                      ON ple.procedure_id = br.PROCEDURE_VERSION_ID AND br.rn = 1 AND ple.is_actual = 1) DAT;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_MEMBER_BID [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_MEMBER_BID';
    rec_array(idx).sql_name := 'T_MEMBER_BID [EAIST2]';
    rec_array(idx).description := 'Ценовое предложение участника';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_MEMBER_BID (ID,
                                           LOT_MEMBER_ID,
                                           PRICE,
                                           BID_DATE,
                                           ID_DATA_SOURCE,
                                           VERSION_DATE,
                                           ID_STATUS)
            SELECT bp.ID,
                   bp.BID_ID||LV.ID_METHODOFSUPPLIER AS LOT_MEMBER_ID,
                   b.CONTRACT_PRICE AS PRICE,
                   b.SUBMIT_DATE BID_DATE,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   b.BID_STATUS
              FROM    D_BID_PROPOSAL@EAIST_MOS_SHARD bp
                   JOIN
                      D_BID@EAIST_MOS_SHARD b
                   ON bp.BID_ID = b.ID AND b.deleted_date IS NULL
                   JOIN t_lot LV on  LV.ID_ENTITY=B.LOT_ID AND LV.ID_DATA_SOURCE=V_ID_DATA_SOURCE AND LV.VERSION_DATE=V_VERSION_DATE-1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_GKU_PNOTIFICATION_ENTITY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_GKU_PNOTIFICATION_ENTITY';
    rec_array(idx).sql_name := 'T_GKU_PNOTIFICATION_ENTITY [EAIST2]';
    rec_array(idx).description := 'Типы нарушений';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_GKU_PNOTIFICATION_ENTITY (ID,
                                                         VIOLATION_DATE,
                                                         GKU_NOTIFIED_DATE,
                                                         VIOLATION_TYPE,
                                                         PLANNED_DATE,
                                                         ACTUAL_DATE,
                                                         ID_TENDER,
                                                         ID_DATA_SOURCE,
                                                         VERSION_DATE,
                                                         ID_VIOLATION_TYPE)
            SELECT n.ID,
                   n.VIOLATION_DATE,
                   n.GKU_NOTIFIED_DATE,
                   n.VIOLATION_TYPE,
                   n.PLANNED_DATE,
                   n.ACTUAL_DATE,
                   pv.ID procedure_id,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   CASE
                      WHEN VIOLATION_TYPE =
                              'Нарушение срока размещения протокола вскрытия конвертов'
                      THEN
                         1
                      WHEN VIOLATION_TYPE =
                              'Нарушение срока размещения извещения'
                      THEN
                         2
                      ELSE
                         NULL
                   END
                      ID_VIOLATION_TYPE
              FROM    D_GKU_PNOTIFICATION_ENTITY@EAIST_MOS_SHARD n
                   JOIN
                      (SELECT ID, ENTITY_ID, created_date
                         FROM D_PROCEDURE_VERSION@EAIST_MOS_SHARD
                        WHERE deleted_date IS NULL) pv
                   ON n.PROCEDURE_ID = pv.ENTITY_ID
             WHERE (pv.ID IN
                       (SELECT ID
                          FROM REPORTS.T_TENDER
                         WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND VERSION_DATE = V_VERSION_DATE)) and n.id not in (12821053, 12819057);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_PLAN [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_PLAN';
    rec_array(idx).sql_name := 'T_PURCHASE_PLAN [EAIST2]';
    rec_array(idx).description := 'План закупок';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.T_PURCHASE_PLAN (ID,
                                              YEAR,
                                              CUSTOMER_ID,
                                              PERIOD,
                                              publication_date,
                                              approved_date,
                                              ID_DATA_SOURCE,
                                              VERSION_DATE,
                                              entity_id,
                                              IS_ACTUAL,
                                              ID_STATUS,
                                              PERIOD_BEGIN,
                                              LAST_CHANGE_DATE,
                                              VERSION_NUMBER,
                                              FIRST_PUBLISH_DATE,
                                              LAST_CHANGE_PUBLISH_DATE,
                                              LAST_PUBLISH_DATE,
                                              FIRST_APPROVE_DATE,
                                              LAST_CHANGE_APPROVE_DATE,
                                              LAST_APPROVE_DATE)
            SELECT dppv.ID,
                   dppe.YEAR,
                   dppe.CUSTOMER_ID,
                   dppe.PERIOD,
                   dppv.publication_date,
                   approved_date,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   dppe.ID,
                   dppv.ACTUAL,
                   dppv.status_id,
                   TO_NUMBER(SUBSTR(dppe.PERIOD, 1, 4)) as PERIOD_BEGIN,
                   pph.status_date,
                   dppv.version,
                   dates.first_pub_Date,
                   dates.last_change_pub_date,
                   dates.last_pub_date,
                   dates.first_approve_date,
                   dates.last_change_approve_date,
                   dates.last_approve_date
              FROM D_PURCHASE_PLAN_ENTITY@EAIST_MOS_SHARD dppe
                   JOIN (SELECT d.ID,
                                d.ENTITY_ID,
                                d.DELETED_DATE,
                                d.ACTUAL,
                                d.status_id,
                                d.version,
                                CASE
                                   WHEN d.ACTUAL = 1
                                   THEN
                                      d.APPROVED_DATE
                                   WHEN d.ACTUAL = 0
                                        AND d.APPROVED_DATE IS NOT NULL
                                   THEN
                                      (  SELECT MAX (d.APPROVED_DATE)
                                           FROM D_PURCHASE_PLAN_VERSION@EAIST_MOS_SHARD dd
                                          WHERE d.ENTITY_ID = dd.ENTITY_ID
                                       GROUP BY dd.ENTITY_ID)
                                END
                                   APPROVED_DATE,
                                CASE
                                   WHEN d.ACTUAL = 1
                                   THEN
                                      d.PUBLICATION_DATE
                                   WHEN d.ACTUAL = 0
                                        AND d.APPROVED_DATE IS NOT NULL
                                   THEN
                                      d.PUBLICATION_DATE
                                   WHEN     d.ACTUAL = 0
                                        AND d.PUBLICATION_DATE IS NOT NULL
                                        AND d.APPROVED_DATE IS NULL
                                   THEN
                                      (  SELECT MAX (d.PUBLICATION_DATE)
                                           FROM D_PURCHASE_PLAN_VERSION@EAIST_MOS_SHARD dd
                                          WHERE d.ENTITY_ID = dd.ENTITY_ID
                                       GROUP BY dd.ENTITY_ID)
                                END
                                   PUBLICATION_DATE
                           FROM D_PURCHASE_PLAN_VERSION@EAIST_MOS_SHARD d
                          WHERE d.DELETED_DATE IS NULL
                                AND (d.ACTUAL = 1
                                     OR (d.APPROVED_DATE IS NOT NULL
                                         AND d.ACTUAL = 0)
                                     OR (    d.PUBLICATION_DATE IS NOT NULL
                                         AND d.ACTUAL = 0
                                         AND d.APPROVED_DATE IS NULL))) dppv
                      ON dppe.id = dppv.entity_id
                   JOIN (  SELECT YEAR,
                                  CUSTOMER_ID,
                                  PERIOD,
                                  MAX (PLAN_VERSION) AS PLAN_VERSION
                             FROM D_PURCHASE_PLAN_ENTITY@EAIST_MOS_SHARD
                         GROUP BY YEAR, CUSTOMER_ID, PERIOD) max_vers
                      ON     dppe.YEAR = max_vers.YEAR
                         AND dppe.CUSTOMER_ID = max_vers.CUSTOMER_ID
                         AND dppe.PERIOD = max_vers.PERIOD
                         AND dppe.PLAN_VERSION = max_vers.PLAN_VERSION
                    Left join (select entity_id, max(status_date) status_date from D_PURCHASE_PLAN_STATUS_HISTORY@eaist_mos_shard group by entity_id) pph on pph.entity_id=dppe.id 
                    LEFT JOIN (select * from (                
                                select distinct ver.entity_id,
                                min(case when stat.status_id=8 then status_date else null end) over (partition by ver.entity_id) first_pub_date,
                                max(case when stat.status_id=8 and ver.id=ver.max_id then status_date else null end) over (partition by ver.entity_id) last_change_pub_date,
                                max(case when stat.status_id=8 then status_date else null end) over (partition by ver.entity_id) last_pub_date,
                                min(case when stat.status_id=5 then status_date else null end) over (partition by ver.entity_id) first_approve_date,
                                max(case when stat.status_id=5 and ver.id=ver.max_id then status_date else null end) over (partition by ver.entity_id) last_change_approve_date,
                                max(case when stat.status_id=5 then status_date else null end) over (partition by ver.entity_id) last_approve_date
                                FROM (select v.id, v.entity_id, max(id) over (partition by entity_id) max_id from D_PURCHASE_PLAN_VERSION@EAIST_MOS_SHARD v) ver
                                JOIN D_PURCHASE_PLAN_STATUS_HISTORY@EAIST_MOS_SHARD stat
                                ON ver.id=stat.version_id
                                ) where coalesce(first_pub_date, last_change_pub_date,first_approve_date, last_change_approve_date) is not null) dates on dates.entity_id=dppe.id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_SHEDULE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_SHEDULE';
    rec_array(idx).sql_name := 'T_PURCHASE_SHEDULE [EAIST2]';
    rec_array(idx).description := 'План-графики закупок';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO T_PURCHASE_SHEDULE (ID,YEAR,CUSTOMER_ID,PERIOD,APPROVED_DATE,PUBLICATION_DATE,ID_DATA_SOURCE,VERSION_DATE,entity_id,IS_ACTUAL,ID_STATUS, 
                                        LAST_CHANGE_DATE, VERSION_NUMBER, FIRST_PUBLISH_DATE, LAST_CHANGE_PUBLISH_DATE, LAST_PUBLISH_DATE, FIRST_APPROVE_DATE, LAST_CHANGE_APPROVE_DATE, LAST_APPROVE_DATE, IS_ACTUAL_ENTITY, reg_number_oos)

            SELECT distinct psv.ID id,
                   pse.YEAR,
                   pse.CUSTOMER_ID,
                   pse.PERIOD,
                   psv.approved_date,
                   psv.publication_date,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   pse.ID ide,
                   psv.ACTUAL,
                   psv.status_id,
                   psh.status_date,
                   psv.version,
                   dates.first_pub_date,
                   dates.last_change_pub_date,
                   dates.last_pub_date,
                   dates.first_approve_date,
                   dates.last_change_approve_date,
                   dates.last_approve_date,
                   case when max(pse.version) over (partition by pse.customer_id, pse.year) = pse.version then 1 else 0 end IS_ACTUAL_ENTITY,
                   psre.oos_plan_number
              FROM    D_PLAN_SCHEDULE_ENTITY@EAIST_MOS_SHARD pse
              JOIN (select p.id,p.entity_id,p.approved_date,p.publication_date,p.ACTUAL,p.status_id, p.version, row_number() over (partition by p.entity_id order by id desc) rn 
              from D_PLAN_SCHEDULE_VERSION@EAIST_MOS_SHARD p where deleted_date is null)  psv on pse.id=psv.entity_id and psv.rn=1    
              join D_PLAN_SCHEDULE_ROOT_ENTITY@EAIST_MOS_SHARD psre on pse.year=psre.year and pse.customer_id=psre.customer_id
              left join (select entity_id, max(status_date) status_date from D_PLAN_SCHEDULE_STATUS_HISTORY@eaist_mos_shard group by entity_id) psh on psh.entity_id=pse.id
              LEFT JOIN (select * from (
                        select distinct ver.entity_id,
                        min(case when stat.status_id=7 then status_date else null end) over (partition by pse.customer_id, pse.year) first_pub_date,
                        max(case when stat.status_id=7 and ver.id=ver.max_id then status_date else null end) over (partition by ver.entity_id) last_change_pub_date,
                        max(case when stat.status_id=7 then status_date else null end) over (partition by ver.entity_id) last_pub_date,
                        min(case when stat.status_id=4 then status_date else null end) over (partition by pse.customer_id, pse.year) first_approve_date,
                        max(case when stat.status_id=4 and ver.id=ver.max_id then status_date else null end) over (partition by ver.entity_id) last_change_approve_date,
                        max(case when stat.status_id=4 then status_date else null end) over (partition by ver.entity_id) last_approve_date
                        FROM (select v.id, v.entity_id, max(id) over (partition by entity_id) max_id from D_PLAN_SCHEDULE_VERSION@EAIST_MOS_SHARD v) ver
                        JOIN D_PLAN_SCHEDULE_STATUS_HISTORY@EAIST_MOS_SHARD stat
                        ON ver.id=stat.version_id
                        JOIN D_PLAN_SCHEDULE_ENTITY@EAIST_MOS_SHARD pse
                        ON pse.id=ver.entity_id
                        ) where coalesce(first_pub_date, last_change_pub_date, first_approve_date, last_change_approve_Date) is not null) dates on dates.entity_id=pse.id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE';
    rec_array(idx).sql_name := 'T_PURCHASE [EAIST2]';
    rec_array(idx).description := 'Объект закупок';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO T_PURCHASE (ID,
                                 ENTITY_ID,
                                 CUSTOMER_ID,
                                 PURCHASE_AMOUNT,
                                 AMOUNT_PER_PERIODICUTY_UNIT,
                                 DURATION_PURCHASE_DATE,
                                 IS_DURATION_SELECTED,
                                 PERIODICITY_START_DATE,
                                 PURCHASE_SUM,
                                 PUBLIC_DISCUTION,
                                 PUBLIC_DISCUTION_ID,
                                 OKEI_ID,
                                 KPGZ_ID,
                                 SPGZ_ID,
                                 STATUS_INFO,
                                 TARGET,
                                 REASON_FOR_CHANGE_ID,
                                 SOURCE_OF_FINANCING_ID,
                                 LIMIT_ID,
                                 PFHD_ID,
                                 PERIODICITY_ID,
                                 COUNT_REPEAT_PERIODICITY,
                                 OPERATION_ID,
                                 APPROVED_DATE,
                                 TARGET_ID--,DOCX
                                          --,DOCX_SIGNATURE
                                          --,XML_OOS
                                          --,XML_OOS_SIGNATURE
                                 ,
                                 SIGNER_USER_ID,
                                 DOCX_FILE_ACCESS_CODE,
                                 OOS_XML_FILE_ACCESS_CODE,
                                 SENT_TO_CORRECTION,
                                 PGZ_REASON,
                                 REASON,
                                 STATUS_ID,
                                 USER_ID,
                                 STATUS_DATE,
                                 COMMENT_,
                                 REASON_FOR_CHANGE_DESCRIPTION,
                                 CHECKED_IN_GRBS,
                                 PLANNED_PUBLISH_YEAR,
                                 VERSION,
                                 ID_DATA_SOURCE,
                                 VERSION_DATE,
                                 REGISTRY_NUMBER,
                                 FIRST_PUBLISH_DATE,
                                 LAST_CHANGE_PUBLISH_DATE,
                                 LAST_PUBLISH_DATE,
                                 FIRST_APPROVE_DATE,
                                 LAST_CHANGE_APPROVE_DATE,
                                 LAST_APPROVE_DATE)
            SELECT pur_ver.ID,
                   pur_ver.ENTITY_ID,
                   pur_ent.customer_id,
                   pur_ver.PURCHASE_AMOUNT,
                   pur_ver.AMOUNT_PER_PERIODICUTY_UNIT,
                   pur_ver.DURATION_PURCHASE_DATE,
                   pur_ver.IS_DURATION_SELECTED,
                   pur_ver.PERIODICITY_START_DATE,
                   pur_ver.PURCHASE_SUM,
                   pur_ver.PUBLIC_DISCUTION,
                   pur_ver.PUBLIC_DISCUTION_ID,
                   pur_ver.OKEI_ID,
                   pur_ver.KPGZ_ID,
                   pur_ver.SPGZ_ID,
                   pur_ver.STATUS_INFO,
                   pur_ver.TARGET,
                   pur_ver.REASON_FOR_CHANGE_ID,
                   pur_ver.SOURCE_OF_FINANCING_ID,
                   pur_ver.LIMIT_ID,
                   pur_ver.PFHD_ID,
                   per.ID,--pur_ver.PERIODICITY_ID,
                   pur_ver.COUNT_REPEAT_PERIODICITY,
                   pur_ver.OPERATION_ID,
                   p_app_date.CREATED_DATE,
                   pur_ver.TARGET_ID,
                   pur_ver.SIGNER_USER_ID,
                   pur_ver.DOCX_FILE_ACCESS_CODE,
                   pur_ver.OOS_XML_FILE_ACCESS_CODE,
                   pur_ver.SENT_TO_CORRECTION,
                   pur_ver.PGZ_REASON,
                   pur_ver.REASON,
                   pur_ver.STATUS_ID,
                   pur_ver.USER_ID,
                   pur_ver.STATUS_DATE,
                   pur_ver.COMMENT_,
                   pur_ver.REASON_FOR_CHANGE_DESCRIPTION,
                   pur_ver.CHECKED_IN_GRBS,
                   pur_ver.PLANNED_PUBLISH_YEAR,
                   pur_ver.VERSION,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   pur_ent.REGISTRY_NUMBER,
                   dates.first_pub_date,
                   dates.last_change_pub_date,
                   dates.last_pub_date,
                   dates.first_approve_date,
                   dates.last_change_approve_date,
                   dates.last_approve_date
              FROM    D_PURCHASE_ENTITY@EAIST_MOS_SHARD pur_ent
                   JOIN
                      (SELECT ID,
                              ENTITY_ID,
                              PURCHASE_AMOUNT,
                              AMOUNT_PER_PERIODICUTY_UNIT,
                              DURATION_PURCHASE_DATE,
                              IS_DURATION_SELECTED,
                              PERIODICITY_START_DATE,
                              PURCHASE_SUM,
                              PUBLIC_DISCUTION,
                              PUBLIC_DISCUTION_ID,
                              OKEI_ID,
                              KPGZ_ID,
                              SPGZ_ID,
                              STATUS_INFO,
                              TARGET,
                              REASON_FOR_CHANGE_ID,
                              SOURCE_OF_FINANCING_ID,
                              LIMIT_ID,
                              PFHD_ID,
                              PERIODICITY_ID,
                              COUNT_REPEAT_PERIODICITY,
                              OPERATION_ID,
                              APPROVED_DATE,
                              TARGET_ID,
                              SIGNER_USER_ID,
                              DOCX_FILE_ACCESS_CODE,
                              OOS_XML_FILE_ACCESS_CODE,
                              SENT_TO_CORRECTION,
                              PGZ_REASON,
                              REASON,
                              STATUS_ID,
                              USER_ID,
                              STATUS_DATE,
                              COMMENT_,
                              REASON_FOR_CHANGE_DESCRIPTION,
                              CHECKED_IN_GRBS,
                              PLANNED_PUBLISH_YEAR,
                              VERSION
                         FROM D_PURCHASE_VERSION@EAIST_MOS_SHARD
                        WHERE deleted_date IS NULL) pur_ver
                   ON pur_ent.id = pur_ver.entity_id 
                   LEFT JOIN (select ENTITY_ID, CREATED_DATE from
                             (SELECT ID,ENTITY_ID, CREATED_DATE, ROW_NUMBER() OVER (PARTITION  BY ENTITY_ID ORDER BY ID desc) rn 
                             FROM D_PURCHASE_VERSION@EAIST_MOS_SHARD
                             WHERE status_id=2) sign_P  where rn=1) p_app_date
                  ON p_app_date.entity_id=pur_ent.id
                    LEFT JOIN (select distinct entity_id,
                                min(case when status_id=6 then created_date else null end) over (partition by entity_id) first_pub_date,
                                max(case when status_id=6 and max_id=id then created_date else null end) over (partition by entity_id) last_change_pub_date,
                                max(case when status_id=6 then created_date else null end) over (partition by entity_id) last_pub_date,
                                min(case when status_id=2 then created_date else null end) over (partition by entity_id) first_approve_date,
                                max(case when status_id=2 and max_id=id then created_date else null end) over (partition by entity_id) last_change_approve_date,
                                max(case when status_id=2 then created_date else null end) over (partition by entity_id) last_approve_date  
                                FROM (select v.id, v.entity_id, max(id) over (partition by entity_id) max_id, status_id, created_date  from D_PURCHASE_VERSION@EAIST_MOS_SHARD v) ver) dates on dates.entity_id=pur_ent.id
                   LEFT JOIN (SELECT ID, NAME FROM SP_PERIODICITY WHERE ID_DATA_SOURCE=V_ID_DATA_SOURCE AND VERSION_DATE=V_VERSION_DATE) per
                   ON pur_ver.PERIODICITY_ID=per.ID;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE - WAS_PUBLISHED [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE';
    rec_array(idx).sql_name := 'T_PURCHASE - WAS_PUBLISHED [EAIST2]';
    rec_array(idx).description := 'Обновление was_published';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      --Обновление WAS_PUBLISHED
      merge into t_purchase trg using
      (select id, id_Data_source, V_VERSION_DATE version_date,
      max((case when (status_id=6) or (was_published=1) then 1 else 0 end)) was_published
      from t_purchase where version_date<=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE group by id, id_data_source) src
      ON (trg.id=src.id and trg.id_Data_source=src.id_Data_source and trg.version_date=src.version_date)
      when matched then
      update set trg.was_published=src.was_published;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_P_PLAN_PURCHASE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_P_PLAN_PURCHASE';
    rec_array(idx).sql_name := 'LNK_P_PLAN_PURCHASE [EAIST2]';
    rec_array(idx).description := 'Связь ОЗ с планами закупок';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO LNK_P_PLAN_PURCHASE (ID, PURCHASE_ID, PURCHASE_PLAN_ID, ID_DATA_SOURCE, VERSION_DATE, year, GRBS)
         SELECT DISTINCT pppe.ID, pppe.PURCHASE_ID, pppe.PURCHASE_PLAN_ID, V_ID_DATA_SOURCE, V_VERSION_DATE, PP.YEAR, grbs_code
         FROM D_PURCHASE_PLAN_PURCHASE_ENTRY@EAIST_MOS_SHARD pppe
         JOIN T_PURCHASE p ON PPPE.PURCHASE_ID = p.id AND pppe.is_actual = 1 AND p.ID_DATA_SOURCE=V_ID_DATA_SOURCE and p.VERSION_DATE=V_VERSION_DATE
         LEFT JOIN (SELECT lcag.id lnk_id, cust.grbs_code, lcag.version_date
                    FROM LNK_CUSTOMERS_ALL_LEVEL lcag
                    JOIN sp_customer cust ON lcag.id_parent=cust.id||'_'||cust.id_DATA_source AND lcag.version_date=cust.version_date AND cust.connect_level=3
                    UNION
                    SELECT id||'_2', grbs_code, version_date FROM sp_customer WHERE connect_level=3 AND id_Data_source=V_ID_DATA_SOURCE) lcag  
         ON p.CUSTOMER_ID||'_2'=lcag.lnk_id and p.version_date=lcag.version_date 
         JOIN T_PURCHASE_PLAN pp ON PPPE.PURCHASE_PLAN_ID = PP.ID AND pppe.is_actual = 1 and pp.id_Data_source=p.id_Data_source and pp.version_date=p.version_date;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_DETAILED [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_DETAILED';
    rec_array(idx).sql_name := 'T_PURCHASE_DETAILED [EAIST2]';
    rec_array(idx).description := 'Детализированный объект закупок';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO T_PURCHASE_DETAILED (ID,
                                          ID_PURCHASE,
                                          EXTENDED_CODE,
                                          REASON,
                                          REASON_FILE_ID,
                                          CITY_OBJ_ID,
                                          STATUS_INFO,
                                          REASON_FOR_CHANGE_ID,
                                          OPERATION_ID,
                                          APPROVED_DATE--,DOCX
                                                       --,DOCX_SIGNATURE
                                                       --,XML_OOS
                                                       --,XML_OOS_SIGNATURE
                                          ,
                                          SIGNER_USER_ID,
                                          DOCX_FILE_ACCESS_CODE,
                                          OOS_XML_FILE_ACCESS_CODE,
                                          NDS,
                                          BIDDING_ON_UNIT_PRODUCTION,
                                          ADDRESS_LIST_ID,
                                          MUNICIPAL_OBJECT_TYPE_ID,
                                          SENT_TO_CORRECTION,
                                          SALES_COMPLETED,
                                          ADDRESS,
                                          HOUSE_ID,
                                          DESCRIPTION,
                                          AO_ID,
                                          DOWN_PERCENT,
                                          FINAL_NMC,
                                          STATUS_ID,
                                          VERSION,
                                          ID_DATA_SOURCE,
                                          VERSION_DATE,
                                          LOT_ID,
                                          PLAN_SHEDULE_ID,
                                          REGISTRY_NUMBER,
                                          ID_CUSTOMER,
                                          ID_ENTITY,
                                          SMP_QUOTA,
                                          FIRST_PUBLISH_DATE,
                                          LAST_CHANGE_PUBLISH_DATE,
                                          LAST_PUBLISH_DATE,
                                          FIRST_APPROVE_DATE,
                                          LAST_CHANGE_APPROVE_DATE,
                                          LAST_APPROVE_DATE)
            SELECT dpv.ID,
                   pde.PURCHASE_ID,
                   dpv.EXTENDED_CODE,
                   dpv.REASON,
                   dpv.REASON_FILE_ID,
                   dpv.CITY_OBJ_ID,
                   dpv.STATUS_INFO,
                   dpv.REASON_FOR_CHANGE_ID,
                   dpv.OPERATION_ID,
                   dpv.APPROVED_DATE--                    ,dpv.DOCX
                                    --                    ,dpv.DOCX_SIGNATURE
                                    --                    ,dpv.XML_OOS
                                    --                    ,dpv.XML_OOS_SIGNATURE
                   ,
                   dpv.SIGNER_USER_ID,
                   dpv.DOCX_FILE_ACCESS_CODE,
                   dpv.OOS_XML_FILE_ACCESS_CODE,
                   dpv.NDS,
                   dpv.BIDDING_ON_UNIT_PRODUCTION,
                   dpv.ADDRESS_LIST_ID,
                   dpv.MUNICIPAL_OBJECT_TYPE_ID,
                   dpv.SENT_TO_CORRECTION,
                   dpv.SALES_COMPLETED,
                   dpv.ADDRESS,
                   dpv.HOUSE_ID,
                   dpv.DESCRIPTION,
                   dpv.AO_ID,
                   dpv.DOWN_PERCENT,
                   dpv.FINAL_NMC,
                   dpv.STATUS_ID,
                   dpv.VERSION,
                   V_ID_DATA_SOURCE AS idsourse,
                   V_VERSION_DATE versiondate,
                   ldpe.LOT_ID,
                   psdpe.plan_schedule_id,
                   dpe.REGISTRY_NUMBER,
                   dpe.CUSTOMER_ID,
                   DPE.ID AS dpeid,
                   nvl(lpe.smp_QUOTA,0) SMP_QUOTA,
                   dates.first_pub_date,
                   dates.last_change_pub_date,
                   dates.last_pub_date,
                   dates.first_approve_date,
                   dates.last_change_approve_date,
                   dates.last_approve_date
              FROM D_DETAILED_PURCHASE_ENTITY@EAIST_MOS_SHARD dpe
                   JOIN (SELECT ID,
                                ENTITY_ID,
                                EXTENDED_CODE,
                                REASON,
                                REASON_FILE_ID,
                                CITY_OBJ_ID,
                                STATUS_INFO,
                                REASON_FOR_CHANGE_ID,
                                OPERATION_ID,
                                APPROVED_DATE,
                                SIGNER_USER_ID,
                                DOCX_FILE_ACCESS_CODE,
                                OOS_XML_FILE_ACCESS_CODE,
                                NDS,
                                BIDDING_ON_UNIT_PRODUCTION,
                                ADDRESS_LIST_ID,
                                MUNICIPAL_OBJECT_TYPE_ID,
                                SENT_TO_CORRECTION,
                                SALES_COMPLETED,
                                ADDRESS,
                                HOUSE_ID,
                                DESCRIPTION,
                                AO_ID,
                                DOWN_PERCENT,
                                FINAL_NMC,
                                STATUS_ID,
                                VERSION
                           FROM D_DETAILED_PURCHASE_VERSION@EAIST_MOS_SHARD WHERE deleted_date IS NULL) dpv ON dpe.id = dpv.entity_id
                           JOIN D_PURCHASE_DPURCHASE_ENTRY@EAIST_MOS_SHARD pde ON dpv.ID = pde.DETAILED_PURCHASE_ID AND pde.is_actual = 1
                           JOIN (select id from D_PURCHASE_VERSION@EAIST_MOS_SHARD where deleted_date is null) pv ON pde.PURCHASE_ID=pv.ID
                           LEFT JOIN (select distinct DETAILED_PURCHASE_ID, smp_quota 
                                      from (select DETAILED_PURCHASE_ID, smp_quota, lot_id from D_LOT_DPURCHASE_ENTRY@EAIST_MOS_SHARD where is_actual=1) LDPE
                                      join (select id from D_LOT_VERSION@EAIST_MOS_SHARD where deleted_date is null) LV ON LDPE.LOT_ID=LV.ID) lpe
                                      ON lpe.DETAILED_PURCHASE_ID=dpv.ID 
                           LEFT JOIN (select ldp.*, row_number() over (partition by DETAILED_PURCHASE_ID@EAIST_MOS_SHARD order by id desc ) rn from D_LOT_DPURCHASE_ENTRY@EAIST_MOS_SHARD ldp where ldp.is_actual = 1            
                           and exists (SELECT id FROM d_lot_version@EAIST_MOS_SHARD WHERE deleted_date IS NULL AND status_id NOT IN (1)--status на формировании
                                 and  ldp.lot_id = id )) ldpe ON dpv.id = ldpe.DETAILED_PURCHASE_ID and ldpe.rn=1
                      LEFT JOIN (  SELECT MAX(PLAN_SCHEDULE_ID) PLAN_SCHEDULE_ID,
                                       detailed_purchase_id
                                  FROM D_PLAN_SCHEDULE_DPURCH_ENTRY@EAIST_MOS_SHARD 
                                 WHERE is_actual = 1 and plan_schedule_id in (select id from d_plan_schedule_version@eaist_mos_shard where deleted_date is null)
                              GROUP BY detailed_purchase_id) psdpe
                      ON dpv.id = psdpe.DETAILED_PURCHASE_ID
                      LEFT JOIN (select * from (
                                  select distinct ver.entity_id,
                                  min(case when stat.status_id=6 then status_date else null end) over (partition by ver.entity_id) first_pub_date,
                                  max(case when stat.status_id=6 and ver.id=ver.max_id then status_date else null end) over (partition by ver.entity_id) last_change_pub_date,
                                  max(case when stat.status_id=6 then status_date else null end) over (partition by ver.entity_id) last_pub_date,
                                  min(case when stat.status_id=2 then status_date else null end) over (partition by ver.entity_id) first_approve_date,
                                  max(case when stat.status_id=2 and ver.id=ver.max_id then status_date else null end) over (partition by ver.entity_id) last_change_approve_date,
                                  max(case when stat.status_id=2 then status_date else null end) over (partition by ver.entity_id) last_approve_date
                                  FROM (select v.id, v.entity_id, max(id) over (partition by entity_id) max_id from D_DETAILED_PURCHASE_VERSION@EAIST_MOS_SHARD v) ver
                                  JOIN D_DPURCHASE_STATUS_HISTORY@EAIST_MOS_SHARD stat
                                  ON ver.id=stat.version_id
                                  ) where coalesce(first_pub_date, last_change_pub_date, first_approve_date, last_change_approve_date) is not null) dates on dates.entity_id=dpe.id
             WHERE ldpe.LOT_ID IN
                      (SELECT ID
                         FROM REPORTS.T_LOT
                        WHERE version_date = V_VERSION_DATE
                              AND id_data_source = V_ID_DATA_SOURCE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_DETAILED - WAS_PUBLISHED [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_DETAILED';
    rec_array(idx).sql_name := 'T_PURCHASE_DETAILED - WAS_PUBLISHED [EAIST2]';
    rec_array(idx).description := 'Обновление was_published';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      --Обновление WAS_PUBLISHED
      merge into t_purchase_detailed trg using
      (select id, id_Data_source, V_VERSION_DATE version_date,
      max((case when (status_id=6) or (was_published=1) then 1 else 0 end)) was_published
      from t_purchase_detailed where id_data_source=V_ID_DATA_SOURCE and version_date<=V_VERSION_DATE group by id, id_data_source) src
      ON (trg.id=src.id and trg.id_Data_source=src.id_Data_source and trg.version_date=src.version_date)
      when matched then
      update set trg.was_published=src.was_published;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_LIMIT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_LIMIT';
    rec_array(idx).sql_name := 'T_PURCHASE_LIMIT [EAIST2]';
    rec_array(idx).description := 'Лимиты для объектов закупок (ОЗ)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO T_PURCHASE_LIMIT (ID,
                                       PURCHASE_ID,
                                       LIMIT_CODE,
                                       FINANCIAL_SOURCE_TYPE,
                                       PURCHASE_SUM,
                                       LIMIT_YEAR,
                                       IS_GRBS_LIMIT,
                                       CUSTOMER_ID,
                                       FINANCING_SOURCE_ID,
                                       ID_DATA_SOURCE,
                                       VERSION_DATE)
            SELECT ID,
                   PURCHASE_ID,
                   LIMIT_CODE,
                   FINANCIAL_SOURCE_TYPE,
                   PURCHASE_SUM,
                   LIMIT_YEAR,
                   IS_GRBS_LIMIT,
                   CUSTOMER_ID,
        --                   CASE
        --                      WHEN IS_GRBS_LIMIT = 1 THEN 1
        --                      WHEN FINANCIAL_SOURCE_TYPE LIKE 'PFHD' THEN 2
        --                      WHEN FINANCIAL_SOURCE_TYPE LIKE 'LIMIT' THEN 3
        --                   END
        --                      FINANCIAL_SOURCE_ID,
                    CASE
                        WHEN FINANCIAL_SOURCE_TYPE LIKE 'LIMIT' THEN 1
                        WHEN FINANCIAL_SOURCE_TYPE LIKE 'PFHD'  THEN 2
                        WHEN FINANCIAL_SOURCE_TYPE LIKE 'OWN_FUNDS' THEN 3
                        WHEN FINANCIAL_SOURCE_TYPE LIKE 'FEDERAL_FUNDS' THEN 4
                    END FINANCING_SOURCE_ID,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM D_PURCHASE_LIMIT_ENTRY@EAIST_MOS_SHARD;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';
    
    -- T_PURCHASE_LIMIT - PURCHASE_SUM [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_LIMIT';
    rec_array(idx).sql_name := 'T_PURCHASE_LIMIT - PURCHASE_SUM [EAIST2]';
    rec_array(idx).description := 'Обновление purchase_sum';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      --Обновление T_PURCHASE.PURCHASE_SUM 
      merge into T_PURCHASE tp
      using (
        select ID_DATA_SOURCE, VERSION_DATE, PURCHASE_ID, sum(PURCHASE_SUM) PURCHASE_SUM 
        from T_PURCHASE_LIMIT
        where VERSION_DATE=V_VERSION_DATE and ID_DATA_SOURCE=V_ID_DATA_SOURCE
        group by ID_DATA_SOURCE, VERSION_DATE, PURCHASE_ID ) t
      on (tp.VERSION_DATE=V_VERSION_DATE and tp.ID_DATA_SOURCE=V_ID_DATA_SOURCE and tp.ID=t.PURCHASE_ID and tp.VERSION_DATE=t.VERSION_DATE and tp.ID_DATA_SOURCE=t.ID_DATA_SOURCE)
      when matched then update set 
      tp.PURCHASE_SUM=t.PURCHASE_SUM;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_DETAILED_LIMIT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_DETAILED_LIMIT';
    rec_array(idx).sql_name := 'T_PURCHASE_DETAILED_LIMIT [EAIST2]';
    rec_array(idx).description := 'Лимиты для детализированных объектов закупок (ДОЗ) - создание временной таблицы';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         st := 'CREATE TABLE tmp_tpdl as         
                            SELECT dpa.id,
                                   dpa.purchase_sum,
                                   dpv.id DPURCHASE_ID,
                                   ple.limit_code,
                                   dpa.limit_year,
                                   ple.financial_source_type LIMIT_TYPE,
                                   CASE
                                      WHEN ple.financial_source_type LIKE ''LIMIT''
                                      THEN
                                         1
                                      WHEN ple.financial_source_type LIKE ''PFHD''
                                      THEN
                                         2
                                      WHEN ple.financial_source_type LIKE ''OWN_FUNDS''
                                      THEN
                                         3
                                      WHEN ple.financial_source_type LIKE ''FEDERAL_FUNDS''
                                      THEN
                                         4
                                   END
                                      FINANCING_SOURCE_ID,
                                   :V_ID_DATA_SOURCE as ID_DATA_SOURCE,
                                   to_date('':V_VERSION_DATE'', ''DD.MM.YYYY'') as VERSION_DATE
                              FROM d_detailed_purchase_amount@eaist_mos_shard dpa
                                   JOIN d_purchase_dpurchase_entry@eaist_mos_shard pde
                                      ON dpa.dpurchase_id = pde.detailed_purchase_id
                                         AND pde.is_actual = 1
                                   JOIN (select id from D_PURCHASE_VERSION@EAIST_MOS_SHARD where deleted_date is null) pv 
                                      ON pde.PURCHASE_ID=pv.ID      
                                   JOIN d_purchase_limit_entry@eaist_mos_shard ple
                                      ON ple.purchase_id = pde.purchase_id
                                         AND dpa.limit_year = ple.limit_year
                                         and dpa.limit_type = ple.financial_source_type
                                   JOIN (SELECT id, deleted_date
                                           FROM d_detailed_purchase_version@eaist_mos_shard) dpv
                                      ON dpv.id = dpa.dpurchase_id
                                         AND dpv.deleted_date IS NULL';
        st := replace(st, ':V_ID_DATA_SOURCE', V_ID_DATA_SOURCE);
        st := replace(st, ':V_VERSION_DATE', to_char(V_VERSION_DATE, 'DD.MM.YYYY'));
        execute immediate st;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_DETAILED_LIMIT - SUB1 [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_DETAILED_LIMIT';
    rec_array(idx).sql_name := 'T_PURCHASE_DETAILED_LIMIT - SUB1 [EAIST2]';
    rec_array(idx).description := 'Лимиты для детализированных объектов закупок (ДОЗ) - заполнение временной таблицы';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO T_PURCHASE_DETAILED_LIMIT (ID,
                                                PURCHASE_SUM,
                                                DPURCHASE_ID,
                                                LIMIT_CODE,
                                                LIMIT_YEAR,
                                                LIMIT_TYPE,
                                                FINANCING_SOURCE_ID,
                                                ID_DATA_SOURCE,
                                                VERSION_DATE)  
           SELECT * FROM tmp_tpdl;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_DETAILED_LIMIT - SUB2 [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_DETAILED_LIMIT';
    rec_array(idx).sql_name := 'T_PURCHASE_DETAILED_LIMIT - SUB2 [EAIST2]';
    rec_array(idx).description := 'Лимиты для детализированных объектов закупок (ДОЗ) - удаление временной таблицы';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        execute immediate('DROP TABLE tmp_tpdl');

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_DETAILED_SPEC [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_DETAILED_SPEC';
    rec_array(idx).sql_name := 'T_PURCHASE_DETAILED_SPEC [EAIST2]';
    rec_array(idx).description := 'Спецификация детализированных объектов закупок (ДОЗ)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO T_PURCHASE_DETAILED_SPEC (ID,
                                               DPURCHASE_ID,
                                               KPGZ_ID,
                                               SPGZ_ID,
                                               PRICING_METHOD_ID,
                                               REASON_PRICING_METHOD,
                                               REASON_PRICING_METHOD_FILE_ID,
                                               AMOUNT,
                                               OKEI_ID,
                                               AMOUNT_OF_EXPENSES,
                                               PERCENTAGE_ORDINARY_PROFIT,
                                               UNIT_PRICE,
                                               PURCHASE_SUM,
                                               PRICE_ID,
                                               REASON,
                                               REASON_FILE_ID,
                                               DELIVERY_NOTE,
                                               DELIVERY_NOTE_USE_IN_LOT,
                                               NMC_ID,
                                               REASON_SET_IN_LOT,
                                               EAIST1_ID,
                                               EAIST1_KPGZ_NAME,
                                               ID_DATA_SOURCE,
                                               VERSION_DATE,
                                               GRBS)
            SELECT s.ID,
                   DPURCHASE_ID,
                   KPGZ_ID,
                   SPGZ_ID,
                   PRICING_METHOD_ID,
                   REASON_PRICING_METHOD,
                   REASON_PRICING_METHOD_FILE_ID,
                   AMOUNT,
                   OKEI_ID,
                   AMOUNT_OF_EXPENSES,
                   PERCENTAGE_ORDINARY_PROFIT,
                   UNIT_PRICE,
                   PURCHASE_SUM,
                   PRICE_ID,
                   s.REASON,
                   s.REASON_FILE_ID,
                   DELIVERY_NOTE,
                   DELIVERY_NOTE_USE_IN_LOT,
                   NMC_ID,
                   REASON_SET_IN_LOT,
                   EAIST1_ID,
                   EAIST1_KPGZ_NAME,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   grbs_code
              FROM D_DETAILED_PURCHASE_SPEC@EAIST_MOS_SHARD s
              join T_PURCHASE_DETAILED doz on s.dpurchase_id=doz.id and doz.ID_DATA_SOURCE=V_ID_DATA_SOURCE and doz.version_date=V_VERSION_DATE
              left join (select lcag.id lnk_id, cust.grbs_code, lcag.version_date
                         from LNK_CUSTOMERS_ALL_LEVEL lcag
                        join sp_customer cust ON lcag.id_parent=cust.id||'_'||cust.id_DATA_source and lcag.version_date=cust.version_date and cust.connect_level=3
                        union
                        select id||'_2', grbs_code, version_date from sp_customer where connect_level=3 and id_Data_source=V_ID_DATA_SOURCE) lcag  
                        ON doz.ID_CUSTOMER||'_2'=lcag.lnk_id and doz.version_date=lcag.version_date;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_GKU_PURCHASE_SCHEDULE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_GKU_PURCHASE_SCHEDULE';
    rec_array(idx).sql_name := 'T_GKU_PURCHASE_SCHEDULE [EAIST2]';
    rec_array(idx).description := 'Нарушения план-графика закупок';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_GKU_PURCHASE_SCHEDULE (ID,
                                                      REQUEST_DATE,
                                                      PURCHASE_SCHEDULE_NAME,
                                                      PURCHASE_SCHEDULE_YEAR,
                                                      PUBLICATION_PLAN_DATE,
                                                      ADJUST_LIMITS_DATE,
                                                      VIOLATION_TYPE,
                                                      CUSTOMER_ID,
                                                      SENT_DATE,
                                                      PLAN_APPROVAL_DATE,
                                                      VERSION_DATE,
                                                      ID_DATA_SOURCE,
                                                      PURCHASE_SCHEDULE_ID)
            SELECT p.ID,
                   REQUEST_DATE,
                   PURCHASE_SCHEDULE_NAME,
                   PURCHASE_SCHEDULE_YEAR,
                   PUBLICATION_PLAN_DATE,
                   ADJUST_LIMITS_DATE,
                   VIOLATION_TYPE,
                   p.CUSTOMER_ID,
                   p.SENT_DATE,
                   p.PLAN_APPROVAL_DATE,
                   V_VERSION_DATE,
                   V_ID_DATA_SOURCE,
                   p.PURCHASE_SCHEDULE_ID
              FROM    D_GKU_PURCHASE_SCHEDULE_N@EAIST_MOS_SHARD p
                   JOIN
                      (  SELECT MAX (ID) id, PURCHASE_SCHEDULE_ID
                           FROM D_GKU_PURCHASE_SCHEDULE_N@EAIST_MOS_SHARD
                       GROUP BY PURCHASE_SCHEDULE_ID) max_id
                   ON p.ID = max_id.id
                      AND p.PURCHASE_SCHEDULE_ID =
                             max_id.PURCHASE_SCHEDULE_ID;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_GKU_PURCHASE_PLAN [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_GKU_PURCHASE_PLAN';
    rec_array(idx).sql_name := 'T_GKU_PURCHASE_PLAN [EAIST2]';
    rec_array(idx).description := 'Нарушения плана закупок';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_GKU_PURCHASE_PLAN (ID,
                                                  REQUEST_DATE,
                                                  PURCHASE_PLAN_ID,
                                                  PURCHASE_PLAN_NAME,
                                                  APPROVAL_DATE,
                                                  FHD_APPROVAL_DATE,
                                                  PLAN_PERIOD_START_DATE,
                                                  PLAN_PERIOD_END_DATE,
                                                  VIOLATION_TYPE,
                                                  CUSTOMER_ID,
                                                  SENT_DATE,
                                                  PUBLICATION_DATE,
                                                  VIOLATION_PERIOD,
                                                  VERSION_DATE,
                                                  ID_DATA_SOURCE)
            SELECT p.ID,
                   REQUEST_DATE,
                   p.PURCHASE_PLAN_ID,
                   PURCHASE_PLAN_NAME,
                   APPROVAL_DATE,
                   FHD_APPROVAL_DATE,
                   PLAN_PERIOD_START_DATE,
                   PLAN_PERIOD_END_DATE,
                   VIOLATION_TYPE,
                   CUSTOMER_ID,
                   SENT_DATE,
                   PUBLICATION_DATE,
                   VIOLATION_PERIOD,
                   V_VERSION_DATE,
                   V_ID_DATA_SOURCE
              FROM D_GKU_PURCHASE_PLAN_N@EAIST_MOS_SHARD p--join (select max(ID) id,PURCHASE_PLAN_ID from D_GKU_PURCHASE_PLAN_N@EAIST_MOS_SHARD group by PURCHASE_PLAN_ID) max_id on p.ID=max_id.id and p.PURCHASE_PLAN_ID=max_id.PURCHASE_PLAN_ID
         ;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT_SPECIFICATION [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT_SPECIFICATION';
    rec_array(idx).sql_name := 'T_LOT_SPECIFICATION [EAIST2]';
    rec_array(idx).description := 'Сведения о спецификациях лотов (актуальна для ЕАИСТ1, для ЕАИСТ2 собирается по данным из финансирования и спецификации ДОЗ)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO T_LOT_SPECIFICATION (ID,
                                          PD_ID,
                                          PDL_ID,
                                          ID_LOT,
                                          BYDGET_FINANSING,
                                          BUDGET_YEAR, 
                                          ID_OKDP,
                                          ID_OKPD,
                                          ID_GRBS,
                                          ID_FOLDER,
                                          ID_TARGET,
                                          ID_EXPENSE,
                                          ID_ECONOMIC,
                                          ID_FIN_SOURCE,
                                          ID_CUSTOMER,
                                          ID_DATA_SOURCE,
                                          VERSION_DATE,
                                          GRBS_CODE,
                                          FOLDER_CODE,
                                          TARGET_CODE,
                                          EXPENSE_CODE,
                                          ECONOMIC_CODE)
            SELECT ROWNUM AS ID,
                   pd.id,
                   pdl.id,
                   pd.LOT_ID,
                   pdl.PURCHASE_SUM BYDGET_FINANSING,
                   pdl.LIMIT_YEAR BUDGET_YEAR,
                   NULL OKDP_ID,
                   NULL OKPD_ID,
                   sg.id GRBS_ID,
                   sb.id FOLDER_ID,
                   tc.id TARGET_ID,
                   te.id EXPENSE_ID,
                   kos.id ECONOMIC_ID,
                   pdl.FINANCING_SOURCE_ID FIN_SOURCE_ID,
                   PD.ID_CUSTOMER,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   pdl.sg_code GRBS_CODE,
                   pdl.sb_code FOLDER_CODE,
                   pdl.tc_code TARGET_CODE,
                   pdl.te_code EXPENSE_CODE,
                   pdl.kos_code ECONOMIC_CODE
              FROM (select pdl.*,
                            case 
                              when LENGTH(pdl.LIMIT_CODE)>=20 then SUBSTR(pdl.LIMIT_CODE, 1, 3)
                            end sg_code,
                            case
                              when LENGTH(pdl.LIMIT_CODE)>=20 then SUBSTR(pdl.LIMIT_CODE, 4, 4) 
                            end sb_code,
                            case
                              when  LENGTH(pdl.LIMIT_CODE)=20 then SUBSTR(pdl.LIMIT_CODE, 8, 7)
                              when  LENGTH(pdl.LIMIT_CODE)=23 then SUBSTR(pdl.LIMIT_CODE, 8, 10)
                            end tc_code,
                            case
                              when  LENGTH(pdl.LIMIT_CODE)=20 then SUBSTR(pdl.LIMIT_CODE, 15, 3)
                              when  LENGTH(pdl.LIMIT_CODE)=23 then SUBSTR(pdl.LIMIT_CODE, 18, 3)
                            end te_code,
                            case
                              when  LENGTH(pdl.LIMIT_CODE)<20 then pdl.LIMIT_CODE
                              when  LENGTH(pdl.LIMIT_CODE)=20 then SUBSTR(pdl.LIMIT_CODE, 18, 3)
                              when  LENGTH(pdl.LIMIT_CODE)=23 then SUBSTR(pdl.LIMIT_CODE, 21, 3)
                            end kos_code
                     from t_purchase_detailed_limit pdl) pdl
                   JOIN t_purchase_detailed pd
                      ON     PDL.DPURCHASE_ID = PD.ID
                         AND pd.id_data_source = V_ID_DATA_SOURCE
                         AND pdl.id_data_source = V_ID_DATA_SOURCE
                         AND pd.version_date = V_VERSION_DATE
                         AND pdl.version_date = V_VERSION_DATE
                   LEFT JOIN sp_grbs sg
                      ON     sg.code = pdl.sg_code
                         AND sg.id_data_source = V_ID_DATA_SOURCE
                         AND sg.version_date = V_VERSION_DATE
                         AND pdl.id_data_source = V_ID_DATA_SOURCE
                         AND pdl.version_date = V_VERSION_DATE
                   LEFT JOIN sp_section_budget sb
                      ON     SB.CODE = pdl.sb_code
                         AND sb.id_data_source = V_ID_DATA_SOURCE
                         AND sb.version_date = V_VERSION_DATE
                         AND pdl.id_data_source = V_ID_DATA_SOURCE
                         AND pdl.version_date = V_VERSION_DATE
                   LEFT JOIN sp_target_clause tc
                      ON     tc.code = pdl.tc_code
                         AND tc.id_data_source = V_ID_DATA_SOURCE
                         AND tc.version_date = V_VERSION_DATE
                         AND pdl.id_data_source = V_ID_DATA_SOURCE
                         AND pdl.version_date = V_VERSION_DATE
                   LEFT JOIN sp_type_expense te
                      ON     te.code = pdl.te_code
                         AND te.id_data_source = V_ID_DATA_SOURCE
                         AND te.version_date = V_VERSION_DATE
                         AND pdl.id_data_source = V_ID_DATA_SOURCE
                         AND pdl.version_date = V_VERSION_DATE
                   LEFT JOIN sp_kosgu kos
                      ON     kos.code = pdl.kos_code
                         AND kos.id_data_source = V_ID_DATA_SOURCE
                         AND kos.version_date = V_VERSION_DATE
                         AND pdl.id_data_source = V_ID_DATA_SOURCE
                         AND pdl.version_date = V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_AGR_CHANGE_TYPE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_AGR_CHANGE_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_AGR_CHANGE_TYPE [EAIST2]';
    rec_array(idx).description := 'Обоснование изменения цены контракта';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_CONTRACT_AGR_CHANGE_TYPE (ID,
                                                          NAME,
                                                          ID_DATA_SOURCE,
                                                          VERSION_DATE)
            SELECT ID,
                   NAME,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM CONTRACT_AGREEMENT_CHANGE_TYPE@EAIST_MOS_RC;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SINGLE_VENDOR_PURCHASE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SINGLE_VENDOR_PURCHASE';
    rec_array(idx).sql_name := 'SP_SINGLE_VENDOR_PURCHASE [EAIST2]';
    rec_array(idx).description := 'Основание закупки у единственного поставщика';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_SINGLE_VENDOR_PURCHASE (ID,
                                                        CODE,
                                                        NAME,
                                                        DESCRIPTION,
                                                        ID_DATA_SOURCE,
                                                        VERSION_DATE)
            SELECT ID,
                   CODE,
                   NAME,
                   DESCRIPTION,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM SINGLE_VENDOR_PURCHASE@EAIST_MOS_RC;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CLAIM_TYPE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CLAIM_TYPE';
    rec_array(idx).sql_name := 'SP_CLAIM_TYPE [EAIST2]';
    rec_array(idx).description := 'Типы штрафов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_CLAIM_TYPE (ID,
                                            NAME,
                                            DESCRIPTION,
                                            ID_DATA_SOURCE,
                                            VERSION_DATE)
            SELECT ID,
                   NAME,
                   DESCRIPTION,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM CONTRACT_CLAIM_COLLECT_TYPE@EAIST_MOS_RC;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_TERM_REASON_TYPE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_TERM_REASON_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_TERM_REASON_TYPE [EAIST2]';
    rec_array(idx).description := 'Причины расторжения контракта';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_CONTRACT_TERM_REASON_TYPE (ID,
                                                           NAME,
                                                           CODE,
                                                           DESCRIPTION,
                                                           ID_DATA_SOURCE,
                                                           VERSION_DATE)
            SELECT ID,
                   NAME,
                   CODE,
                   DESCRIPTION,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM CONTRACT_TERM_REASON_TYPE@EAIST_MOS_RC;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';
    
    -- SP_CONTRACT_TERMINATION_TYPE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_TERMINATION_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_TERMINATION_TYPE [EAIST2]';
    rec_array(idx).description := 'Типы причин расторжения';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_CONTRACT_TERMINATION_TYPE (ID,
                                                           DESCRIPTION,
                                                           NAME,
                                                           ID_DATA_SOURCE,
                                                           VERSION_DATE)
            SELECT ID,
                   DESCRIPTION,
                   NAME,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM CONTRACT_TERMINATION_TYPE@EAIST_MOS_RC;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_DOCUMENT_CATEGORY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_DOCUMENT_CATEGORY';
    rec_array(idx).sql_name := 'SP_CONTRACT_DOCUMENT_CATEGORY [EAIST2]';
    rec_array(idx).description := 'Cправочник категорий документов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_CONTRACT_DOCUMENT_CATEGORY (ID,
                                                            CODE,
                                                            DESCRIPTION,
                                                            NAME,
                                                            GROUP_CODE,
                                                            ID_DATA_SOURCE,
                                                            VERSION_DATE)
            SELECT ID,
                   CODE,
                   DESCRIPTION,
                   NAME,
                   GROUP_CODE,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM CONTRACT_DOCUMENT_CATEGORY@EAIST_MOS_RC;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_SUB_CONTRACT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_SUB_CONTRACT';
    rec_array(idx).sql_name := 'T_SUB_CONTRACT [EAIST2]';
    rec_array(idx).description := 'Субподрядчики по контрактам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_SUB_CONTRACT(
                                                      ID,
                                                      CONTRACT_ID,
                                                      ORGANIZATION_ID,
                                                      PRICE,
                                                      ID_DATA_SOURCE,
                                                      VERSION_DATE )
            select ID,CONTRACT_ID, ENTITY_ID ,COST_IN_RUBLE,V_ID_DATA_SOURCE,V_VERSION_DATE from contract_subcontractor@eaist_mos_rc where deleted_date is null and is_actual=1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT';
    rec_array(idx).sql_name := 'T_CONTRACT [EAIST2]';
    rec_array(idx).description := 'Контракт';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.T_CONTRACT (ID,
                                         NAME,
                                         ID_CUSTOMER,
                                         ID_STATUS,
                                         ID_SUPPLIER,
                                         CONTRACT_NUMBER,
                                         CONTRACT_DATE,
                                         START_DATE,
                                         END_DATE,
                                         COST_IN_RUBLE,
                                         DURATION_END_DATE,
                                         DURATION_START_DATE,
                                         IS_VIOLATION_EXEC,
                                         ID_DATA_SOURCE,
                                         VERSION_DATE,
                                         ID_SINGLE_VENDOR_PURCHASE,
                                         REG_DATE,
                                         REGISTRY_NUMBER,
                                         CURRENCY_ID,
                                         IS_COAUTHOR_MISSING,
                                         REGISTRY_NUMBER_OOS,
                                         CONTRACTTYPE_ID,
                                         NEED_CHANGE_APPROVE,
                                         NEED_CONCLUSION_APPROVE,
                                         EDITION_NUMBER,
                                         CONTRACTIMPORT_ID,
                                         TENDER_ID,
                                         NEWSTATE_ID,
                                         CONTRACT_DRAFT_DATE,
                                         EVASIONSUPPLIER_ID,
                                         IS_ELECTRONIC_CONCLUSION,
                                         ENTITY_ID,
                                         METHOD_OF_SUPPLIER_ID,
                                         CONTRACT_TERM_DATE,
                                         SMP_QUOTA,
                                         IS_HAVENT_CLAIM,
                                         COST,
                                         ID_TERMINATION_TYPE,
                                         ID_REASONTYPE,
                                         EP_SUMM,
                                         PAYMENT_SUM,
                                         FACT_START_DATE,
                                         FACT_END_DATE)
             
                                                
            SELECT distinct con.ID,
                   con.Name,
                   con.CUSTOMER_ID AS ID_CUSTOMER,
                   con.STATE_ID AS ID_STATUS,
                   con.SUPPLIER_ID AS ID_SUPPLIER,
                   con.CONTRACT_NUMBER,
                   con.CONCLUSION_DATE AS CONTRACT_DATE,
                   con.EXECUTION_START_DATE AS START_DATE, /*Дата начала исполнения (фактическая)*/
                   con.EXECUTION_END_DATE AS END_DATE, /*Дата окончания исполнения(фактическая)*/
                   con.COST_IN_RUBLE,
                   con.DURATION_END_DATE, /*плановая дата окончания действия контракта, для вычисления контрактов с истекшим сроком действия*/
                   con.DURATION_START_DATE, /*плановая дата начала действия контракта */
                   0 AS IS_VIOLATION_EXEC,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   con.SINGLEVENDORPURCHASE_ID,
                   con.REGISTRY_DATE,
                   con.REGISTRY_NUMBER,
                   con.CURRENCY_ID,
                   con.IS_COAUTHOR_MISSING,
                   con.REGISTRY_NUMBER_OOS,
                   con.CONTRACTTYPE_ID,
                   con.NEED_CHANGE_APPROVE,
                   con.NEED_CONCLUSION_APPROVE,
                   con.EDITION_NUMBER,
                   con.CONTRACTIMPORT_ID,
                   l.TENDER_ID,--con.TENDER_ID,
                   con.NEWSTATE_ID,
                   con.CONTRACT_DRAFT_DATE,
                   con.EVASIONSUPPLIER_ID,
                   con.IS_ELECTRONIC_CONCLUSION,
                   con.ENTITY_ID,
                   lv.method_of_supplier_id,
                   cd.term_date,
                   (pdc.smp_sum/pdc.lot_sum)*100 SMP_QUOTA,
                   CASE
                     WHEN (con.STATE_ID=10 and cc.contract_id is null) THEN 1
                     ELSE 0
                   END IS_HAVENT_CLAIM,
                   con.COST,
                   tts.type_id,
                   tts.reasontype_id,
                   epSumm.EP_SUMM,
                   paymentSumm.PAYMENT_SUMM,
                   cd.exec_date,
                   case when con.state_id in (7, 9) then exec_date.document_date else null end exec_end_Date--cd.end_exec_date
              --FROM CONTRACT@EAIST_MOS_RC con
              FROM eaist_rc.CONTRACT@eaist_mos_shard con
              --join contract_lot@eaist_mos_rc cl on con.lot_id = cl.id
              join eaist_rc.contract_lot@eaist_mos_shard cl on con.lot_id = cl.id
              
              left join (select * from t_lot where version_date=trunc(sysdate) and id_data_source=2) l on l.id_entity=cl.ext_id

              LEFT JOIN (select lpe.lot_id, sum(pdc.purchase_sum) lot_sum, sum(pdc.purchase_sum*nvl(lpe.smp_quota,0)/100) smp_sum from d_lot_dpurchase_entry@eaist_mos_shard lpe
              join D_DETAILED_PURCHASE_SPEC@EAIST_MOS_SHARD pdc on lpe.detailed_purchase_id=pdc.dPurchase_id 
              group by lpe.lot_id) pdc on pdc.lot_id=cl.ext_id
              --LEFT JOIN (select sum(advance_cost) EP_SUMM, contract_id from contract_stage_financing@eaist_mos_rc where is_actual=1 and deleted_date is null group by contract_id) epSumm
              LEFT JOIN (select sum(advance_cost) EP_SUMM, contract_id from eaist_rc.contract_stage_financing@eaist_mos_shard where is_actual=1 and deleted_date is null group by contract_id) epSumm
              ON epSumm.contract_id=con.id
              --LEFT JOIN (select sum(cost_in_ruble) payment_summ, contract_id from contract_payment@eaist_mos_rc where is_actual=1 and deleted_date is null group by contract_id) paymentSumm 
              LEFT JOIN (select sum(cost_in_ruble) payment_summ, contract_id from eaist_rc.contract_payment@eaist_mos_shard where is_actual=1 and deleted_date is null group by contract_id) paymentSumm 
              ON paymentSumm.contract_id=con.id
              --LEFT JOIN (select entity_id, min(created_date) term_date from contract@eaist_mos_rc where state_id=10 group by entity_id) ct on con.entity_id=ct.entity_id
              --LEFT JOIN (select entity_id, min(created_date) exec_date from contract@eaist_mos_rc where state_id=7 group by entity_id) ce on con.entity_id=ct.entity_id
              --LEFT JOIN (select entity_id, min(created_date) end_exec_date from contract@eaist_mos_rc where state_id=9 group by entity_id) cee on con.entity_id=ct.entity_id
              LEFT JOIN   (SELECT distinct 
                          entity_id,
                          min(case when state_id=10 then created_date else null end) term_date,
                          min(case when state_id=7 then created_date else null end) exec_date,
                          min(case when state_id=9 then created_date else null end) end_exec_date
                          --from contract@eaist_mos_rc group by entity_id) cd on con.entity_id=cd.entity_id
                          from eaist_rc.contract@eaist_mos_shard group by entity_id) cd on con.entity_id=cd.entity_id
                          
              LEFT JOIN
              (select distinct contract_id, document_date 
              from (
                 select cs.*, 
                        avg(cs.state_id) over(partition by cs.contract_id) avg_state,
                        max(cs.created_date) over(partition by cs.contract_id) max_created_date,
                        document_date,
                        max(document_date) over (partition by cs.contract_id) max_document_date
                 --from contract_stage@eaist_mos_rc cs
                 from eaist_rc.contract_stage@eaist_mos_shard cs
                 --join CONTRACT_STAGE_EXAMINATION@eaist_mos_rc cse on cs.id=cse.stage_id 
                 join eaist_rc.CONTRACT_STAGE_EXAMINATION@eaist_mos_shard cse on cs.id=cse.stage_id 
                 where cs.deleted_date is null and cse.is_actual = 1 and cse.is_violation_acceptance = 0) 
                 where avg_state=403 and created_date=max_created_date and max_document_date=document_date) exec_date on exec_date.contract_id=con.id          
              
              LEFT JOIN (SELECT distinct type_id, reasontype_id, contract_id 
              --FROM contract_stage@eaist_mos_rc ts
              FROM eaist_rc.contract_stage@eaist_mos_shard ts
              JOIN (select stage_id, type_id, reasontype_id, created_date, max(created_date) over (partition by stage_id) max_created_date 
                        --from contract_stage_termination@eaist_mos_rc where deleted_date is null and is_actual=1) tts
                        from eaist_rc.contract_stage_termination@eaist_mos_shard where deleted_date is null and is_actual=1) tts
              on ts.id=tts.stage_id and tts.created_date=tts.max_created_date and type_id is not null) tts
              on  tts.contract_id=con.id
              
              
              
              --LEFT JOIN (select distinct contract_id from CONTRACT_CLAIM@EAIST_MOS_RC WHERE DELETED_DATE IS NULL) CC ON CC.CONTRACT_ID=CON.ID
              LEFT JOIN (select distinct contract_id from eaist_rc.CONTRACT_CLAIM@EAIST_MOS_shard WHERE DELETED_DATE IS NULL) CC ON CC.CONTRACT_ID=CON.ID
              left join (select id,entity_id,deleted_date, method_of_supplier_id from d_lot_version@eaist_mos_shard) lv on lv.entity_id = cl.ext_id and lv.deleted_date is null
             WHERE con.ID = con.ENTITY_ID and con.deleted_date is null and con.is_actual=1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_SPEC [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_SPEC';
    rec_array(idx).sql_name := 'T_CONTRACT_SPEC [EAIST2]';
    rec_array(idx).description := 'Спецификация по контрактам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_CONTRACT_SPEC
          (ID, CONTRACT_ID, QUANTITY, UNIT_COST, COST_IN_RUBLE, VERSION_DATE, ID_DATA_SOURCE, OKPD_ID, KPGZ_ID, OKEI_ID)
        select        
          sp.ID as ID
          ,sp.CONTRACT_ID as CONTRACT_ID
          ,sp.QUANTITY as COUNT_PRODUCTION
          ,sp.UNIT_COST as UNIT_PRICE
          ,sp.COST_IN_RUBLE as SUMM
          ,V_VERSION_DATE as version_date
          ,V_ID_DATA_SOURCE as ID_DATA_SOURCE
          ,okpd_id
          ,kpgz_id
          ,okei_id
        from contract_specification@eaist_mos_rc sp;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_FINANSING_CONTRACTS [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_FINANSING_CONTRACTS';
    rec_array(idx).sql_name := 'T_FINANSING_CONTRACTS [EAIST2]';
    rec_array(idx).description := 'Финансирование (Строки ГК - КБК)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_FINANSING_CONTRACTS (ID,
                                                    ID_CONTRACT,
                                                    ID_FINANCING_SOURCE,
                                                    ID_PURPOSE,
                                                    ID_GRBS,
                                                    ID_EXPENSE,
                                                    ID_FOLDER,
                                                    ID_ECONOMIC,
                                                    SUMMA_IN_CURRENCY_NDS,
                                                    BUDGET_YEAR,
                                                    ECONOMIC_SUMM,
                                                    ID_DATA_SOURCE,
                                                    VERSION_DATE,
                                                    PURPOSE_CODE,
                                                    GRBS_CODE,
                                                    EXPENSE_CODE,
                                                    FOLDER_CODE,
                                                    ECONOMIC_CODE)
         SELECT     CF.ID,
                     CF.CONTRACT_ID,
                     SF.ID,--CF.FINANCINGSOURCE_ID,
                     cl.ID AS ID_PURPOSE,                   --CF.KBKCLAUSE_ID,
                     GRBS.id ID_GRBS,                         --CF.KBKGRBS_ID,
                     EXP.ID ID_FUNCTIONAL,             --CF.KBKEXPENSETYPE_ID,
                     sec.ID ID_EXPENSE,                   --CF.KBKDEVISION_ID,
                     kos.ID ID_ECONOMIC,                      --CF.KBKKOSGU_ID
                     SUM (CF.COST_IN_RUBLE) COST_IN_RUBLE,
                     CF.BUDGETYEAR_ID,
                     CON_ECON.ECONOMIC_SUMM,
                     V_ID_DATA_SOURCE,
                     V_VERSION_DATE,
                     CF.KBK_CLAUSE PURPOSE_CODE,
                     CF.KBK_GRBS GRBS_CODE,
                     CF.KBK_EXPENSE_TYPE EXPENSE_CODE,
                     CF.KBK_DEVISION FOLDER_CODE,
                     CF.KBK_KOSGU ECONOMIC_CODE
                FROM (select cf.* from(SELECT * FROM CONTRACT_FINANCING@EAIST_MOS_RC WHERE DELETED_DATE IS NULL and is_actual=1) cf
                                      inner join 
                                      (select entity_id from contract@EAIST_MOS_RC where deleted_date is null and is_actual=1) tc 
                                      ON cf.contract_id = tc.entity_id ) CF
                     /*left join EAIST_RC.KBK_CLAUSE cl on CF.KBK_CLAUSE=cl.CODE
                     left join EAIST_RC.KBK_GRBS GRBS on CF.KBK_GRBS=GRBS.CODE
                     left join EAIST_RC.KBK_EXPENSE_TYPE exp on CF.KBK_EXPENSE_TYPE=exp.CODE
                     left join EAIST_RC.KBK_DEVISION sec on CF.KBK_DEVISION =sec.CODE
                     left join EAIST_RC.KBK_KOSGU kos on CF.KBK_KOSGU=kos.CODE*/
                    LEFT JOIN (select * from SP_SOURCE_FINANCE where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE) sf ON cf.FINANCING_SOURCE_TYPE=sf.code
                     /*берем из своих источников, пока в EAIST_RC не появятся нормальные данные*/
                     LEFT JOIN REPORTS.SP_TARGET_CLAUSE cl
                        ON CF.KBK_CLAUSE = cl.CODE and cl.ID_DATA_SOURCE = V_ID_DATA_SOURCE  AND cl.VERSION_DATE = V_VERSION_DATE
                     LEFT JOIN REPORTS.SP_GRBS GRBS
                        ON CF.KBK_GRBS = GRBS.CODE AND GRBS.ID_DATA_SOURCE = V_ID_DATA_SOURCE AND GRBS.VERSION_DATE = V_VERSION_DATE
                     LEFT JOIN REPORTS.SP_TYPE_EXPENSE EXP
                        ON CF.KBK_EXPENSE_TYPE = EXP.CODE AND EXP.ID_DATA_SOURCE = V_ID_DATA_SOURCE AND EXP.VERSION_DATE = V_VERSION_DATE
                     LEFT JOIN REPORTS.SP_SECTION_BUDGET sec
                        ON CF.KBK_DEVISION = sec.CODE AND sec.ID_DATA_SOURCE = V_ID_DATA_SOURCE AND sec.VERSION_DATE = V_VERSION_DATE
                     LEFT JOIN REPORTS.SP_KOSGU kos 
                        ON CF.KBK_KOSGU = kos.CODE AND kos.ID_DATA_SOURCE = V_ID_DATA_SOURCE AND kos.VERSION_DATE = V_VERSION_DATE
                     LEFT JOIN /*агрегируем экономию*/
                               (  SELECT CONTRACT_ID,
                                         KBK_CLAUSE,
                                         KBK_DEVISION,
                                         KBK_EXPENSE_TYPE,
                                         KBK_GRBS,
                                         KBK_KOSGU,
                                         BUDGETYEAR_ID,
                                         SUM (COST_IN_RUBLE) ECONOMIC_SUMM
                                    FROM CONTRACT_ECONOMY@EAIST_MOS_RC
                                   WHERE ID = ENTITY_ID AND DELETED_DATE IS NULL
                                GROUP BY CONTRACT_ID,
                                         KBK_CLAUSE,
                                         KBK_DEVISION,
                                         KBK_EXPENSE_TYPE,
                                         KBK_GRBS,
                                         KBK_KOSGU,
                                         BUDGETYEAR_ID) CON_ECON
                        ON     CF.CONTRACT_ID = CON_ECON.CONTRACT_ID
                           AND cl.CODE = CON_ECON.KBK_CLAUSE
                           AND sec.CODE = CON_ECON.KBK_DEVISION
                           AND EXP.CODE = CON_ECON.KBK_EXPENSE_TYPE
                           AND GRBS.CODE = CON_ECON.KBK_GRBS
                           AND kos.CODE = CON_ECON.KBK_KOSGU
                           AND CF.BUDGETYEAR_ID = CON_ECON.BUDGETYEAR_ID      
            GROUP BY CF.ID,
                     CF.CONTRACT_ID,
                     SF.ID,--CF.FINANCINGSOURCE_ID,
                     cl.ID,
                     GRBS.id,
                     EXP.ID,
                     sec.ID,
                     kos.ID,
                     CF.BUDGETYEAR_ID,
                     CON_ECON.ECONOMIC_SUMM,
                     CF.KBK_CLAUSE,
                     CF.KBK_GRBS,
                     CF.KBK_EXPENSE_TYPE,
                     CF.KBK_DEVISION,
                     CF.KBK_KOSGU;
              /*SELECT MAX (CF.ID) AS ID,
                     CF.CONTRACT_ID,
                     CF.FINANCINGSOURCE_ID,
                     cl.ID AS ID_PURPOSE,                   --CF.KBKCLAUSE_ID,
                     GRBS.id ID_GRBS,                         --CF.KBKGRBS_ID,
                     EXP.ID ID_FUNCTIONAL,             --CF.KBKEXPENSETYPE_ID,
                     sec.ID ID_EXPENSE,                   --CF.KBKDEVISION_ID,
                     kos.ID ID_ECONOMIC,                      --CF.KBKKOSGU_ID
                     SUM (CF.COST_IN_RUBLE) COST_IN_RUBLE,
                     CF.BUDGETYEAR_ID,
                     CON_ECON.ECONOMIC_SUMM,
                     V_ID_DATA_SOURCE,
                     V_VERSION_DATE
                FROM CONTRACT_FINANCING@EAIST_MOS_RC CF
                     --left join EAIST_RC.KBK_CLAUSE cl on CF.KBK_CLAUSE=cl.CODE
                     --left join EAIST_RC.KBK_GRBS GRBS on CF.KBK_GRBS=GRBS.CODE
                     --left join EAIST_RC.KBK_EXPENSE_TYPE exp on CF.KBK_EXPENSE_TYPE=exp.CODE
                     --left join EAIST_RC.KBK_DEVISION sec on CF.KBK_DEVISION =sec.CODE
                     --left join EAIST_RC.KBK_KOSGU kos on CF.KBK_KOSGU=kos.CODE

                     --берем из своих источников, пока в EAIST_RC не появятся нормальные данные
                     LEFT JOIN REPORTS.SP_TARGET_CLAUSE cl
                        ON CF.KBK_CLAUSE = cl.CODE
                     LEFT JOIN REPORTS.SP_GRBS GRBS
                        ON CF.KBK_GRBS = GRBS.CODE
                     LEFT JOIN REPORTS.SP_TYPE_EXPENSE EXP
                        ON CF.KBK_EXPENSE_TYPE = EXP.CODE
                     LEFT JOIN REPORTS.SP_SECTION_BUDGET sec
                        ON CF.KBK_DEVISION = sec.CODE
                     LEFT JOIN REPORTS.SP_KOSGU kos
                        ON CF.KBK_KOSGU = kos.CODE
                     LEFT JOIN --агрегируем экономию
                               (  SELECT CONTRACT_ID,
                                         KBK_CLAUSE,
                                         KBK_DEVISION,
                                         KBK_EXPENSE_TYPE,
                                         KBK_GRBS,
                                         KBK_KOSGU,
                                         BUDGETYEAR_ID,
                                         SUM (COST_IN_RUBLE) ECONOMIC_SUMM
                                    FROM CONTRACT_ECONOMY@EAIST_MOS_RC
                                   WHERE ID = ENTITY_ID AND DELETED_DATE IS NULL
                                GROUP BY CONTRACT_ID,
                                         KBK_CLAUSE,
                                         KBK_DEVISION,
                                         KBK_EXPENSE_TYPE,
                                         KBK_GRBS,
                                         KBK_KOSGU,
                                         BUDGETYEAR_ID) CON_ECON
                        ON     CF.CONTRACT_ID = CON_ECON.CONTRACT_ID
                           AND cl.CODE = CON_ECON.KBK_CLAUSE
                           AND sec.CODE = CON_ECON.KBK_DEVISION
                           AND EXP.CODE = CON_ECON.KBK_EXPENSE_TYPE
                           AND GRBS.CODE = CON_ECON.KBK_GRBS
                           AND kos.CODE = CON_ECON.KBK_KOSGU
                           AND CF.BUDGETYEAR_ID = CON_ECON.BUDGETYEAR_ID
               WHERE     cl.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                     AND GRBS.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                     AND EXP.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                     AND sec.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                     AND kos.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                     AND cl.VERSION_DATE = V_VERSION_DATE
                     AND GRBS.VERSION_DATE = V_VERSION_DATE
                     AND EXP.VERSION_DATE = V_VERSION_DATE
                     AND sec.VERSION_DATE = V_VERSION_DATE
                     AND kos.VERSION_DATE = V_VERSION_DATE
            GROUP BY CF.CONTRACT_ID,
                     CF.FINANCINGSOURCE_ID,
                     cl.ID,
                     GRBS.id,
                     EXP.ID,
                     sec.ID,
                     kos.ID,
                     CF.BUDGETYEAR_ID,
                     CON_ECON.ECONOMIC_SUMM;
                     --Временно, пока не проверен измененный*/

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_STAGE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_STAGE';
    rec_array(idx).sql_name := 'T_CONTRACT_STAGE [EAIST2]';
    rec_array(idx).description := 'Этапы';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_CONTRACT_STAGE (ID,
                                               ACTUAL_DATE,
                                               STAGE_NUMBER,
                                               ID_CONTRACT,
                                               COST_IN_RUBLE,
                                               IS_ACTUAL,
                                               PLAN_DATE,
                                               ID_STATE,
                                               DESCRIPTION,
                                               NAME,
                                               ID_DATA_SOURCE,
                                               VERSION_DATE,
                                               FACT_END_DATE)
            SELECT cs.ID,
                   cs.ACTUAL_DATE,
                   cs.STAGE_NUMBER,
                   cs.CONTRACT_ID,
                   cs.COST_IN_RUBLE,
                   cs.IS_ACTUAL,
                   cs.PLAN_DATE,
                   cs.STATE_ID,
                   cs.DESCRIPTION,
                   cs.NAME,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   document_date
              FROM CONTRACT_STAGE@EAIST_MOS_RC cs
              join (select distinct
                      stage_id,
                      document_date,
                      max(document_date) over (partition by stage_id) max_document_date
                      from CONTRACT_STAGE_EXAMINATION@eaist_mos_rc where is_actual = 1 and is_violation_acceptance = 0) cse on cse.stage_id=cs.id and max_document_date=document_date
             WHERE DELETED_DATE IS NULL; 

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_STAGE_TERMINATION [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_STAGE_TERMINATION';
    rec_array(idx).sql_name := 'T_CONTRACT_STAGE_TERMINATION [EAIST2]';
    rec_array(idx).description := 'Расторжение этапов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_CONTRACT_STAGE_TERMINATION (
                        ID,
                        DOCUMENT_DATE,
                        DOCUMENT_NUMBER,
                        IS_ACTUAL,
                        IS_NOT_CONFIRMED,
                        IS_VIOLATION_FIXED,
                        NOTIFICATION_DATE,
                        REASON,
                        REGISTRY_DATE,
                        TERMINATION_DATE,
                        ID_REASONTYPE,
                        ID_STAGE,
                        ID_STATE,
                        ID_TYPE,
                        ID_DATA_SOURCE,
                        VERSION_DATE)
            SELECT ID,
                   DOCUMENT_DATE,
                   DOCUMENT_NUMBER,
                   IS_ACTUAL,
                   IS_NOT_CONFIRMED,
                   IS_VIOLATION_FIXED,
                   NOTIFICATION_DATE,
                   REASON,
                   REGISTRY_DATE,
                   TERMINATION_DATE,
                   REASONTYPE_ID,
                   STAGE_ID,
                   STATE_ID,
                   TYPE_ID,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM CONTRACT_STAGE_TERMINATION@EAIST_MOS_RC
             WHERE (REASONTYPE_ID IN
                       (SELECT id
                          FROM REPORTS.SP_CONTRACT_TERM_REASON_TYPE
                         WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND TO_DATE (VERSION_DATE) = TO_DATE (V_VERSION_DATE))
                    OR REASONTYPE_ID IS NULL)
                   AND (STATE_ID IN
                           (SELECT id
                              FROM REPORTS.SP_STATUS
                             WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                   AND TO_DATE (VERSION_DATE) =
                                          TO_DATE (V_VERSION_DATE))
                        OR STATE_ID IS NULL)
                   AND (STAGE_ID IN
                           (SELECT id
                              FROM REPORTS.T_CONTRACT_STAGE
                             WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                   AND TO_DATE (VERSION_DATE) =
                                          TO_DATE (V_VERSION_DATE))
                        OR STAGE_ID IS NULL)
                   AND (TYPE_ID IN
                           (SELECT id
                              FROM REPORTS.SP_CONTRACT_TERMINATION_TYPE
                             WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                   AND TO_DATE (VERSION_DATE) =
                                          TO_DATE (V_VERSION_DATE))
                        OR TYPE_ID IS NULL)
                   AND ID = ENTITY_ID
                   AND DELETED_DATE IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CONTRACT_KPGZ [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CONTRACT_KPGZ';
    rec_array(idx).sql_name := 'LNK_CONTRACT_KPGZ [EAIST2]';
    rec_array(idx).description := 'Строки ГК по КПГЗ';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.LNK_CONTRACT_KPGZ (ID,
                                                ID_CONTRACT,
                                                ID_KPGZ,
                                                COST_IN_RUBLE,
                                                ID_DATA_SOURCE,
                                                VERSION_DATE)
            SELECT ID,
                   CONTRACT_ID,
                   KPGZ_ID,
                   COST_IN_RUBLE,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE
              FROM CONTRACT_SPECIFICATION@EAIST_MOS_RC
             WHERE (CONTRACT_ID IN
                       (SELECT id
                          FROM REPORTS.T_CONTRACT
                         WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND VERSION_DATE = V_VERSION_DATE)
                    OR CONTRACT_ID IS NULL)
                   AND (KPGZ_ID IN (SELECT id FROM eaist_nsi.N_KPGZ@EAIST_MOS_RC)
                        OR KPGZ_ID IS NULL)
                   AND DELETED_DATE IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_CLAIM [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_CLAIM';
    rec_array(idx).sql_name := 'T_CONTRACT_CLAIM [EAIST2]';
    rec_array(idx).description := 'Штрафы по контрактам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_CONTRACT_CLAIM (ID,
                                               COMPUTED_COST,
                                               END_DATE,
                                               PAID_COST,
                                               REASON,
                                               START_DATE,
                                               ID_COLLECTTYPE,
                                               ID_CONTRACT,
                                               ID_REASONTYPE,
                                               ID_DATA_SOURCE,
                                               VERSION_DATE,
                                               IS_INITIATOR_CUSTOMER,
                                               IS_CANCEL_PENALTY)
            SELECT ID,
                   COMPUTED_COST,
                   END_DATE,
                   PAID_COST,
                   REASON,
                   START_DATE,
                   COLLECTTYPE_ID,
                   CONTRACT_ID,
                   REASONTYPE_ID,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   IS_INITIATOR_CUSTOMER,
                   CASE
                    WHEN END_DATE IS NOT NULL AND COMPUTED_COST<>0 AND NVL(PAID_COST, 0)=0 THEN 1
                    ELSE 0
                   END IS_CANCEL_PENALTY
              FROM CONTRACT_CLAIM@EAIST_MOS_RC
             WHERE (CONTRACT_ID IN
                       (SELECT id
                          FROM REPORTS.T_CONTRACT
                         WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND VERSION_DATE = V_VERSION_DATE)
                    OR CONTRACT_ID IS NULL)
                   AND (STATE_ID IN (SELECT id FROM STATE@EAIST_MOS_RC)
                        OR STATE_ID IS NULL)
                   AND (COLLECTTYPE_ID IN
                           (SELECT id
                              FROM CONTRACT_CLAIM_COLLECT_TYPE@EAIST_MOS_RC)
                        OR COLLECTTYPE_ID IS NULL)
                   AND (REASONTYPE_ID IN
                           (SELECT id
                              FROM CONTRACT_DOCUMENT_CATEGORY@EAIST_MOS_RC)
                        OR REASONTYPE_ID IS NULL)
                   AND DELETED_DATE IS NULL--and STATE_ID in (select ID from SP_STATUS where name like '%Зарегистрирован%')
                                           /*берем только со статусом "Зарегистрировано", поле со статусом убрали*/

         ;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_BUDJET_LIABILITY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_BUDJET_LIABILITY';
    rec_array(idx).sql_name := 'T_CONTRACT_BUDJET_LIABILITY [EAIST2]';
    rec_array(idx).description := 'Бюджетное обязательство';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_CONTRACT_BUDJET_LIABILITY (
                        ID,
                        CONTRACT_ID,
                        FUNCTIONAL,
                        PURPOSE,
                        EXPENSE,
                        SUMMA,
                        BUDGET_YEAR,
                        DATE_BUDJECT_LIABILITY,
                        NUMBER_BUDJECT_LIABILITY,
                        GRBS,
                        ECONOMIC,
                        STATUS_ID,
                        ID_DATA_SOURCE,
                        VERSION_DATE)
            SELECT ID,
                               CONTRACT_ID,
                               CASE
                                  WHEN (KBK_CODE LIKE '%-%') and (LENGTH(KBK_CODE)=24) THEN SUBSTR (KBK_CODE, 18, 3)
                                  WHEN (KBK_CODE LIKE '%-%') and (LENGTH(KBK_CODE)=27) THEN SUBSTR (KBK_CODE, 21, 3)
                                  ELSE NULL
                               END
                                  FUNCTIONAL_CODE,
                               CASE
                                  WHEN (KBK_CODE LIKE '%-%') and (LENGTH(KBK_CODE)=24) THEN SUBSTR (KBK_CODE, 10, 7)
                                  WHEN (KBK_CODE LIKE '%-%') and (LENGTH(KBK_CODE)=27) THEN SUBSTR (KBK_CODE, 10, 10)
                                  ELSE NULL
                               END
                                  PURPOSE_CODE,
                               CASE
                                  WHEN (KBK_CODE LIKE '%-%') and (LENGTH(KBK_CODE)>=20) THEN SUBSTR (KBK_CODE, 5, 4)
                                  ELSE NULL
                               END
                                  EXPENSE_CODE,
                               COST,
                               BUDGETYEAR_ID,
                               LIABILITY_DATE,
                               LIABILITY_NUMBER,
                               CASE
                                  WHEN (KBK_CODE LIKE '%-%') and (LENGTH(KBK_CODE)>=20) THEN SUBSTR (KBK_CODE, 1, 3)
                                  ELSE NULL
                               END
                                  GRBS_CODE,
                               CASE
                                  WHEN (LENGTH(KBK_CODE)<20) THEN KBK_CODE
                                  WHEN (KBK_CODE LIKE '%-%') and (LENGTH(KBK_CODE)=24) THEN SUBSTR (KBK_CODE, 22, 3)
                                  WHEN (KBK_CODE LIKE '%-%') and (LENGTH(KBK_CODE)=27) THEN SUBSTR (KBK_CODE, 25, 3)
                                  ELSE NULL
                               END
                                  ECONOMIC_CODE,
                               STATE_ID,
                               V_ID_DATA_SOURCE,
                               V_VERSION_DATE
                          FROM CONTRACT_LIABILITY@EAIST_MOS_RC cl
                         WHERE DELETED_DATE IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_AGREEMENT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_AGREEMENT';
    rec_array(idx).sql_name := 'T_CONTRACT_AGREEMENT [EAIST2]';
    rec_array(idx).description := 'Дополнительное соглашение';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          INSERT INTO REPORTS.T_CONTRACT_AGREEMENT (ID,
                                                   CONTRACT_ID,
                                                   CONCLUSION_DATE,
                                                   SIGN_NUMBER,
                                                   SUMM,
                                                   PREDSUMM,
                                                   ID_DATA_SOURCE,
                                                   VERSION_DATE,
                                                   PRICE_CHANGE_REASON_ID,
                                                   IS_ELECTRONIC_CONCLUSION,
                                                   ACTION_BEGIN,
                                                   ACTION_END)
          SELECT agr.id,
               c.id AS ID_CONTRACT,
               agr.DOCUMENT_DATE,
               agr.DOCUMENT_NUMBER,
               c.cost_in_ruble,
               c_prev.cost_in_ruble,
               V_ID_DATA_SOURCE,
               V_VERSION_DATE,
               agr.CHANGETYPE_ID,       /*CONTRACT_AGREEMENT_CHANGE_TYPE*/
               agr.IS_ELECTRONIC_CONCLUSION,
               agr.EXECUTION_START_DATE,
               agr.EXECUTION_END_DATE 
          FROM  (select id, entity_id,cost_in_ruble, edition_number from CONTRACT@EAIST_MOS_RC where IS_ACTUAL=1 and DELETED_DATE IS NULL) c
          JOIN (select * from CONTRACT_AGREEMENT@EAIST_MOS_RC where is_sum_changing = 1 AND DELETED_DATE IS NULL)  agr
          ON c.id = agr.contract_id
          left join (select id, entity_id, cost_in_ruble, edition_number from CONTRACT@EAIST_MOS_RC) c_prev
          on c.entity_id=c_prev.entity_id and c.edition_number-1=c.edition_number;
         /*INSERT INTO REPORTS.T_CONTRACT_AGREEMENT (ID,
                                                   CONTRACT_ID,
                                                   CONCLUSION_DATE,
                                                   SIGN_NUMBER,
                                                   PREDSUMM,
                                                   ID_DATA_SOURCE,
                                                   VERSION_DATE,
                                                   PRICE_CHANGE_REASON_ID,
                                                   IS_ELECTRONIC_CONCLUSION,
                                                   ACTION_BEGIN,
                                                   ACTION_END)
            SELECT agr.id,
                   c2.id AS ID_CONTRACT,
                   agr.DOCUMENT_DATE,
                   agr.DOCUMENT_NUMBER,
                   c2.cost_in_ruble,
                   V_ID_DATA_SOURCE,
                   V_VERSION_DATE,
                   agr.CHANGETYPE_ID,      
                   agr.IS_ELECTRONIC_CONCLUSION,
                   agr.EXECUTION_START_DATE,
                   agr.EXECUTION_END_DATE                      
              FROM contract@EAIST_MOS_RC c2
                   JOIN (  SELECT MAX (c1.edition_number) maxx, c1.entity_id
                             FROM    contract@EAIST_MOS_RC c1
                                  LEFT JOIN
                                     (  SELECT qq.entity_id,
                                               MIN (qq.edition_number) minn
                                          FROM (SELECT c.entity_id,
                                                       c.edition_number
                                                  FROM    CONTRACT@EAIST_MOS_RC c
                                                       JOIN
                                                          CONTRACT_AGREEMENT@EAIST_MOS_RC a
                                                       ON c.id = a.contract_id
                                                 WHERE     a.is_sum_changing = 1 
                                                       AND a.DELETED_DATE IS NULL
                                                       AND c.DELETED_DATE IS NULL) qq
                                      GROUP BY qq.entity_id) q
                                  ON c1.entity_id = q.entity_id
                                     AND c1.edition_number = q.minn - 1
                            WHERE     c1.registry_number_oos IS NOT NULL
                                  AND c1.registry_date IS NOT NULL
                                  AND c1.DELETED_DATE IS NULL
                                  AND c1.id = c1.ENTITY_ID
                         GROUP BY c1.entity_id) o
                      ON c2.edition_number = o.maxx
                         AND c2.entity_id = o.entity_id
                   JOIN (SELECT id,
                                contract_id,
                                DOCUMENT_DATE,
                                DOCUMENT_NUMBER,
                                CHANGETYPE_ID,
                                IS_ELECTRONIC_CONCLUSION,
                                EXECUTION_START_DATE,
                                EXECUTION_END_DATE
                           FROM CONTRACT_AGREEMENT@EAIST_MOS_RC
                          WHERE is_sum_changing = 1 AND deleted_date IS NULL) agr
                      ON c2.id = agr.contract_id
             WHERE C2.DELETED_DATE IS NULL AND c2.id = C2.ENTITY_ID
                   AND (c2.id IN
                           (SELECT ID
                              FROM REPORTS.T_CONTRACT
                             WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                   AND VERSION_DATE = V_VERSION_DATE));*/

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT - PLAN_DATE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT';
    rec_array(idx).sql_name := 'T_CONTRACT - PLAN_DATE [EAIST2]';
    rec_array(idx).description := 'Обновление плановой даты исполнения у контракта';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         UPDATE REPORTS.T_CONTRACT
            SET PLAN_DATE =
                   (  SELECT MAX (s.PLAN_DATE)
                        FROM    REPORTS.T_CONTRACT_STAGE s
                             JOIN
                                (  SELECT ID_CONTRACT,
                                          MAX (ID) AS id,
                                          ID_DATA_SOURCE,
                                          VERSION_DATE
                                     FROM REPORTS.T_CONTRACT_STAGE
                                 GROUP BY ID_CONTRACT,
                                          ID_DATA_SOURCE,
                                          VERSION_DATE) ss
                             ON     s.ID_CONTRACT = ss.ID_CONTRACT
                                AND s.id = ss.id
                                AND ss.ID_DATA_SOURCE = s.ID_DATA_SOURCE
                                AND TO_DATE (ss.VERSION_DATE) =
                                       TO_DATE (s.VERSION_DATE)
                       WHERE     SS.ID_CONTRACT = T_CONTRACT.ID
                             AND ss.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                             AND ss.VERSION_DATE = V_VERSION_DATE
                             AND s.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                             AND s.VERSION_DATE = V_VERSION_DATE
                             AND s.PLAN_DATE IS NOT NULL
                    GROUP BY s.ID_CONTRACT)
          WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE
                AND EXISTS
                       (  SELECT MAX (s.PLAN_DATE)
                            FROM    REPORTS.T_CONTRACT_STAGE s
                                 JOIN
                                    (  SELECT ID_CONTRACT,
                                              MAX (ID) AS id,
                                              ID_DATA_SOURCE,
                                              VERSION_DATE
                                         FROM REPORTS.T_CONTRACT_STAGE
                                     GROUP BY ID_CONTRACT,
                                              ID_DATA_SOURCE,
                                              VERSION_DATE) ss
                                 ON     s.ID_CONTRACT = ss.ID_CONTRACT
                                    AND s.id = ss.id
                                    AND ss.ID_DATA_SOURCE = s.ID_DATA_SOURCE
                                    AND ss.VERSION_DATE =
                                           TRUNC (s.VERSION_DATE, 'dd')
                           WHERE     SS.ID_CONTRACT = T_CONTRACT.ID
                                 AND ss.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                 AND ss.VERSION_DATE = V_VERSION_DATE
                                 AND s.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                 AND s.VERSION_DATE = V_VERSION_DATE
                        GROUP BY s.ID_CONTRACT);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT - IS_VIOLATION_EXEC [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT';
    rec_array(idx).sql_name := 'T_CONTRACT - IS_VIOLATION_EXEC [EAIST2]';
    rec_array(idx).description := 'Обновление флага is_violation_exec';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         UPDATE REPORTS.T_CONTRACT
            SET IS_VIOLATION_EXEC = 1
          WHERE     ID IN (SELECT CONTRACT_ID
                             FROM CONTRACT_STAGE@EAIST_MOS_RC
                            WHERE NVL (ACTUAL_DATE, SYSDATE) > PLAN_DATE)
                AND VERSION_DATE = V_VERSION_DATE
                AND ID_DATA_SOURCE = V_ID_DATA_SOURCE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT - END_DATE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT';
    rec_array(idx).sql_name := 'T_CONTRACT - END_DATE [EAIST2]';
    rec_array(idx).description := 'Определение даты расторжения';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        /*как определить тип - взять этап с максимальной плановой датой из всех этапов в статусе Зарегистрирован
        в найденном этапе найти запись о расторжении (CONTRACT_STAGE_TERMINATION), отличном от нарушения устранены.
        что же касается даты расторжения, то в этой же записи в таблице CONTRACT_STAGE_TERMINATION нужно взять:
        termination_date -  в случае типа расторжения "по решению заказчика в одностороннем порядке"
        document_date - во всех остальных случаях
        отличном от нарушения устранены.в том числе и с пустым значением в поле статус*/
         UPDATE REPORTS.T_CONTRACT RC
            SET END_DATE =
                   (SELECT CASE
                              WHEN t.TYPE_ID = 1 THEN t.TERMINATION_DATE
                              ELSE t.DOCUMENT_DATE
                           END
                              TERMINATION_DATE
                      FROM CONTRACT_STAGE_TERMINATION@EAIST_MOS_RC t
                           JOIN T_CONTRACT_STAGE cs
                              ON t.STAGE_ID = cs.ID
                           JOIN (  SELECT MAX (cs.ID) ID,
                                          cs.ID_CONTRACT,
                                          MAX (PLAN_DATE) PLAN_DATE
                                     FROM    T_CONTRACT_STAGE cs
                                          LEFT JOIN
                                             SP_STATUS st
                                          ON cs.ID_STATE = st.ID
                                             AND st.ID_DATA_SOURCE =
                                                    cs.ID_DATA_SOURCE
                                             AND st.VERSION_DATE =
                                                    cs.VERSION_DATE
                                    WHERE ST.NAME LIKE
                                             'Зарегистрирован%'
                                          AND st.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                          AND cs.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                          AND cs.VERSION_DATE =
                                                 V_VERSION_DATE
                                          AND st.VERSION_DATE =
                                                 V_VERSION_DATE
                                 GROUP BY cs.ID_CONTRACT) max_date
                              ON cs.ID = max_date.ID --cs.CONTRACT_ID=max_date.CONTRACT_ID and cs.PLAN_DATE=max_date.PLAN_DATE
                           JOIN T_CONTRACT con
                              ON cs.ID_CONTRACT = con.ID
                                 AND cs.ID_DATA_SOURCE = con.ID_DATA_SOURCE
                                 AND cs.VERSION_DATE =
                                        V_VERSION_DATE
                           LEFT JOIN SP_STATUS st
                              ON t.STATE_ID = st.ID
                           LEFT JOIN SP_CONTRACT_TERMINATION_TYPE tp
                              ON t.TYPE_ID = tp.ID
                     WHERE     t.STATE_ID != 304
                           AND cs.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                           AND st.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                           AND tp.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                           AND cs.VERSION_DATE = V_VERSION_DATE
                           AND st.VERSION_DATE = V_VERSION_DATE
                           AND tp.VERSION_DATE = V_VERSION_DATE
                           AND con.VERSION_DATE = V_VERSION_DATE
                           AND T.DELETED_DATE IS NULL
                           AND t.ID = T.ENTITY_ID
                           AND RC.ID = con.ID)
          WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE
                AND EXISTS
                       (SELECT con.ID
                          FROM CONTRACT_STAGE_TERMINATION@EAIST_MOS_RC t
                               JOIN T_CONTRACT_STAGE cs
                                  ON t.STAGE_ID = cs.ID
                               JOIN (  SELECT MAX (cs.ID) ID,
                                              cs.ID_CONTRACT,
                                              MAX (PLAN_DATE) PLAN_DATE
                                         FROM    T_CONTRACT_STAGE cs
                                              LEFT JOIN
                                                 SP_STATUS st
                                              ON cs.ID_STATE = st.ID
                                                 AND st.ID_DATA_SOURCE =
                                                        cs.ID_DATA_SOURCE
                                                 AND st.VERSION_DATE =
                                                        cs.VERSION_DATE
                                        WHERE ST.NAME LIKE
                                                 'Зарегистрирован%'
                                              AND st.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                              AND cs.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                              AND cs.VERSION_DATE =
                                                     V_VERSION_DATE
                                              AND st.VERSION_DATE =
                                                     V_VERSION_DATE
                                     GROUP BY cs.ID_CONTRACT) max_date
                                  ON cs.ID = max_date.ID --cs.CONTRACT_ID=max_date.CONTRACT_ID and cs.PLAN_DATE=max_date.PLAN_DATE
                               JOIN T_CONTRACT con
                                  ON cs.ID_CONTRACT = con.ID
                                     AND cs.ID_DATA_SOURCE =
                                            con.ID_DATA_SOURCE
                                     AND cs.VERSION_DATE = con.VERSION_DATE
                               LEFT JOIN SP_STATUS st
                                  ON t.STATE_ID = st.ID
                               LEFT JOIN SP_CONTRACT_TERMINATION_TYPE tp
                                  ON t.TYPE_ID = tp.ID
                         WHERE     t.STATE_ID != 304
                               AND cs.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND st.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND tp.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND cs.VERSION_DATE = V_VERSION_DATE
                               AND st.VERSION_DATE = V_VERSION_DATE
                               AND tp.VERSION_DATE = V_VERSION_DATE
                               AND con.VERSION_DATE = V_VERSION_DATE
                               AND T.DELETED_DATE IS NULL
                               AND t.ID = T.ENTITY_ID
                               AND RC.ID = con.ID);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT - ID_REASONTYPE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT';
    rec_array(idx).sql_name := 'T_CONTRACT - ID_REASONTYPE [EAIST2]';
    rec_array(idx).description := 'Определение причины расторжения';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         UPDATE REPORTS.T_CONTRACT RC
            SET ID_REASONTYPE =
                   (SELECT REASONTYPE_ID
                      FROM CONTRACT_STAGE_TERMINATION@EAIST_MOS_RC t
                           JOIN T_CONTRACT_STAGE cs
                              ON t.STAGE_ID = cs.ID
                           JOIN (  SELECT MAX (cs.ID) ID,
                                          cs.ID_CONTRACT,
                                          MAX (PLAN_DATE) PLAN_DATE
                                     FROM    T_CONTRACT_STAGE cs
                                          LEFT JOIN
                                             SP_STATUS st
                                          ON cs.ID_STATE = st.ID
                                             AND st.ID_DATA_SOURCE =
                                                    cs.ID_DATA_SOURCE
                                             AND st.VERSION_DATE =
                                                    cs.VERSION_DATE
                                    WHERE ST.NAME LIKE
                                             'Зарегистрирован%'
                                          AND st.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                          AND cs.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                          AND cs.VERSION_DATE =
                                                 V_VERSION_DATE
                                          AND st.VERSION_DATE =
                                                 V_VERSION_DATE
                                 GROUP BY cs.ID_CONTRACT) max_date
                              ON cs.ID = max_date.ID --cs.CONTRACT_ID=max_date.CONTRACT_ID and cs.PLAN_DATE=max_date.PLAN_DATE
                           JOIN T_CONTRACT con
                              ON     cs.ID_CONTRACT = con.ID
                                 AND cs.ID_DATA_SOURCE = con.ID_DATA_SOURCE
                                 AND cs.VERSION_DATE = con.VERSION_DATE
                           LEFT JOIN SP_STATUS st
                              ON t.STATE_ID = st.ID
                           LEFT JOIN SP_CONTRACT_TERMINATION_TYPE tp
                              ON t.TYPE_ID = tp.ID
                     WHERE     t.STATE_ID != 304
                           AND cs.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                           AND st.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                           AND tp.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                           AND cs.VERSION_DATE = V_VERSION_DATE
                           AND st.VERSION_DATE = V_VERSION_DATE
                           AND tp.VERSION_DATE = V_VERSION_DATE
                           AND con.VERSION_DATE = V_VERSION_DATE
                           AND T.DELETED_DATE IS NULL
                           AND t.ID = T.ENTITY_ID
                           AND RC.ID = con.ID)
          WHERE ID_DATA_SOURCE = V_ID_DATA_SOURCE AND VERSION_DATE = V_VERSION_DATE
                AND EXISTS
                       (SELECT con.ID
                          FROM CONTRACT_STAGE_TERMINATION@EAIST_MOS_RC t
                               JOIN T_CONTRACT_STAGE cs
                                  ON t.STAGE_ID = cs.ID
                               JOIN (  SELECT MAX (cs.ID) ID,
                                              cs.ID_CONTRACT,
                                              MAX (PLAN_DATE) PLAN_DATE
                                         FROM    T_CONTRACT_STAGE cs
                                              LEFT JOIN
                                                 SP_STATUS st
                                              ON cs.ID_STATE = st.ID
                                                 AND st.ID_DATA_SOURCE =
                                                        cs.ID_DATA_SOURCE
                                                 AND st.VERSION_DATE =
                                                        cs.VERSION_DATE
                                        WHERE ST.NAME LIKE
                                                 'Зарегистрирован%'
                                              AND st.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                              AND cs.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                                              AND cs.VERSION_DATE =
                                                     V_VERSION_DATE
                                              AND st.VERSION_DATE =
                                                     V_VERSION_DATE
                                     GROUP BY cs.ID_CONTRACT) max_date
                                  ON cs.ID = max_date.ID --cs.CONTRACT_ID=max_date.CONTRACT_ID and cs.PLAN_DATE=max_date.PLAN_DATE
                               JOIN T_CONTRACT con
                                  ON cs.ID_CONTRACT = con.ID
                                     AND cs.ID_DATA_SOURCE =
                                            con.ID_DATA_SOURCE
                                     AND cs.VERSION_DATE = con.VERSION_DATE
                               LEFT JOIN SP_STATUS st
                                  ON t.STATE_ID = st.ID
                               LEFT JOIN SP_CONTRACT_TERMINATION_TYPE tp
                                  ON t.TYPE_ID = tp.ID
                         WHERE     t.STATE_ID != 304
                               AND cs.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND st.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND tp.ID_DATA_SOURCE = V_ID_DATA_SOURCE
                               AND cs.VERSION_DATE = V_VERSION_DATE
                               AND st.VERSION_DATE = V_VERSION_DATE
                               AND tp.VERSION_DATE = V_VERSION_DATE
                               AND con.VERSION_DATE = V_VERSION_DATE
                               AND T.DELETED_DATE IS NULL
                               AND t.ID = T.ENTITY_ID
                               AND RC.ID = con.ID);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CONTRACT_LOT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CONTRACT_LOT';
    rec_array(idx).sql_name := 'LNK_CONTRACT_LOT [EAIST2]';
    rec_array(idx).description := 'Связь контрактов и лотов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.LNK_CONTRACT_LOT (ID_LOT,ID_CONTRACT,ID_DATA_SOURCE,VERSION_DATE)
         select distinct
                lv.id
                ,c.id as contract
                ,V_ID_DATA_SOURCE
                ,V_VERSION_DATE
            from eaist_rc.contract@eaist_mos_shard c
                join eaist_rc.contract_lot@eaist_mos_shard cl on c.lot_id = cl.id --соединяем контракт с лотом в рк
                left join (select 
                          nvl(lv.ID, maxlv.ID) ID,
                          nvl(lv.ENTITY_ID, maxlv.ENTITY_ID) ENTITY_ID
                          from 
                          (SELECT l.ID,
                                                          l.ENTITY_ID,
                                                          l.REASON_CONCLUSION_CONTRACT,
                                                          l.CONTRACT_NMC,
                                                          l.LOT_NAME,
                                                          l.METHOD_OF_SUPPLIER_ID,
                                                          l.SMP_TYPE,
                                                          l.CAN_MULTIPLE,
                                                          l.STATUS_ID,
                                                          L.START_DATE,
                                                          l.bidding_on_unit_production,
                                                          l.joint_auction
                                                     FROM D_LOT_VERSION@EAIST_MOS_SHARD l
                                                    WHERE deleted_date IS NULL) lv 
                          FULL JOIN                                                   
                          (SELECT l.ID,
                                  l.ENTITY_ID,
                                  l.REASON_CONCLUSION_CONTRACT,
                                  l.CONTRACT_NMC,
                                  l.LOT_NAME,
                                  l.METHOD_OF_SUPPLIER_ID,
                                  l.SMP_TYPE,
                                  l.CAN_MULTIPLE,
                                  l.STATUS_ID,
                                  L.START_DATE,
                                  l.bidding_on_unit_production,
                                  l.joint_auction
                             FROM D_LOT_VERSION@EAIST_MOS_SHARD l
                             join
                              (select distinct max(id)  over (partition by entity_id) maxid, entity_id from D_LOT_VERSION@EAIST_MOS_SHARD) maxl
                              on l.entity_id=maxl.entity_id and l.id=maxl.maxid) maxlv
                           ON lv.entity_id=maxlv.entity_id) lv on lv.entity_id = cl.ext_id --соединяем лот в рк с лотом в торгах
                left join d_procedure_lot_entry@eaist_mos_shard ple on ple.lot_id = lv.id and ple.is_actual = 1 --ищем связь несовместных лотов и процедур
                left join d_lot_lot_entry@eaist_mos_shard lle on lv.id = lle.lot_id and lle.is_actual = 1 --ищем связь совместных лотов с главными совместными лотами
                left join d_procedure_lot_entry@eaist_mos_shard ple1 on ple1.lot_id = lle.root_lot_id and ple1.is_actual = 1 --ищем связь главных совместных лотов с процедурами
                where c.is_actual = 1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_LOT_LOT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_LOT_LOT';
    rec_array(idx).sql_name := 'LNK_LOT_LOT [EAIST2]';
    rec_array(idx).description := 'Совместные закупки';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO LNK_LOT_LOT (ID, LOT_ID, ROOT_LOT_ID, ID_DATA_SOURCE, VERSION_DATE)
         select ID, LOT_ID, ROOT_LOT_ID, V_ID_DATA_SOURCE ID_DATA_SOURCE, V_VERSION_DATE VERSION_DATE from d_lot_lot_entry@eaist_mos_shard where is_actual=1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_LOT_LOT - INCLUDED_JOINT_AUCTION [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT';
    rec_array(idx).sql_name := 'T_LOT - INCLUDED_JOINT_AUCTION [EAIST2]';
    rec_array(idx).description := 'Обновление included_joint_auction';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      --Обновление included_joint_auction из LNK_LOT_LOT
      merge into t_lot l
      using (select distinct lot_id from LNK_LOT_LOT where id_data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE) lll
      on (l.id=lll.lot_id and l.id_data_source=V_ID_DATA_SOURCE and l.version_date=V_VERSION_DATE)
      when matched then update set
      l.included_joint_auction=1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

	-- LNK_LOT_LOT - IS_UNION_TRADE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_TENDER';
    rec_array(idx).sql_name := 'T_TENDER - IS_UNION_TRADE[EAIST2]';
    rec_array(idx).description := 'Обновление is_union_trade';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      --Обновление included_joint_auction из LNK_LOT_LOT
      merge into t_tender trg
    
      using (select distinct t.id from
      (select distinct lot_id, version_date, id_Data_source from LNK_LOT_LOT where id_data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE) lll
      join t_lot l on lll.lot_id=l.id and lll.version_date=l.version_date and lll.id_data_source=l.id_Data_source
      join t_tender t on t.id=l.tender_id and t.version_date=l.version_date and t.id_Data_source=l.id_data_source) src
      on (src.id=trg.id and trg.id_data_source=V_ID_DATA_SOURCE and trg.version_date=V_VERSION_DATE)
      when matched then update set
      trg.is_union_trade=1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT_MEMBER - EA [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT_MEMBER';
    rec_array(idx).sql_name := 'T_LOT_MEMBER - EA [EAIST2]';
    rec_array(idx).description := 'Заявки по электронным аукционам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#

INSERT INTO REPORTS.T_LOT_MEMBER (ID,
                                           ID_LOT,
                                           ID_SUPPLIER,
                                           REQUEST_NUMBER,
                                           REGISTRATION_NUMBER,
                                           PLACE,
                                           STATE,
                                           METHOD_OF_SUPPLIER_ID,
                                           ID_DATA_SOURCE,
                                           VERSION_DATE,
                                           GRBS)

select  distinct
              d.ea_ID||41||(CASE WHEN l.joint_auction=1 or l.included_joint_auction=1 then l.id end) id, 
              l.id lot_id, 
              win.participant_id, 
              d.journal_number REQUEST_NUMBER, 
              d.journal_number REGISTRATION_NUMBER,
              win.place place,
              --(case when d.admitted_second=1 or (d.admitted_second is null and d.admitted is null) or (eo.id is not null) then 3 
              (case when d.admitted_second=1 or (eo.id is not null) then 3 --edited by Abramov 18.07.2016 10:36 task 1038
                   when d.admitted=1 and d.admitted_second is null and eo.id is null then 1
                   else 2 end) as STATE,
              4 METHOD_OF_SUPPLIER_ID,
              V_ID_DATA_SOURCE, 
              V_VERSION_DATE, 
              lcag.grbs_code
  from
  (select distinct journal_number, oos_reg_number, max(admitted_second) admitted_second, max(admitted) admitted, max(id) ea_id
  from D_EA_APPLICATION@eaist_mos_shard
  group by journal_number, oos_reg_number) d
  join (select id, registry_number from t_tender where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE) t on d.oos_reg_number=t.registry_number
  join (select id, tender_Id, version_date, customer_id, joint_auction, included_joint_auction from t_lot where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE) l on l.tender_id=t.id
  --left join d_ea_winner@eaist_mos_shard win on d.journal_number=win.journal_number and d.oos_reg_number=win.oos_reg_number
  left join (select distinct journal_number, oos_reg_number, max(price) over (partition by journal_number, oos_reg_number) max_price , max(added_at) over (partition by journal_number, oos_reg_number) max_date, place, participant_id, price, added_at
  from d_ea_winner@eaist_mos_shard) win on d.journal_number=win.journal_number and d.oos_reg_number=win.oos_reg_number and win.added_at=win.max_date and win.price=win.max_price
  left join d_ea_price_offer@eaist_mos_shard eo on d.oos_reg_number=eo.oos_reg_number and d.journal_number=eo.journal_number
  left join (select lcag.id lnk_id, cust.grbs_code, lcag.version_date
               from LNK_CUSTOMERS_ALL_LEVEL lcag
               join sp_customer cust ON lcag.id_parent=cust.id||'_'||cust.id_DATA_source and lcag.version_date=cust.version_date and cust.connect_level=3  and cust.version_date=V_VERSION_DATE
                 union
                select id||'_2', grbs_code, version_date from sp_customer where connect_level=3 and id_Data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE) lcag  
     ON l.CUSTOMER_ID||'_2'=lcag.lnk_id and l.version_date=lcag.version_date;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_MEMBER_BID - EA [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_MEMBER_BID';
    rec_array(idx).sql_name := 'T_MEMBER_BID - EA [EAIST2]';
    rec_array(idx).description := 'Заявки по электронным аукционам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.T_MEMBER_BID (ID,
                                           LOT_MEMBER_ID,
                                           BID_DATE,
                                           PRICE,
                                           ID_DATA_SOURCE,
                                           VERSION_DATE)
          select distinct 
            win.ID||4||(CASE WHEN l.joint_auction=1 or l.included_joint_auction=1 then l.id end) winid, 
            win.ID||4||(CASE WHEN l.joint_auction=1 or l.included_joint_auction=1 then l.id end), 
            d.APP_DATE, 
            win.PRICE, 
            V_ID_DATA_SOURCE id_Data_source, 
            V_VERSION_DATE version_date 
          from (select win.*, t.id tender_id, rank() over (partition by journal_number, oos_reg_number order by price, added_at desc) rnk
                from d_ea_winner@eaist_mos_shard win 
                join t_tender t on win.procedure_id=t.entity_id and t.id_data_source=V_ID_DATA_SOURCE and t.version_date=V_VERSION_DATE) win
          join (select id, tender_Id, joint_auction, included_joint_auction from t_lot where version_date= V_VERSION_DATE and id_data_source= V_ID_DATA_SOURCE) l on l.tender_id=win.tender_id and win.rnk=1
          left join (select journal_number, oos_reg_number, min(app_date) app_date 
                     from D_EA_APPLICATION@eaist_mos_shard
                     group by journal_number, oos_reg_number) d
          on d.journal_number=win.journal_number and d.oos_reg_number=win.oos_reg_number;
    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_BANK [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_BANK';
    rec_array(idx).sql_name := 'T_BANK [EAIST2]';
    rec_array(idx).description := 'Информация о банках';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          insert into T_BANK (ID, NAME, ENTITY_ID, ID_DATA_SOURCE, VERSION_DATE, SHORT_NAME, CITY, POST_INDEX, ADDRESS, CORRESPONDENT_ACCOUNT, BIK, PHONE, OKATO_TER_CODE, OKPO_CODE, REG_NUM)
          select ID, NAME, ENTITY_ID, V_ID_DATA_SOURCE id_data_source, V_VERSION_DATE Version_date, SHORT_NAME, CITY, POST_INDEX, ADDRESS, CORRESPONDENT_ACCOUNT, BIK, PHONE, OKATO_TERR_CODe, OKPO_CODE, REG_NUM 
          from n_bank@eaist_mos_nsi WHERE DELETED_DATE IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_BANK_DETAIL [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_BANK_DETAIL';
    rec_array(idx).sql_name := 'T_BANK_DETAIL [EAIST2]';
    rec_array(idx).description := 'Детальная информация о банках';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          INSERT INTO T_BANK_DETAIL (ID, BIK, ACCOUNT_NUMBER, PARTICIPANT_ID, SETTLEMENT_ACCOUNT_NUMBER, ACCOUNTTYPE_ID, ID_DATA_SOURCE, VERSION_DATE)
          select ID, BIK, ACCOUNT_NUMBER, PARTICIPANT_ID, SETTLEMENT, ACCOUNT_TYPE, V_ID_DATA_SOURCE ID_DATA_SOURCE, V_VERSION_DATE VERSION_DATE 
          from N_BANK_DETAIL@eaist_mos_NSI where deleted_Date is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT_NMC_EXAMINATION [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT_NMC_EXAMINATION';
    rec_array(idx).sql_name := 'T_LOT_NMC_EXAMINATION [EAIST2]';
    rec_array(idx).description := 'Заявки на экспертизу НМЦ лота';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO T_LOT_NMC_EXAMINATION (ID, LOT_ID, SEND_DATE, APPROVED_DATE, EXPECTED_EXAMINATION_DATE, CURRENT_STATE, EXPECTED_EXAMINATION_END_DATE, ID_DATA_SOURCE, version_date, GRBS)
        SELECT ex.ID, ex.LOT_ID, ex.SEND_DATE, ex.APPROVED_DATE, ex.EXPECTED_EXAMINATION_DATE, ex.CURRENT_STATE, ex.EXPECTED_EXAMINATION_END_DATE, V_ID_DATA_SOURCE ID_DATA_SOURCE, V_VERSION_DATE version_date, grbs_code 
        FROM (SELECT ID, LOT_ID, SEND_DATE, APPROVED_DATE, EXPECTED_EXAMINATION_DATE, CURRENT_STATE, EXPECTED_EXAMINATION_END_DATE FROM D_LOT_NMC_EXAMINATION@eaist_mos_shard) ex
        JOIN (SELECT * FROM T_LOT WHERE id_data_source=V_ID_DATA_SOURCE) lots ON lots.id_entity=ex.lot_id AND lots.version_date=V_VERSION_DATE AND lots.id_Data_source=V_ID_DATA_SOURCE
        LEFT JOIN (SELECT lcag.id lnk_id, cust.grbs_code, lcag.version_date
                  FROM LNK_CUSTOMERS_ALL_LEVEL lcag
                  JOIN sp_customer cust ON lcag.id_parent=cust.id||'_'||cust.id_DATA_source AND lcag.version_date=cust.version_date AND cust.connect_level=3
                  UNION
                  SELECT id||'_2', grbs_code, version_date FROM sp_customer WHERE connect_level=3 AND id_Data_source=V_ID_DATA_SOURCE) lcag  
                  ON lots.CUSTOMER_ID||'_2'=lcag.lnk_id AND lots.version_date=lcag.version_date;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT_NMC_EXAM_CONCLUSION [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT_NMC_EXAM_CONCLUSION';
    rec_array(idx).sql_name := 'T_LOT_NMC_EXAM_CONCLUSION [EAIST2]';
    rec_array(idx).description := 'Заключение по экспертизе о НМЦ лота';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO T_LOT_NMC_EXAM_CONCLUSION (ID, LOT_NMC_EXAMINATION_ID, APPROVED_DATE, CONCLUSION_DATE, CONCLUSION_NUMBER, CONCLUSION_NMC, RESULT, ID_DATA_SOURCE, version_date, GRBS)
        SELECT conc.ID, conc.LOT_NMC_EXAMINATION_ID, conc.APPROVED_DATE, conc.CONCLUSION_DATE, conc.CONCLUSION_NUMBER, conc.CONCLUSION_NMC, conc.RESULT, V_ID_DATA_SOURCE ID_DATA_SOURCE, V_VERSION_DATE version_date, GRBS
        FROM (SELECT ID, LOT_NMC_EXAMINATION_ID, APPROVED_DATE, CONCLUSION_DATE, CONCLUSION_NUMBER, CONCLUSION_NMC, RESULT FROM D_LOT_NMC_EXAM_CONCLUSION@eaist_mos_shard) conc
        JOIN t_lot_nmc_examination exam on exam.id_data_source=V_ID_DATA_SOURCE AND exam.versioN_date=V_VERSION_DATE AND conc.LOT_NMC_EXAMINATION_ID=exam.id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_DELIVERY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_DELIVERY';
    rec_array(idx).sql_name := 'T_CONTRACT_DELIVERY [EAIST2]';
    rec_array(idx).description := 'Сведения об исполнении поставщиком';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          INSERT INTO T_CONTRACT_DELIVERY (ID, CONDITION, END_DATE, ENTITY_ID, QUANTITY, START_DATE, PERIODTYPE_ID, SPECIFICATION_ID, ID_DATA_SOURCE, VERSION_DATE)
          select ID, CONDITION,  END_DATE, ENTITY_ID, QUANTITY, START_DATE, PERIODTYPE_ID, SPECIFICATION_ID, V_ID_DATA_SOURCE ID_DATA_SOURCE, V_VERSION_DATE VERSION_DATE 
          from contract_delivery@eaist_mos_rc where deleted_date is null and is_actual=1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_DELIVERY_PER_TYPE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_DELIVERY_PER_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_DELIVERY_PER_TYPE [EAIST2]';
    rec_array(idx).description := 'Тип периода поставки';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          INSERT INTO SP_CONTRACT_DELIVERY_PER_TYPE (ID, DESCRIPTION, NAME, ID_DATA_SOURCE, VERSION_DATE)
          select ID, DESCRIPTION, NAME, V_ID_DATA_SOURCE ID_DATA_SOURCE, V_VERSION_DATE VERSION_DATE from CONTRACT_DELIVERY_PERIOD_TYPE@eaist_mos_rc;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_DETAILED_PURCHASE_DELIVERY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_DETAILED_PURCHASE_DELIVERY';
    rec_array(idx).sql_name := 'T_DETAILED_PURCHASE_DELIVERY [EAIST2]';
    rec_array(idx).description := '';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          insert into T_detailed_purchase_delivery 
          (id, delivery_amount, delivery_date, days_count, is_absolute_date,Dpurchase_spec_id, delivery_date_start, days_count_start, conditions, version_date, id_data_source)
          select id, delivery_amount, delivery_date, days_count, is_absolute_date,Dpurchase_spec_id, delivery_date_start, days_count_start, conditions, V_VERSION_DATE version_date, V_ID_DATA_SOURCE id_data_source 
          from d_detailed_purchase_delivery@eaist_mos_shard;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_OOS_FTP_PROTOCOL [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_OOS_FTP_PROTOCOL';
    rec_array(idx).sql_name := 'T_OOS_FTP_PROTOCOL [EAIST2]';
    rec_array(idx).description := 'Протоколы закупок';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO T_OOS_FTP_PROTOCOL (id,procedure_entity_id,protocol_date, sign_date, publish_date,process_order, GRBS, version_date, id_data_source, protocol_type)
          SELECT prot.id,procedure_entity_id,protocol_date, sign_date, publish_date,process_order, grbs_code, V_VERSION_DATE version_date, V_ID_DATA_SOURCE id_data_source, protocol_type  
          FROM (SELECT id, procedure_entity_id, protocol_date, sign_date, publish_date, process_order, protocol_type FROM D_OOS_FTP_PROTOCOL@EAIST_MOS_SHARD) prot
          LEFT JOIN T_TENDER prod ON prod.ENTITY_ID=prot.PROCEDURE_ENTITY_ID AND prod.VERSION_DATE=V_VERSION_DATE AND prod.ID_DATA_SOURCE=V_ID_DATA_SOURCE
          LEFT JOIN (SELECT lcag.id lnk_id, cust.grbs_code, lcag.version_date
                    FROM LNK_CUSTOMERS_ALL_LEVEL lcag
                    JOIN sp_customer cust ON lcag.id_parent=cust.id||'_'||cust.id_DATA_source AND lcag.version_date=cust.version_date AND cust.connect_level=3
                    UNION
                    SELECT id||'_2', grbs_code, version_date FROM sp_customer WHERE connect_level=3 AND id_Data_source=V_ID_DATA_SOURCE) lcag  
          ON prod.ID_CUSTOMER_EAIST2||'_2'=lcag.lnk_id and prod.version_date=lcag.version_date;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_COMMISSION_SESSION_E2 [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_COMMISSION_SESSION_E2';
    rec_array(idx).sql_name := 'T_COMMISSION_SESSION_E2 [EAIST2]';
    rec_array(idx).description := 'Информация о выбранной комиссии и о ее заседании по рассмотрению заявок поставщиков на участие в закупках';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          insert into T_COMMISSION_SESSION_E2 (id, procedure_id, session_date, approved_date, protocol_published_date, protocol_number, version_date, id_data_source)
          select id, procedure_id, session_date, approved_date, protocol_published_date,  protocol_number, V_VERSION_DATE version_date, V_ID_DATA_SOURCE id_data_source  from d_commission_session@EAIST_MOS_SHARD;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CONTRACT_DOCUMENT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CONTRACT_DOCUMENT';
    rec_array(idx).sql_name := 'LNK_CONTRACT_DOCUMENT [EAIST2]';
    rec_array(idx).description := '';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          INSERT INTO LNK_contract_document (ID, DOCUMENT_DATE, DOCUMENT_NUMBER, CATEGORY_ID, CONTRACT_ID, DOCUMENT_ID, IS_FROM_LIBRARY, version_date, id_data_source)
          select ID, DOCUMENT_DATE, DOCUMENT_NUMBER, CATEGORY_ID, CONTRACT_ID, DOCUMENT_ID, IS_FROM_LIBRARY, V_VERSION_DATE version_date, V_ID_DATA_SOURCE id_data_source 
          from contract_document@eaist_mos_rc where deleted_date is null and is_actual=1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_PAYMENT [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_PAYMENT';
    rec_array(idx).sql_name := 'T_CONTRACT_PAYMENT [EAIST2]';
    rec_array(idx).description := 'Фактические платежи по контракту';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          INSERT INTO T_CONTRACT_PAYMENT (ID, COST, PAYMENT_DATE, CONTRACT_ID, COST_IN_RUBLE, CURRENCY_ID, STAGEFINANCING_ID, STATE_ID, ADVANCE_COST, DOCUMENT_DATE, 
                                          DOCUMENT_NUMBER, CATEGORY_ID, DOCUMENT_ID, IS_ADVANCE_COST, STAGE_ID, version_date, id_data_source)
          select  ID, COST, PAYMENT_DATE, CONTRACT_ID, COST_IN_RUBLE, CURRENCY_ID, STAGEFINANCING_ID, STATE_ID, ADVANCE_COST, DOCUMENT_DATE, 
                  DOCUMENT_NUMBER, CATEGORY_ID, DOCUMENT_ID, IS_ADVANCE_COST, STAGE_ID, V_VERSION_DATE version_date, V_ID_DATA_SOURCE id_data_source
          from contract_payment@eaist_mos_rc where is_actual=1 and deleted_date is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_ECONOMY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_ECONOMY';
    rec_array(idx).sql_name := 'T_CONTRACT_ECONOMY [EAIST2]';
    rec_array(idx).description := 'Сведения по экономии';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          INSERT INTO T_CONTRACT_ECONOMY (ID, COST, COST_IN_RUBLE, ENTITY_ID, FINANCING_SOURCE_ID, KBK_CLAUSE, KBK_DEVISION, KBK_EXPENSE_TYPE, 
                                          KBK_GRBS, KBK_KOSGU, BUDGET_YEAR,CONTRACT_ID, VERSION_DATE,ID_DATA_SOURCE)
          select CE.ID, CE.COST, CE.COST_IN_RUBLE, CE.ENTITY_ID, sf.id as financing_source_id, CE.KBK_CLAUSE, CE.KBK_DEVISION, CE.KBK_EXPENSE_TYPE, 
                  CE.KBK_GRBS, CE.KBK_KOSGU, CE.BUDGETYEAR_ID as BUDGET_YEAR, CE.CONTRACT_ID, V_VERSION_DATE VERSION_DATE, V_ID_DATA_SOURCE ID_DATA_SOURCE
          from Contract_Economy@eaist_mos_rc ce
          JOIN SP_SOURCE_FINANCE sf on ce.financing_source_type=sf.code and sf.id_data_source=V_ID_DATA_SOURCE and sf.version_date=V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CURRENCY [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CURRENCY';
    rec_array(idx).sql_name := 'SP_CURRENCY [EAIST2]';
    rec_array(idx).description := '';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          Insert into SP_CURRENCY (ID, DESCRIPTION, NAME, CODE, PRIORITY, VERSION_DATE, ID_DATA_SOURCE)
          select ID, DESCRIPTION, NAME, CODE, PRIORITY, V_VERSION_DATE VERSION_DATE, V_ID_DATA_SOURCE ID_DATA_SOURCE from currency@eaist_mos_rc;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_PARTICIPANT_BANK_DETAILS [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_PARTICIPANT_BANK_DETAILS';
    rec_array(idx).sql_name := 'LNK_PARTICIPANT_BANK_DETAILS [EAIST2]';
    rec_array(idx).description := '';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          INSERT INTO LNK_PARTICIPANT_BANK_DETAILS (PARTICIPANT_ID, BANK_DETAIL_ID, VERSION_DATE, ID_DATA_SOURCE)
          select PARTICIPANT_ID, BANK_DETAIL_ID, V_VERSION_DATE VERSION_DATE, V_ID_DATA_SOURCE ID_DATA_SOURCE from N_PARTICIPANT_BANK_DETAILS@eaist_mos_nsi;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- TMP_OLDNEW_KPGZ [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'TMP_OLDNEW_KPGZ';
    rec_array(idx).sql_name := 'TMP_OLDNEW_KPGZ [EAIST2]';
    rec_array(idx).description := 'Обновление TMP_OLDNEW_KPGZ';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        execute immediate 'truncate table tmp_oldnew_kpgz';
        
        insert into tmp_oldnew_kpgz (old_id, ENTITY_ID, NEW_ID)
        select dkp.ID OLD_ID, dkp.ENTITY_ID, akp.ID NEW_ID
        FROM N_KPGZ@EAIST_MOS_NSI dkp
        INNER JOIN N_KPGZ@EAIST_MOS_NSI akp 
        ON dkp.ENTITY_ID=akp.ENTITY_ID and dkp.DELETED_DATE IS NOT NULL and akp.DELETED_DATE IS NULL;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE - KPGZ [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE';
    rec_array(idx).sql_name := 'T_PURCHASE - KPGZ [EAIST2]';
    rec_array(idx).description := 'Изменение КПГЗ на актуальный';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
            merge into t_purchase tp
            using  (
              select tp.id, tp.version_date, tp.id_data_source, dkp.new_id
              from t_purchase tp
              inner join tmp_oldnew_kpgz dkp
              ON  tp.KPGZ_ID=dkp.OLD_ID and tp.version_date=V_VERSION_DATE and tp.id_data_source=V_ID_DATA_SOURCE ) dkp
            on (tp.id=dkp.id and tp.version_date=dkp.version_date  and tp.id_data_source=dkp.id_data_source)
            when matched then update set 
            tp.kpgz_id=dkp.new_id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PURCHASE_DETAILED_SPEC - KPGZ [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PURCHASE_DETAILED_SPEC';
    rec_array(idx).sql_name := 'T_PURCHASE_DETAILED_SPEC - KPGZ [EAIST2]';
    rec_array(idx).description := 'Изменение КПГЗ на актуальный';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
              merge into T_PURCHASE_DETAILED_SPEC tp
              using  (
                select tp.rowid ri, tp.id, tp.version_date, tp.id_data_source, dkp.new_id
                from T_PURCHASE_DETAILED_SPEC tp
                inner join tmp_oldnew_kpgz dkp
                ON  tp.KPGZ_ID=dkp.OLD_ID and tp.version_date=V_VERSION_DATE and tp.id_data_source=V_ID_DATA_SOURCE ) dkp
              on (tp.rowid=dkp.ri)
              when matched then update set 
              tp.kpgz_id=dkp.new_id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CONTRACT_KPGZ - KPGZ [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CONTRACT_KPGZ';
    rec_array(idx).sql_name := 'LNK_CONTRACT_KPGZ - KPGZ [EAIST2]';
    rec_array(idx).description := 'Изменение КПГЗ на актуальный';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
              merge into LNK_CONTRACT_KPGZ tp
              using  (
                select tp.id, tp.version_date, tp.id_data_source, dkp.new_id
                from LNK_CONTRACT_KPGZ tp
                inner join tmp_oldnew_kpgz dkp
                ON  tp.ID_KPGZ=dkp.OLD_ID and tp.version_date=V_VERSION_DATE and tp.id_data_source=V_ID_DATA_SOURCE ) dkp
              on (tp.id=dkp.id and tp.version_date=dkp.version_date  and tp.id_data_source=dkp.id_data_source)
              when matched then update set 
              tp.id_kpgz=dkp.new_id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_MER_ALL_LEVEL [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_MER_ALL_LEVEL';
    rec_array(idx).sql_name := 'LNK_MER_ALL_LEVEL [EAIST2]';
    rec_array(idx).description := 'Таблица связей объединенных МЭР';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into LNK_MER_ALL_LEVEL (id, ID_PARENT, CONNECT_LEVEL, version_date, id_data_source)
        select id, ID_FIRST, id_level, version_date, id_data_source 
        from
          (SELECT id,
                 to_number(trim(',' FROM sys_connect_by_path(CASE WHEN LEVEL = 1 THEN id END, ','))) ID_FIRST,
                 id_level, version_date, id_data_source 
          from (
          select id, id_data_source, nvl(id_parent,0) id_parent, version_date, regexp_count(code,'\.')+2 id_level
          from sp_mer_code where version_date=V_VERSION_DATE
          and id_data_source=V_ID_DATA_SOURCE and lower(code) not like '%разд%' 
             union
          select 0, 2, null, V_VERSION_DATE, 1 from dual
          ) soj
          connect BY PRIOR id =id_parent)
        where id<>ID_FIRST;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_KPGZ_ALL_LEVEL [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_KPGZ_ALL_LEVEL';
    rec_array(idx).sql_name := 'LNK_KPGZ_ALL_LEVEL [EAIST2]';
    rec_array(idx).description := 'Таблица связей объединенных КПГЗ';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      insert into LNK_KPGZ_ALL_LEVEL (id, ID_PARENT, CONNECT_LEVEL, version_date, id_data_source)
      select id, ID_FIRST, id_level, version_date, id_data_source 
      from
        (SELECT id,
               to_number(trim(',' FROM sys_connect_by_path(CASE WHEN LEVEL = 1 THEN id END, ','))) ID_FIRST,
               id_level, version_date, id_data_source 
        from (
        select id, id_data_source, nvl(id_parent,0) id_parent, version_date, regexp_count(code,'\.')+2 id_level
        from sp_kpgz where version_date=V_VERSION_DATE
        and id_data_source=V_ID_DATA_SOURCE
          union
        select 0, 2, null, V_VERSION_DATE, 1 from dual
        ) soj
        connect BY PRIOR id =id_parent)
      where id<>ID_FIRST;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_METHOD [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_METHOD';
    rec_array(idx).sql_name := 'SP_METHOD [EAIST1]';
    rec_array(idx).description := 'Справочник способа объявления заказа';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into sp_method(id,name,id_data_source,VERSION_DATE) --select id, NAME, V_ID_DATA_SOURCE, V_VERSION_DATE from vocabulary@tkdbn1 where voc_type_id=4011;
        select id, NAME, V_ID_DATA_SOURCE, V_VERSION_DATE from vocabulary@tkdbn1 where id in (select distinct tender_type_id from tender@tkdbn1 where tender_type_id is not null);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER_RATING [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER_RATING';
    rec_array(idx).sql_name := 'SP_CUSTOMER_RATING [EAIST1]';
    rec_array(idx).description := 'Справочник рейтингов заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_CUSTOMER_RATING
                      ( ID, CUSTOMER_ID,PERIOD,PERIOD_TYPE,PERIOD_TEXT, COUNT_OF_CONTRACT_DETECTED,COUNT_OF_CUSTOMER_RESPONS, DETECTED_GLAV_CONTROL, YEAR, ID_DATA_SOURCE,VERSION_DATE)
                    select ID, CUSTOMER_ID,
                            case 
                                when period like 'Год' then 'y'
                                when period like '1-е полугодие' then 'h'
                             end PERIOD,
                             case 
                                when period like 'Год' then 0
                                when period like '1-е полугодие' then 1
                             end PERIOD_TYPE,
                             PERIOD PERIOD_TEXT, 
                             COUNT_OF_CONTRACT_DETECTED,COUNT_OF_CUSTOMER_RESPONS, DETECTED_GLAV_CONTROL, YEAR, V_ID_DATA_SOURCE,V_VERSION_DATE
                      from 
                      N_CUSTOMER_RATING@EAIST_MOS_NSI
                      where DELETED_DATE is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CONNECTION_ORGANIZATION [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CONNECTION_ORGANIZATION';
    rec_array(idx).sql_name := 'LNK_CONNECTION_ORGANIZATION [EAIST1]';
    rec_array(idx).description := 'Рассчетные связи родителей организаций с детьми всех уровней';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        for j in (
          select * from sp_customer where ID_DATA_SOURCE = V_ID_DATA_SOURCE and version_date = V_VERSION_DATE
        ) 
        loop
          insert into LNK_CONNECTION_ORGANIZATION (id,id_child,ID_DATA_SOURCE,parent_level,child_LEVEL,VERSION_DATE)
            SELECT  j.id, id as child_id, ID_DATA_SOURCE,j.connect_level parent_level,level as child_LEVEL, VERSION_DATE
            FROM   sp_customer o    
            START WITH    id = j.id and ID_DATA_SOURCE=j.ID_DATA_SOURCE and version_date=j.version_date
            CONNECT BY     id_parent=PRIOR id and ID_DATA_SOURCE=prior ID_DATA_SOURCE and version_date=prior version_date;
        end loop;
        delete from  LNK_CONNECTION_ORGANIZATION where id=id_child;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_TORG_TYPE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_TORG_TYPE';
    rec_array(idx).sql_name := 'SP_TORG_TYPE [EAIST1]';
    rec_array(idx).description := 'Справочник типов заказа';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      insert into SP_TORG_TYPE(ID,NAME,ID_DATA_SOURCE,VERSION_DATE) --select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from vocabulary@tkdbn1 where voc_type_id=4005;
        select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from vocabulary@tkdbn1 where id in (select distinct torg_type_id from tender@tkdbn1 where torg_type_id is not null);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_STATUS - TENDER [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_STATUS';
    rec_array(idx).sql_name := 'SP_STATUS - TENDER [EAIST1]';
    rec_array(idx).description := 'Статусы тендеров';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_STATUS (ID, NAME, id_category,ID_DATA_SOURCE,VERSION_DATE)
            select rownum as id, workflow_status as NAME, 16 as id_category, V_ID_DATA_SOURCE, V_VERSION_DATE from (select distinct workflow_status from tender@tkdbn1 order by workflow_status);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_TENDER [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_TENDER';
    rec_array(idx).sql_name := 'T_TENDER [EAIST1]';
    rec_array(idx).description := 'Тендер';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_TENDER(ID
                                        ,PUBLICATION_DATE
                                        ,REGISTRY_NUMBER
                                        ,ID_TENDER_TYPE
                                        ,SUBJECT
                                        ,ID_TORG_TYPE
                                        ,IS_44FZ
                                        ,WORKFLOW_STATUS
                                        ,ID_ENTERPRISE_ENTITY
                                        ,IS_UNION_TRADE
                                        ,PUBLISH_OOS_DATE
                                        ,LAST_PROTOCOL_DATE_FROM_OOS
                                        ,CREATE_PROTOCOL_DATE          
                                        ,TENDER_LEVEL
                                        ,ok_registry_number
                                        ,is_active
                                        ,ID_STATUS
                                        ,CANCEL_DATE
                                        ,CHANGE_COUNT
                                        ,PUBLICATION_CHANGE_DATE
                                        ,REQUEST_END_DATE
                                        ,WAS_PUBLISHED
                                        ,ID_DATA_SOURCE
                                        ,VERSION_DATE) 
            select    ID
                        ,PUBLICATION_DATE
                        ,REGISTRY_NUMBER
                        ,TENDER_TYPE_ID
                        ,SUBJECT
                        ,TORG_TYPE_ID
                        ,IS_44FZ
                        ,WORKFLOW_STATUS
                        ,ENTERPRISE_ENTITY_ID
                        ,IS_UNION_TRADE
                        ,PUBLISH_OOS_DATE
                        ,LAST_PROTOCOL_DATE_FROM_OOS
                        ,CREATE_PROTOCOL_DATE          
                        ,TENDER_LEVEL
                        ,ok_registry_number                       
                        ,case when WORKFLOW_STATUS in ('Торги отменены','Торги не состоялись') then 0 else 1 end is_active
                        ,st.ID_STATUS
                        ,CANCELDATE
                        ,CHANGE_COUNT
                        ,PUBLICATION_CHANGE_DATE
                        ,REQUEST_END
                        ,case when PUBLISH_OOS_DATE is not null then 1 else 0 end WAS_PUBLISHED
                        ,V_ID_DATA_SOURCE
                        ,V_VERSION_DATE
            from tender@tkdbn1 td
            left join 
            (select id as ID_STATUS, name, id_data_source, version_date  from SP_STATUS where id_category=16 and id_data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE) st
            on td.workflow_status=st.name;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT';
    rec_array(idx).sql_name := 'T_LOT [EAIST1]';
    rec_array(idx).description := 'Лот';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_LOT(  ID,
                            ID_ENTITY
                                    ,TENDER_ID   
                                    ,IS_UNIT    
                                    ,NAME    
                                    ,IS_SMALL
                                    ,IS_MULTI_WINNER
                                    ,LOT_NUMBER 
                                    ,MAXIMUM_CONTRACT_COST
                                    ,LOT_REGISTRY_NUMBER_PLAN
                                    ,LOT_REGISTRY_NUMBER
                                    ,is_active                                                             
                                    ,ID_DATA_SOURCE
                                    ,VERSION_DATE
                                    ,CUSTOMER_ID
                                    ,ID_METHODOFSUPPLIER
                                    ,INCLUDED_JOINT_AUCTION) 
        select l.ID, l.ID,l.TENDER_ID,l.IS_UNIT,l.LOT_NAME,l.IS_SMALL,multi_winner,l.LOT_NUMBER,sl.nmc,t.ok_registry_number,t.REGISTRY_NUMBER,1,V_ID_DATA_SOURCE,V_VERSION_DATE, c.entity_id, t.TENDER_TYPE_ID,
        t.IS_UNION_TRADE
            from lot@tkdbn1 l /*(select * from                                                                                                                                                                       
                     (select ID,TENDER_ID,IS_UNIT,LOT_NAME,IS_SMALL,LOT_NUMBER,row_number() over (partition by id order by id) cnt  from lot@tkdbn1) lot      
                    where lot.cnt=1) l*/
            left join (select nvl(sum(BYDGET_FINANSING),0) nmc,lot_id from lot_specification@tkdbn1 group by lot_id) sl on sl.lot_id=l.id
            left join tender@tkdbn1 t on l.TENDER_ID=t.id
            left join (SELECT curr.ID, curr.entity_id
                        FROM enterprise@tkdbn1 curr,
                            (  SELECT entity_id, MAX (t.date_start) max_date, COUNT (*) cnt
                                 FROM enterprise@tkdbn1 t
                             GROUP BY entity_id) mv
                        WHERE curr.entity_id = mv.entity_id AND curr.date_start = mv.max_date) c on c.entity_id=t.ENTERPRISE_ENTITY_ID;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT - PUBLICATION DATES [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT';
    rec_array(idx).sql_name := 'T_LOT - PUBLICATION DATES [EAIST1]';
    rec_array(idx).description := 'Обновление publication_date';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        merge into t_lot trg using
          (select l.id, t.publication_date, t.publish_oos_date 
          from (select * from t_lot where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE) l
          left join (select * from t_tender where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE) t 
          on l.tender_id=t.id 
          where publication_date is not null or publish_oos_date is not null) src
        on (trg.id=src.id)
        when matched then
        update set trg.plan_publication_date = src.publication_date, trg.FACT_PUBLICATION_DATE=src.publish_oos_date;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT_MEMBER [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT_MEMBER';
    rec_array(idx).sql_name := 'T_LOT_MEMBER [EAIST1]';
    rec_array(idx).description := 'Участник, подавший заявку';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_LOT_MEMBER( ID   
                                            ,ID_LOT        
                                            ,ID_SUPPLIER   
                                            ,REQUEST_NUMBER
                                            ,RANK          
                                            ,STATE
                                            ,PLACE
                                            ,ID_DATA_SOURCE
                                            ,VERSION_DATE) 
         select 
          lm.id,
          lm.lot_id,
          org.entity_id,
          lm.request_number,
          lm.rank,
          lm.state,
          lm.rank, 
          V_ID_DATA_SOURCE,
          V_VERSION_DATE
         from (select * from lot_member@tkdbn1 where supplier_id is not null) lm 
            join enterprise@tkdbn1 org
            on lm.supplier_id=org.id;--select ID,LOT_ID,SUPPLIER_ID,REQUEST_NUMBER,RANK,STATE ,V_ID_DATA_SOURCE,V_VERSION_DATE from lot_member@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SOURCE_FINANCE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SOURCE_FINANCE';
    rec_array(idx).sql_name := 'SP_SOURCE_FINANCE [EAIST1]';
    rec_array(idx).description := 'Источник финансирования';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_SOURCE_FINANCE(ID,CODE,name,ID_DATA_SOURCE,VERSION_DATE) select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from FINANCING_SOURCE_TYPE@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SECTION_BUDGET [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SECTION_BUDGET';
    rec_array(idx).sql_name := 'SP_SECTION_BUDGET [EAIST1]';
    rec_array(idx).description := 'Раздел/подраздел бюджета';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_SECTION_BUDGET(ID,CODE,name,ID_DATA_SOURCE,VERSION_DATE) select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from EXPENSE_FORM@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_TYPE_EXPENSE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_TYPE_EXPENSE';
    rec_array(idx).sql_name := 'SP_TYPE_EXPENSE [EAIST1]';
    rec_array(idx).description := 'Вид расходов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_TYPE_EXPENSE(ID,CODE,name,ID_DATA_SOURCE,VERSION_DATE) select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from FUNCTIONAL_EXPENSE_FORM@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KOSGU [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KOSGU';
    rec_array(idx).sql_name := 'SP_KOSGU [EAIST1]';
    rec_array(idx).description := 'КОСГУ';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_KOSGU(ID,CODE,NAME,ID_DATA_SOURCE,VERSION_DATE) select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from ECONOMIC_GRADING_CODE@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_TARGET_CLAUSE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_TARGET_CLAUSE';
    rec_array(idx).sql_name := 'SP_TARGET_CLAUSE [EAIST1]';
    rec_array(idx).description := 'Целевая статья';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_TARGET_CLAUSE(ID,CODE,name,ID_DATA_SOURCE,VERSION_DATE) select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from PURPOSE_EXPENSE_CLAUSE@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_GRBS [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_GRBS';
    rec_array(idx).sql_name := 'SP_GRBS [EAIST1]';
    rec_array(idx).description := 'ГРБС';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_GRBS(ID,CODE,DESCRIPTION,ID_DATA_SOURCE,VERSION_DATE) select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from GRBS_FORM@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_MEMBER_BID [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_MEMBER_BID';
    rec_array(idx).sql_name := 'T_MEMBER_BID [EAIST1]';
    rec_array(idx).description := 'Ценовое предложение участника';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_MEMBER_BID(ID,LOT_MEMBER_ID,PRICE,BID_DATE,ID_DATA_SOURCE,VERSION_DATE) select ID,LOT_MEMBER_ID,PRICE,BID_DATE,V_ID_DATA_SOURCE,V_VERSION_DATE from MEMBER_BID@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_OKDP_INNOVATION [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_OKDP_INNOVATION';
    rec_array(idx).sql_name := 'SP_OKDP_INNOVATION [EAIST1]';
    rec_array(idx).description := 'Cправочник инновационной продукции ОКПД';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_OKDP_INNOVATION(ID,PRODUCT_RUBRICATOR_ID,NAME,ID_DATA_SOURCE,VERSION_DATE) select ID,PRODUCT_RUBRICATOR_ID,NAME,V_ID_DATA_SOURCE,V_VERSION_DATE from OKDP_INNOVATION@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT_SPECIFICATION [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT_SPECIFICATION';
    rec_array(idx).sql_name := 'T_LOT_SPECIFICATION [EAIST1]';
    rec_array(idx).description := 'Сведения о спецификациях лотов (актуальна для ЕАИСТ1, для ЕАИСТ2 собирается по данным из финансирования и спецификации ДОЗ)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    insert into T_LOT_SPECIFICATION( ID              
                                                      ,ID_LOT          
                                                      ,BYDGET_FINANSING
                                                      ,BUDGET_YEAR     
                                                      ,ID_PRODUCT      
                                                      ,ID_OKDP         
                                                      ,ID_OKPD         
                                                      ,ID_MC_PRODUCT   
                                                      ,ID_FOLDER       
                                                      ,ID_EXPENSE      
                                                      ,ID_ECONOMIC     
                                                      ,ID_TARGET       
                                                      ,ID_GRBS         
                                                      ,UNIT_PRICE      
                                                      ,PRODUCTION_COUNT
                                                      ,ID_INNOVATION   
                                                      ,ID_FIN_SOURCE   
                                                      ,ID_CUSTOMER     
                                                      ,ID_DATA_SOURCE
                                                      ,VERSION_DATE ) 
        select   ID              
                  ,LOT_ID          
                  ,BYDGET_FINANSING
                  ,BUDGET_YEAR     
                  ,PRODUCT_ID      
                  ,OKDP_ID         
                  ,OKPD_ID         
                  ,MC_PRODUCT_ID   
                  ,FOLDER_ID       
                  ,EXPENSE_ID      
                  ,ECONOMIC_ID     
                  ,TARGET_ID       
                  ,GRBS_ID         
                  ,UNIT_PRICE      
                  ,PRODUCTION_COUNT
                  ,INNOVATION_ID   
                  ,FIN_SOURCE_ID   
                  ,CUSTOMER_ID 
                  ,V_ID_DATA_SOURCE
                  ,V_VERSION_DATE
        from lot_specification@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_TERM_REASON_TYPE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_TERM_REASON_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_TERM_REASON_TYPE [EAIST1]';
    rec_array(idx).description := 'Причины расторжения контракта';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CONTRACT_TERM_REASON_TYPE (id,name,id_data_source,version_date)
            select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from c_vocabulary@tkdbn1 where id in (select distinct termination_reason_id from execution@tkdbn1);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_TERMINATION_TYPE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_TERMINATION_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_TERMINATION_TYPE [EAIST1]';
    rec_array(idx).description := 'Типы причин расторжения';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CONTRACT_TERMINATION_TYPE (id,name,id_data_source,version_date)
            select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from c_vocabulary@tkdbn1 where id in (select distinct termination_id from execution@tkdbn1);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_STATUS_CATEGORY [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_STATUS_CATEGORY';
    rec_array(idx).sql_name := 'SP_STATUS_CATEGORY [EAIST1]';
    rec_array(idx).description := 'Справочник категорий статусов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (
                        1,
                        'contract',
                        'Статусы для договоров',
                        'Договор',
                        V_ID_DATA_SOURCE,
                        V_VERSION_DATE);    
      
         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (
                        2,
                        'purchase',
                        'Статусы объектов закупки (ОЗ)',
                        'ОЗ',
                        V_ID_DATA_SOURCE,
                        V_VERSION_DATE);

         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (3,
                      'lot',
                      'Статусы для лотов',
                      'Лот',
                      V_ID_DATA_SOURCE,
                      V_VERSION_DATE);

         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (4,
                      'plan_purchase',
                      'Статусы для плана закупок',
                      'План закупок',
                      V_ID_DATA_SOURCE,
                      V_VERSION_DATE);

         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (
                        5,
                        'doz',
                        'Статусы детализированных объектов закупки',
                        'ДОЗ',
                        V_ID_DATA_SOURCE,
                        V_VERSION_DATE);
                        
         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (
                        12,
                        '',
                        'Статусы бюджетных обязательств (БО)',
                        'БО',
                        V_ID_DATA_SOURCE,
                        V_VERSION_DATE);

         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (
                        14,
                        'PlanSchedule',
                        'Статусы планов-графиков детализированных объектов закупки',
                        'План-график',
                        V_ID_DATA_SOURCE,
                        V_VERSION_DATE);

         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (15,
                      'BidStatus',
                      'Статусы заявок',
                      'Статусы заявок',
                      V_ID_DATA_SOURCE,
                      V_VERSION_DATE);

         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (16,
                      '',
                      'Статусы процедур закупок',
                      'Статусы процедур закупок',
                      V_ID_DATA_SOURCE,
                      V_VERSION_DATE);

         INSERT INTO REPORTS.SP_STATUS_CATEGORY (ID,
                                                 CODE,
                                                 DESCRIPTION,
                                                 NAME,
                                                 ID_DATA_SOURCE,
                                                 VERSION_DATE)
              VALUES (17,
                      '',
                      'Состояние процедур закупок',
                      'Состояние процедур закупок',
                      V_ID_DATA_SOURCE,
                      V_VERSION_DATE);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_STATUS - INSERT [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_STATUS';
    rec_array(idx).sql_name := 'SP_STATUS - INSERT [EAIST1]';
    rec_array(idx).description := 'Статусы этапов контрактов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_STATUS(ID,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) 
            select id,name,10,V_ID_DATA_SOURCE,V_VERSION_DATE from c_vocabulary@tkdbn1 where id in (select distinct type_id from execution@tkdbn1)-- расторгнут/исполнен этап контракта
            union all
            select id,name,10,V_ID_DATA_SOURCE,V_VERSION_DATE from c_vocabulary@tkdbn1 where id in (select distinct status_id from execution@tkdbn1);/* статус исполнения этапа контракта*/

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_AGR_CHANGE_TYPE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_AGR_CHANGE_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_AGR_CHANGE_TYPE [EAIST1]';
    rec_array(idx).description := 'Обоснование изменения цены контракта';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CONTRACT_AGR_CHANGE_TYPE(ID,NAME,ID_DATA_SOURCE,VERSION_DATE) 
        select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from c_vocabulary@tkdbn1 where id in (select distinct PRICE_CHANGE_REASON_ID from agreement@tkdbn1);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_AGREEMENT [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_AGREEMENT';
    rec_array(idx).sql_name := 'T_CONTRACT_AGREEMENT [EAIST1]';
    rec_array(idx).description := 'Дополнительное соглашение';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_CONTRACT_AGREEMENT(ID                      
                                                                ,CONTRACT_ID             
                                                                ,SUMM                    
                                                                ,PREDSUMM                
                                                                ,CONCLUSION_DATE         
                                                                ,ACTION_BEGIN            
                                                                ,ACTION_END              
                                                                ,SIGN_NUMBER             
                                                                ,PRICE_CHANGE_REASON_ID  
                                                                ,NOTICE_DATE             
                                                                ,IS_ELECTRONIC_CONCLUSION
                                                                ,ID_DATA_SOURCE          
                                                                ,VERSION_DATE ) 
        select ID                      
                 ,CONTRACT_ID             
                 ,SUMM                    
                 ,PREDSUMM                
                 ,CONCLUSION_DATE         
                 ,ACTION_BEGIN            
                 ,ACTION_END              
                 ,SIGN_NUMBER             
                 ,PRICE_CHANGE_REASON_ID  
                 ,NOTICE_DATE             
                 ,IS_ELECTRONIC_CONCLUSION,
                 V_ID_DATA_SOURCE,
                 V_VERSION_DATE
                 from AGREEMENT@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_STATUS - CONTRACT [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_STATUS';
    rec_array(idx).sql_name := 'SP_STATUS - CONTRACT [EAIST1]';
    rec_array(idx).description := 'Статусы контрактов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_STATUS(ID,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) select id,name,1,V_ID_DATA_SOURCE,V_VERSION_DATE from WF_STATE@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SINGLE_VENDOR_PURCHASE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SINGLE_VENDOR_PURCHASE';
    rec_array(idx).sql_name := 'SP_SINGLE_VENDOR_PURCHASE [EAIST1]';
    rec_array(idx).description := 'Основание закупки у единственного поставщика';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_SINGLE_VENDOR_PURCHASE(ID,NAME,ID_DATA_SOURCE,VERSION_DATE) 
            select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from nsi_vocabulary@tkdbn1 where id in (select distinct singlesource_id from contract@tkdbn1);/*select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from c_vocabulary@tkdbn1 where VOC_TYPE_ID in (3,42);42 лишний, для согл данных, есть таск*/

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_SUB_CONTRACT [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_SUB_CONTRACT';
    rec_array(idx).sql_name := 'T_SUB_CONTRACT [EAIST1]';
    rec_array(idx).description := 'Субподрядчики по контрактам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_SUB_CONTRACT(
                                                      ID,
                                                      CONTRACT_ID,
                                                      organization_id,
                                                      SIGN_DATE,
                                                      DOC_NUMBER,
                                                      ACTION_DATE,
                                                      PRICE,
                                                      STATUS_ID,
                                                      ID_DATA_SOURCE,
                                                      VERSION_DATE )
            select ID,CONTRACT_ID,ENTERPRISE_ENTITY_ID,SIGN_DATE,DOC_NUMBER,ACTION_DATE,PRICE,STATUS_ID,V_ID_DATA_SOURCE,V_VERSION_DATE from sub_contract@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT';
    rec_array(idx).sql_name := 'T_CONTRACT [EAIST1]';
    rec_array(idx).description := 'Контракт';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    insert into T_CONTRACT(ID      
                                                ,ID_STATUS                 
                                                ,ID_CUSTOMER             
                                                ,ID_SUPPLIER        
                                                ,CONTRACT_NUMBER 
                                                ,registry_number_oos
                                                ,CONTRACT_DATE   
                                                ,COST_IN_RUBLE
                                                ,DURATION_START_DATE -- дата начала действия контракта
                                                ,PLAN_DATE_START --Плановая дата начала исполнения контракта 
                                                ,DURATION_END_DATE -- дата окончания действия контракта
                                                ,START_DATE --Дата начала исполнения (фактичекая) - в еаист1 совпадает с плановой датой начала исполнения контракта
                                                ,END_DATE  -- в execution - Дата окончания исполнения(фактическая)
                                                ,ID_SINGLE_VENDOR_PURCHASE  
                                                ,ID_REASONTYPE 
                                                ,ID_TERMINATION_TYPE           
                                                ,PLAN_DATE      -- Плановая дата окончания исполнения (максимальная дата последнего этапа контракта), тоже что и "Дата регистрации исполнения"
                                                ,REG_DATE       
                                                ,SIGN_SUMM      
                                                ,EP_SUMM        
                                                ,EXECUTION_BEGIN --  Дата регистрации исполнения 
                                                ,sign_number    
                                                ,PAYMENT_SUM       
                                                ,ID_EXT_SYSTEM                          
                                                ,ID_DATA_SOURCE
                                                ,VERSION_DATE     ) 
            select ID                   
                    ,WF_STATE_ID        
                    ,CUSTOMER_ENTITY_ID 
                    ,SUPPLIER_ENTITY_ID
                    ,SIGN_NUMBER
                    ,REESTR_NUMBER_807PP            
                    ,SIGN_DATE          
                    ,SUMM   
                    ,ACTION_BEGIN   
                    ,EXECUTION_BEGIN --Срок исполнения (начало)
                    ,ACTION_END
                    ,EXECUTION_BEGIN
                    ,fed.fact_end
                    ,SINGLESOURCE_ID
                    ,tr.termination_reason_id
                    ,tr.termination_id 
                    ,ped.plan_end
                    ,REG_DATE       
                    ,SIGN_SUMM      
                    ,EP_SUMM        
                    ,ped.plan_end
                    ,sign_number 
                    ,ps.payment_summ
                    ,EXT_SYSTEM_ID
                    ,V_ID_DATA_SOURCE
                    ,V_VERSION_DATE
            from contract@tkdbn1 c
            left join (select max(fact_end_date) fact_end, contract_id from execution@tkdbn1 group by contract_id) fed on c.id=fed.contract_id --Фактическая дата окончания срока исполнения/Фактическая дата окончания контракта/Фактическая дата расторжения государственного контракта
            left join (select max(plan_end_date) plan_end, contract_id from execution@tkdbn1 group by contract_id) ped on c.id=ped.contract_id --Плановая дата окончания срока исполнения/Плановая дата окончания контракта/Плановая дата расторжения государственного контракта
            left join (select  termination_reason_id,termination_id,contract_id from execution@tkdbn1 where termination_reason_id is not null and type_id=71 and status_id=1203) tr on c.id=tr.contract_id
            left join (select nvl(sum(payment_summ),0) payment_summ, contract_id from CONTRACT_BUDJET_LIABILITY@tkdbn1 where status_id = 20001 group by contract_id) ps on c.id=ps.contract_id;            

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_SPEC [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_SPEC';
    rec_array(idx).sql_name := 'T_CONTRACT_SPEC [EAIST1]';
    rec_array(idx).description := 'Спецификации по контрактам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    insert into t_contract_spec 
		(id, 
		version_date, 
		contract_id, 
		quantity, 
		cost_in_ruble, 
		okpd_id, 
		id_data_source)
	select 
		id, 
		V_VERSION_DATE, 
		contract_id, 
		quantity, 
		sum_in_currency_nds, 
		okpd_id, 
		V_ID_DATA_SOURCE 
	from specification@tkdbn1 
	where id<>519953;            

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CONTRACT_LOT [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CONTRACT_LOT';
    rec_array(idx).sql_name := 'LNK_CONTRACT_LOT [EAIST1]';
    rec_array(idx).description := 'Связь контрактов и лотов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into LNK_CONTRACT_LOT(ID_CONTRACT,ID_LOT,ID_EXT_SYSTEM,ID_DATA_SOURCE,VERSION_DATE) 
            select CONTRACT_ID,LOT_ID,EXT_SYSTEM_ID,V_ID_DATA_SOURCE,V_VERSION_DATE from CONTRACT_LOT@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_FINANSING_CONTRACTS [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_FINANSING_CONTRACTS';
    rec_array(idx).sql_name := 'T_FINANSING_CONTRACTS [EAIST1]';
    rec_array(idx).description := 'Финансирование контракта. Соответствует таблице FINANSING в ЕИАСТ1';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_FINANSING_CONTRACTS(ID                     
                                                                ,ID_CONTRACT          
                                                                ,ID_FINANCING_SOURCE  
                                                                ,ID_PURPOSE           
                                                                ,ID_GRBS              
                                                                ,ID_EXPENSE        
                                                                ,ID_FOLDER           
                                                                ,ID_ECONOMIC          
                                                                ,SUMMA_IN_CURRENCY_NDS
                                                                ,BUDGET_YEAR          
                                                                ,ECONOMIC_SUMM        
                                                                ,ID_DATA_SOURCE
                                                                ,VERSION_DATE
                                                                ,PURPOSE_CODE
                                                                ,GRBS_CODE
                                                                ,EXPENSE_CODE
                                                                ,FOLDER_CODE
                                                                ,ECONOMIC_CODE) 
            select CF.ID                     
                    ,CF.CONTRACT_ID          
                    ,CF.FINANCING_SOURCE_ID  
                    ,CF.PURPOSE_ID --sp_target_clause           
                    ,CF.GRBS_ID --SP_GRBS             
                    ,CF.FUNCTIONAL_ID  --SP_TYPE_EXPENSE      
                    ,CF.EXPENSE_ID   --SP_SECTION_BUDGET        
                    ,CF.ECONOMIC_ID --SP_KOSGU         
                    ,CF.SUMMA_IN_CURRENCY_NDS
                    ,CF.BUDGET_YEAR          
                    ,CF.ECONOMIC_SUMM
                    ,V_ID_DATA_SOURCE 
                    ,V_VERSION_DATE
                    ,cl.CODE PURPOSE_CODE
                    ,GRBS.CODE GRBS_CODE
                    ,sec.CODE EXPENSE_CODE
                    ,exp.CODE FOLDER_CODE
                    ,kos.CODE ECONOMIC_CODE
            from FINANSING@tkdbn1 CF
            LEFT JOIN REPORTS.SP_TARGET_CLAUSE cl
                ON CF.PURPOSE_ID = cl.ID and cl.ID_DATA_SOURCE = V_ID_DATA_SOURCE   AND cl.VERSION_DATE = V_VERSION_DATE
             LEFT JOIN REPORTS.SP_GRBS GRBS
                ON CF.GRBS_ID = GRBS.ID AND GRBS.ID_DATA_SOURCE = V_ID_DATA_SOURCE  AND GRBS.VERSION_DATE = V_VERSION_DATE
             LEFT JOIN REPORTS.SP_TYPE_EXPENSE EXP
                ON CF.FUNCTIONAL_ID = EXP.ID AND EXP.ID_DATA_SOURCE = V_ID_DATA_SOURCE  AND EXP.VERSION_DATE = V_VERSION_DATE
             LEFT JOIN REPORTS.SP_SECTION_BUDGET sec
                ON CF.EXPENSE_ID = sec.ID AND sec.ID_DATA_SOURCE = V_ID_DATA_SOURCE  AND sec.VERSION_DATE = V_VERSION_DATE
             LEFT JOIN REPORTS.SP_KOSGU kos 
                ON CF.ECONOMIC_ID = kos.ID AND kos.ID_DATA_SOURCE = V_ID_DATA_SOURCE  AND kos.VERSION_DATE = V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_DOCUMENT_CATEGORY [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_DOCUMENT_CATEGORY';
    rec_array(idx).sql_name := 'SP_CONTRACT_DOCUMENT_CATEGORY [EAIST1]';
    rec_array(idx).description := 'Cправочник категорий документов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CONTRACT_DOCUMENT_CATEGORY(ID,
                                                  NAME,
                                                  ID_DATA_SOURCE,
                                                  VERSION_DATE) 
            select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from c_vocabulary@tkdbn1 where VOC_TYPE_ID in (22,23);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CLAIM_TYPE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CLAIM_TYPE';
    rec_array(idx).sql_name := 'SP_CLAIM_TYPE [EAIST1]';
    rec_array(idx).description := 'Типы штрафов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_CLAIM_TYPE (ID,NAME,DESCRIPTION,ID_DATA_SOURCE,VERSION_DATE)
            select ID, NAME, NAME, V_ID_DATA_SOURCE,V_VERSION_DATE from c_vocabulary@tkdbn1 where voc_type_id=65;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_CLAIM [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_CLAIM';
    rec_array(idx).sql_name := 'T_CONTRACT_CLAIM [EAIST1]';
    rec_array(idx).description := 'Штрафы по контрактам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_CONTRACT_CLAIM(ID
                                                        ,END_DATE
                                                        ,PAID_COST
                                                        ,ID_CONTRACT
                                                        ,ID_REASONTYPE
                                                        ,PENALTY_DATE
                                                        ,SIGN_DATE
                                                        ,ID_SUPPLIER
                                                        ,ID_RESULT_CLAIM
                                                        ,ID_DATA_SOURCE
                                                        ,VERSION_DATE
                                                        ,ID_COLLECTTYPE) 
            select ID
            ,FINISH_DATE
            ,PENALTY_SUMMA
            ,CONTRACT_ID
            ,BASE_CLAIM_ID
            ,PENALTY_DATE
            ,SIGN_DATE
            ,SUPPLIER_ID
            ,RESULT_CLAIM_ID
            ,V_ID_DATA_SOURCE
            ,V_VERSION_DATE
            ,PENALTY_TYPE_ID
            from CLAIM_WORK@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KPGZ [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KPGZ';
    rec_array(idx).sql_name := 'SP_KPGZ [EAIST1]';
    rec_array(idx).description := 'Классификатор предметов государственного заказа (КПГЗ)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_KPGZ (ID,ID_PARENT,CODE,NAME,ID_DATA_SOURCE,VERSION_DATE) 
            select ID,PARENT_ID,CODE,NAME,V_ID_DATA_SOURCE,V_VERSION_DATE from MC_RUBRICATOR_EXT@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KPGZ - 2,3 LVL [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KPGZ';
    rec_array(idx).sql_name := 'SP_KPGZ - 2,3 LVL [EAIST1]';
    rec_array(idx).description := 'Простановка КПГЗ 2 и 3 уровня';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
           MERGE INTO SP_KPGZ sk
           USING (
              SELECT sk.id, sk.id_data_source, sk.version_date, sk2.id AS parent_id
              FROM sp_KPGZ sk
              INNER JOIN sp_KPGZ sk2 ON sk.id_data_source=sk2.id_data_source AND sk.version_date=sk2.version_date
              AND substr(sk.code,1,INSTR(sk.code,'.',1,2)-1)=substr(sk2.code,1,INSTR(sk.code,'.',1,2)-1) AND INSTR(sk2.code,'.',1,2)=0 
              AND sk.version_date=V_VERSION_DATE AND sk.id_data_source=V_ID_DATA_SOURCE) t
           ON (sk.id=t.id AND sk.id_data_source=t.id_data_source AND sk.version_date=t.version_date AND sk.version_date=V_VERSION_DATE
           AND sk.id_data_source=V_ID_DATA_SOURCE)
           WHEN MATCHED THEN UPDATE SET
              sk.ID_PARENT_LEVEL2=t.parent_id;
              
            MERGE INTO SP_KPGZ sk
            USING (
              SELECT sk.id, sk.id_data_source, sk.version_date, sk3.id AS parent_id
              FROM sp_KPGZ sk
              INNER JOIN sp_KPGZ sk3 ON sk.id_data_source=sk3.id_data_source AND sk.version_date=sk3.version_date
              AND substr(sk.code,1,INSTR(sk.code,'.',1,3)-1)=substr(sk3.code,1,INSTR(sk.code,'.',1,3)-1) AND INSTR(sk3.code,'.',1,3)=0 
              AND sk.version_date=V_VERSION_DATE AND sk.id_data_source=V_ID_DATA_SOURCE) t 
            ON (sk.id=t.id AND sk.id_data_source=t.id_data_source AND sk.version_date=t.version_date AND sk.version_date=V_VERSION_DATE
            AND sk.id_data_source=V_ID_DATA_SOURCE)
            WHEN MATCHED THEN UPDATE SET
              sk.ID_PARENT_LEVEL3=t.parent_id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_STATUS [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_STATUS';
    rec_array(idx).sql_name := 'SP_STATUS [EAIST1]';
    rec_array(idx).description := 'Статусы БО контрактов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_STATUS(ID,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) select id,name,12,V_ID_DATA_SOURCE,V_VERSION_DATE from c_vocabulary@tkdbn1 where voc_type_id=51;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_BUDJET_LIABILITY [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_BUDJET_LIABILITY';
    rec_array(idx).sql_name := 'T_CONTRACT_BUDJET_LIABILITY [EAIST1]';
    rec_array(idx).description := 'Бюджетное обязательство';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_CONTRACT_BUDJET_LIABILITY(ID   
                                                                        ,CONTRACT_ID               
                                                                        ,FUNCTIONAL              
                                                                        ,PURPOSE                 
                                                                        ,EXPENSE                 
                                                                        ,ECONOMIC                
                                                                        ,SUMMA                   
                                                                        ,BUDGET_YEAR             
                                                                        ,DATE_BUDJECT_LIABILITY  
                                                                        ,NUMBER_BUDJECT_LIABILITY
                                                                        ,GRBS                    
                                                                        ,PAYMENT_SUMM            
                                                                        ,STATUS_ID               
                                                                        ,NUMBER_REPLACE          
                                                                        ,COMPANY_ID              
                                                                        ,IS_CURRENT                                                                     
                                                                        ,ID_DATA_SOURCE
                                                                        ,VERSION_DATE) 
            select    ID
                        ,CONTRACT_ID               
                        ,FUNCTIONAL              
                        ,PURPOSE                 
                        ,EXPENSE                 
                        ,ECONOMIC                
                        ,SUMMA                   
                        ,BUDGET_YEAR             
                        ,DATE_BUDJECT_LIABILITY  
                        ,NUMBER_BUDJECT_LIABILITY
                        ,GRBS                    
                        ,PAYMENT_SUMM            
                        ,STATUS_ID               
                        ,NUMBER_REPLACE          
                        ,COMPANY_ID              
                        ,IS_CURRENT
                        ,V_ID_DATA_SOURCE
                        ,V_VERSION_DATE
                        from CONTRACT_BUDJET_LIABILITY@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_COMMISSION_SESSION_TYPE [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_COMMISSION_SESSION_TYPE';
    rec_array(idx).sql_name := 'SP_COMMISSION_SESSION_TYPE [EAIST1]';
    rec_array(idx).description := 'Справочник типов заседаний комиссий';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into reports.SP_COMMISSION_SESSION_TYPE(id,name,id_data_source,VERSION_DATE) select id, NAME,
          V_ID_DATA_SOURCE, V_VERSION_DATE from vocabulary@tkdbn1 where id in (select distinct session_type_id from commission_session@tkdbn1);/*voc_type_id=7*/

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_COMMISSION_SESSION [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_COMMISSION_SESSION';
    rec_array(idx).sql_name := 'T_COMMISSION_SESSION [EAIST1]';
    rec_array(idx).description := 'Информация о выбранной комиссии и о ее заседании по рассмотрению заявок поставщиков на участие в закупках';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into reports.T_COMMISSION_SESSION(ID             
                                                                        ,SESSION_TYPE_ID
                                                                        ,MEET_DATE      
                                                                        ,LOT_ID         
                                                                        ,PROTOCOL_NUMBER
                                                                        ,ID_DATA_SOURCE 
                                                                        ,VERSION_DATE ) 
            select ID,SESSION_TYPE_ID,MEET_DATE,LOT_ID,PROTOCOL_NUMBER,
              V_ID_DATA_SOURCE, V_VERSION_DATE from COMMISSION_SESSION@tkdbn1;/*нулловых лотов нет, все есть в lot, id не уникальны*/

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_NOTIFICATION [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_NOTIFICATION';
    rec_array(idx).sql_name := 'T_NOTIFICATION [EAIST1]';
    rec_array(idx).description := 'Сведения об извещениях по контрактам с единственным поставщиком (номер извещения по таким контрактам такой же, как и номер лота)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into REPORTS.T_NOTIFICATION (LOT_REESTR_NUMBER,CONTRACT_ID,ID_DATA_SOURCE,VERSION_DATE)
            select LOT_REESTR_NUMBER,CONTRACT_ID,V_ID_DATA_SOURCE,V_VERSION_DATE from notification@tkdbn1 n
            join REPORTS.T_CONTRACT con on n.CONTRACT_ID=con.ID and con.ID_DATA_SOURCE=V_ID_DATA_SOURCE and VERSION_DATE=V_VERSION_DATE
            --where LOT_REESTR_NUMBER is not null
            ;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT - MAXIMUM_CONTRACT_COST [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT';
    rec_array(idx).sql_name := 'T_LOT - MAXIMUM_CONTRACT_COST [EAIST1]';
    rec_array(idx).description := 'MAXIMUM_CONTRACT_COST из TENDER_SS';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      merge into t_lot 
       using 
          (select ppz_lot_id id, summa from tender_ss@tkdbn1 t_ss 
          inner join
          (select id from    
          (select distinct l.id, replace(l.LOT_REGISTRY_NUMBER, '-')||l.lot_number reestr_number, l.LOT_REGISTRY_NUMBER, l.lot_number, l.maximum_contract_cost from
          (select * from t_lot where id_data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE and maximum_contract_cost is null) l) t1 
          inner join
          (select distinct replace(lot_reestr_number, '-') reestr_number from t_notification t1 where id_data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE) t2
          on t1.reestr_number=t2.reestr_number) tn on t_ss.ppz_lot_id=tn.id) src
        on (t_lot.id=src.id) 
      when matched then update set t_lot.maximum_contract_cost=src.summa;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_PPZ_LOT [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_PPZ_LOT';
    rec_array(idx).sql_name := 'T_PPZ_LOT [EAIST1]';
    rec_array(idx).description := 'Таблица лотов плана-графика';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_PPZ_LOT(  ID
                                --,TENDER_ID
                                ,PLAN_SHEDULE_ID
                                ,REGISTRY_NUMBER
                                ,SUBJECT
                                ,GENERAL_GOV_CUSTOMER_ENTITY_ID
                                ,PLAN_DATE
                                ,TENDER_FORM_ID
                                ,LOT_NUMBER
                                ,STATUS_ID
                                ,IS_SMP
                                ,GUARANTEE_SUMM
                                ,POSITION_NUMBER
                                ,IS_BUY_FOR_AMBULANCE
                                ,IS_REPUBLICATE
                                ,ID_DATA_SOURCE
                                ,VERSION_DATE) 
        select ID
              --,TENDER_ID
              ,PLAN_SHEDULE_ID
              ,REGISTRY_NUMBER
              ,SUBJECT
              ,GENERAL_GOV_CUSTOMER_ENTITY_ID
              ,PLAN_DATE
              ,TENDER_FORM_ID
              ,LOT_NUMBER
              ,STATUS_ID
              ,IS_SMP
              ,GUARANTEE_SUMM
              ,POSITION_NUMBER
              ,IS_BUY_FOR_AMBULANCE
              ,IS_REPUBLICATE
              ,V_ID_DATA_SOURCE
              ,V_VERSION_DATE
        from ppz_lot@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT - T_PPZ_LOT [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT';
    rec_array(idx).sql_name := 'T_LOT - T_PPZ_LOT [EAIST1]';
    rec_array(idx).description := 'in T_LOT';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_LOT(  ID
                           ,ID_PURCHASE_SCHEDULE
                           ,LOT_REGISTRY_NUMBER_PLAN
                           ,NAME
                           ,CUSTOMER_ID
                           ,PLAN_PUBLICATION_DATE
                           ,ID_METHODOFSUPPLIER
                           ,LOT_NUMBER
                           ,ID_STATUS
                           ,IS_SMALL
                           ,LOT_REGISTRY_NUMBER
                           ,MAXIMUM_CONTRACT_COST
                                ,ID_DATA_SOURCE
                                ,VERSION_DATE
                                ,IS_SINGLE_VENDOR) 
        select l.ID + 100000000
              ,l.PLAN_SHEDULE_ID
              ,l.REGISTRY_NUMBER
              ,l.SUBJECT
              ,l.GENERAL_GOV_CUSTOMER_ENTITY_ID
              ,l.PLAN_DATE
              ,l.TENDER_FORM_ID
              ,l.LOT_NUMBER
              ,l.STATUS_ID
              ,l.IS_SMP
              ,t.REESTR_NUMBER
              ,t.SUMMA
              ,V_ID_DATA_SOURCE
              ,V_VERSION_DATE
              ,1
        from ppz_lot@tkdbn1 l
        left join
          (select ppz_lot_id, reestr_number, max(summa) summa
           from tender_ss@tkdbn1 t 
           group by ppz_lot_id, reestr_number) t
        on l.id=t.ppz_lot_id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_TENDER_SS [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_TENDER_SS';
    rec_array(idx).sql_name := 'T_TENDER_SS [EAIST1]';
    rec_array(idx).description := '';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO T_TENDER_SS(
            PPZ_LOT_ID
            ,CONTRACT_ID
            ,REESTR_NUMBER
            ,SUBJECT
            ,SUMMA
            ,LOT_NUM
            ,VERSION_DATE
            ,ID_DATA_SOURCE)
        SELECT
            PPZ_LOT_ID
            ,CONTRACT_ID
            ,REESTR_NUMBER
            ,SUBJECT
            ,SUMMA
            ,LOT_NUM
            ,V_VERSION_DATE
            ,V_ID_DATA_SOURCE
        from tender_ss@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_LOT_SPECIFICATION - FROM PPZ_LOT_SPECIFICATION [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_LOT_SPECIFICATION';
    rec_array(idx).sql_name := 'T_LOT_SPECIFICATION - FROM PPZ_LOT_SPECIFICATION [EAIST1]';
    rec_array(idx).description := 'from PPZ_LOT_SPECIFICATION';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
      insert into T_LOT_SPECIFICATION( ID              
                                                        ,ID_LOT          
                                                        ,BYDGET_FINANSING
                                                        ,BUDGET_YEAR         
                                                        ,ID_OKDP         
                                                        ,ID_OKPD         
                                                        ,ID_MC_PRODUCT   
                                                        ,ID_FOLDER       
                                                        ,ID_EXPENSE      
                                                        ,ID_ECONOMIC     
                                                        ,ID_TARGET       
                                                        ,ID_GRBS         
                                                        ,UNIT_PRICE      
                                                        ,PRODUCTION_COUNT
                                                        ,ID_FIN_SOURCE     
                                                        ,ID_DATA_SOURCE
                                                        ,VERSION_DATE ) 
          select 
                    pls.ID+100000000 ID         
                    ,pls.LOT_ID+100000000 LOT_ID    
                    ,pls.SUMMA 
                    ,pls.YEAR     
                    ,pls.OKDP_ID         
                    ,pls.OKPD_ID         
                    ,pls.MC_PRODUCT_ID   
                    ,pls.EXPENSE_FORM_ID       
                    ,pls.FUNCTIONAL_EXPENSE_FORM_ID      
                    ,pls.ECONOMIC_GRADING_CODE_ID     
                    ,pls.PURPOSE_EXPENSE_CLAUSE_ID       
                    ,pls.GRBS_FORM_ID         
                    ,pls.UNIT_SUMMA      
                    ,pls.COUNT PRODUCTION_COUNT
                    ,pls.FINANCING_SOURCE_TYPE_ID
                    ,V_ID_DATA_SOURCE
                    ,V_VERSION_DATE
          from
            (select id, max(rn) rn from                  
                    (select           
                              ID         
                              ,rownum rn
                    from ppz_lot_specification@tkdbn1 ) 
            group by id) t
          inner join (select pls.*, rownum rn from ppz_lot_specification@tkdbn1 pls) pls
          on t.id=pls.id and t.rn=pls.rn;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_COMPLEX [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_COMPLEX';
    rec_array(idx).sql_name := 'SP_COMPLEX [PURCHASE_SMALL]';
    rec_array(idx).description := 'Справочник комплексов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into reports.SP_COMPLEX(id,complex_name,id_data_source,VERSION_DATE) select id, complex_name, V_ID_DATA_SOURCE, V_VERSION_DATE from complex@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_DEPARTMENT [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_DEPARTMENT';
    rec_array(idx).sql_name := 'SP_DEPARTMENT [PURCHASE_SMALL]';
    rec_array(idx).description := 'Справочник ведомств';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into reports.SP_DEPARTMENT(ID                    
                                                            ,GRBS                
                                                            ,ID_COMPLEX          
                                                            ,ID_ORGANIZATION          
                                                            ,ID_DATA_SOURCE      
                                                            ,VERSION_DATE) 
            select id,grbs,complex_id,enterprise_entity_id, V_ID_DATA_SOURCE, V_VERSION_DATE from department@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_ORGANIZATION [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION';
    rec_array(idx).sql_name := 'SP_ORGANIZATION [PURCHASE_SMALL]';
    rec_array(idx).description := 'Cправочник организаций';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into REPORTS.SP_ORGANIZATION(ID,ID_PARENT,INN,KPP,ORGANIZATION_TYPE,FULL_NAME,SHORT_NAME,ID_DATA_SOURCE,VERSION_DATE,IS_CUSTOMER,IS_SUPPLIER,ENTITY_ID,FORMATTED_NAME, address, ogrn, phone,email, CONNECT_LEVEL ) 
            select o.*,level from (
                            SELECT curr.ID,
                              curr.PARENT_ID,
                              curr.INN,
                              curr.KPP,
                              curr.COMPANY_TYPE,
                              curr.FULL_NAME,
                              curr.NAME,
                              V_ID_DATA_SOURCE,
                              V_VERSION_DATE,
                              case when company_type=3 then 1 else 0 end as IS_CUSTOMER,
                              case when company_type in (1,2) then 1 else 0 end as IS_SUPPLIER,
                              curr.entity_id,
                              FORMATE_NAME(FULL_NAME) as FORMATTED_NAME,
                              a.full_address,
                              curr.ogrn,
                              curr.phone,
                              curr.email
                             FROM enterprise@tkdbn1 curr,
                                  (  SELECT entity_id, MAX (t.date_start) max_date, COUNT (*) cnt
                                       FROM enterprise@tkdbn1 t
                                   GROUP BY entity_id) mv, address@tkdbn1 a
                              WHERE curr.entity_id = mv.entity_id AND curr.date_start = mv.max_date and curr.address_fact_id=a.id) o
          connect by prior o.entity_id=o.parent_id
          start with o.parent_id is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_ORGANIZATION - UPDATE [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION';
    rec_array(idx).sql_name := 'SP_ORGANIZATION - UPDATE [PURCHASE_SMALL]';
    rec_array(idx).description := 'Блок update';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          update sp_organization set id=entity_id where ID_DATA_SOURCE=V_ID_DATA_SOURCE and VERSION_DATE = V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER [PURCHASE_SMALL]';
    rec_array(idx).description := 'Справочник заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CUSTOMER (ID,ID_PARENT,FULL_NAME,SHORT_NAME,CONNECT_LEVEL,INN,KPP,IS_SMP,SPZ_CODE, ID_DATA_SOURCE,VERSION_DATE,S_KEY_SORT, FORMATTED_NAME )
            select ID,ID_PARENT,FULL_NAME,SHORT_NAME,CONNECT_LEVEL,INN,KPP,IS_SMP,SPZ_CODE, ID_DATA_SOURCE,VERSION_DATE,S_KEY_SORT,
            FORMATE_NAME(FULL_NAME) from
              (  select cust.*,rownum s_key_sort from 
                 (
                    select 0 ID,null ID_PARENT,'Москва' FULL_NAME,'Москва' SHORT_NAME,1 CONNECT_LEVEL,null INN,null KPP,null IS_SMP,null SPZ_CODE,V_ID_DATA_SOURCE ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE, '0' key_sort from dual
                     union all
                    select id,0,regexp_replace(complex_name,'^[^[:alpha:]]{1,}',''),regexp_replace(complex_name,'^[^[:alpha:]]{1,}',''),2,null,null,null,null,V_ID_DATA_SOURCE ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE,to_char(id) key_sort
                        from sp_complex where ID_DATA_SOURCE=V_ID_DATA_SOURCE and VERSION_DATE=V_VERSION_DATE
                     union all
                    select v.id,v.id_complex,v.grbs,v.grbs,3,org.inn,org.kpp,null,null,V_ID_DATA_SOURCE ID_DATA_SOURCE,V_VERSION_DATE,v.grbs key_sort
                         from (select distinct id,grbs,id_complex from sp_department where ID_DATA_SOURCE=V_ID_DATA_SOURCE and VERSION_DATE=V_VERSION_DATE) v
                         join sp_organization org on v.id=org.id and org.id_data_source=V_ID_DATA_SOURCE and ORG.VERSION_DATE=V_VERSION_DATE
                     union all
                    select uchr.id,uchr.vedom_id,uchr.FULL_NAME,uchr.SHORT_NAME,4 conn_level,uchr.INN,uchr.KPP,uchr.IS_SMP,uchr.SPZ_CODE,V_ID_DATA_SOURCE ID_DATA_SOURCE,V_VERSION_DATE VERSION_DATE,uchr.FULL_NAME key_sort
                         from (select o.id,d.id vedom_id,d.id_complex,o.FULL_NAME,o.SHORT_NAME,o.INN,o.KPP,o.IS_SMP,o.SPZ_CODE,o.ID_DATA_SOURCE,o.VERSION_DATE from (select * from sp_department where id!=id_organization) d
                         join sp_organization o on d.id_organization=o.id and d.version_date=V_VERSION_DATE and O.VERSION_DATE=V_VERSION_DATE and D.ID_DATA_SOURCE=V_ID_DATA_SOURCE and o.ID_DATA_SOURCE=V_ID_DATA_SOURCE) uchr
                ) cust
                connect by prior cust.id=cust.id_parent
                start with cust.id=0
                order siblings by key_sort  );

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER - GRBS [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER - GRBS [PURCHASE_SMALL]';
    rec_array(idx).description := 'Простановка кодов ГРБС';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    update sp_customer c set c.grbs_code=(select grbs_code from lnk_grbs_code_customer where id_customer=c.id and eaist=1) where id_data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CUSTOMER_RATING [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER_RATING';
    rec_array(idx).sql_name := 'SP_CUSTOMER_RATING [PURCHASE_SMALL]';
    rec_array(idx).description := 'Справочник рейтингов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        INSERT INTO REPORTS.SP_CUSTOMER_RATING
                      ( ID, CUSTOMER_ID,PERIOD,PERIOD_TYPE,PERIOD_TEXT, COUNT_OF_CONTRACT_DETECTED,COUNT_OF_CUSTOMER_RESPONS, DETECTED_GLAV_CONTROL, YEAR, ID_DATA_SOURCE,VERSION_DATE)
                    select ID, CUSTOMER_ID,
                            case 
                                when period like 'Год' then 'y'
                                when period like '1-е полугодие' then 'h'
                             end PERIOD,
                             case 
                                when period like 'Год' then 0
                                when period like '1-е полугодие' then 1
                             end PERIOD_TYPE,
                             PERIOD PERIOD_TEXT, 
                             COUNT_OF_CONTRACT_DETECTED,COUNT_OF_CUSTOMER_RESPONS, DETECTED_GLAV_CONTROL, YEAR, V_ID_DATA_SOURCE, V_VERSION_DATE
                      from 
                      N_CUSTOMER_RATING@EAIST_MOS_NSI
                      where DELETED_DATE is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_CONNECTION_ORGANIZATION [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CONNECTION_ORGANIZATION';
    rec_array(idx).sql_name := 'LNK_CONNECTION_ORGANIZATION [PURCHASE_SMALL]';
    rec_array(idx).description := 'Рассчетные связи родителей организаций с детьми всех уровней';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        for j in (
          select * from sp_customer where ID_DATA_SOURCE = V_ID_DATA_SOURCE and version_date = V_VERSION_DATE
        ) 
        loop
          insert into LNK_CONNECTION_ORGANIZATION (id,id_child,ID_DATA_SOURCE,parent_level,child_LEVEL,VERSION_DATE)
            SELECT  j.id, id as child_id, ID_DATA_SOURCE,j.connect_level parent_level,level as child_LEVEL, VERSION_DATE
            FROM   sp_customer o    
            START WITH    id = j.id and ID_DATA_SOURCE=j.ID_DATA_SOURCE and version_date=j.version_date
            CONNECT BY     id_parent=PRIOR id and ID_DATA_SOURCE=prior ID_DATA_SOURCE and version_date=prior version_date;
        end loop;
        delete from  LNK_CONNECTION_ORGANIZATION where id=id_child;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KPGZ [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KPGZ';
    rec_array(idx).sql_name := 'SP_KPGZ [PURCHASE_SMALL]';
    rec_array(idx).description := 'Классификатор предметов государственного заказа (КПГЗ)';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_KPGZ (ID,ID_PARENT,CODE,NAME,ID_DATA_SOURCE,VERSION_DATE) 
            select ID,PARENT_ID,CODE,NAME,V_ID_DATA_SOURCE,V_VERSION_DATE from MC_RUBRICATOR_EXT@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KPGZ - 2,3 LVL [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KPGZ';
    rec_array(idx).sql_name := 'SP_KPGZ - 2,3 LVL [PURCHASE_SMALL]';
    rec_array(idx).description := 'Простановка КПГЗ 2 и 3 уровня';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        MERGE INTO SP_KPGZ sk
        USING (
          SELECT sk.id, sk.id_data_source, sk.version_date, sk2.id AS parent_id
          FROM sp_KPGZ sk
          INNER JOIN sp_KPGZ sk2 ON sk.id_data_source=sk2.id_data_source AND sk.version_date=sk2.version_date
          AND substr(sk.code,1,INSTR(sk.code,'.',1,2)-1)=substr(sk2.code,1,INSTR(sk.code,'.',1,2)-1) AND INSTR(sk2.code,'.',1,2)=0 
          AND sk.version_date=V_VERSION_DATE AND sk.id_data_source=V_ID_DATA_SOURCE) t
        ON (sk.id=t.id AND sk.id_data_source=t.id_data_source AND sk.version_date=t.version_date AND sk.version_date=V_VERSION_DATE
          AND sk.id_data_source=V_ID_DATA_SOURCE)
        WHEN MATCHED THEN UPDATE SET
          sk.ID_PARENT_LEVEL2=t.parent_id;
                  
        MERGE INTO SP_KPGZ sk
        USING (
          SELECT sk.id, sk.id_data_source, sk.version_date, sk3.id AS parent_id
          FROM sp_KPGZ sk
          INNER JOIN sp_KPGZ sk3 ON sk.id_data_source=sk3.id_data_source AND sk.version_date=sk3.version_date
          AND substr(sk.code,1,INSTR(sk.code,'.',1,3)-1)=substr(sk3.code,1,INSTR(sk.code,'.',1,3)-1) AND INSTR(sk3.code,'.',1,3)=0 
          AND sk.version_date=V_VERSION_DATE AND sk.id_data_source=V_ID_DATA_SOURCE) t 
        ON (sk.id=t.id AND sk.id_data_source=t.id_data_source AND sk.version_date=t.version_date AND sk.version_date=V_VERSION_DATE
          AND sk.id_data_source=V_ID_DATA_SOURCE)
        WHEN MATCHED THEN UPDATE SET
          sk.ID_PARENT_LEVEL3=t.parent_id;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SOURCE_FINANCE [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SOURCE_FINANCE';
    rec_array(idx).sql_name := 'SP_SOURCE_FINANCE [PURCHASE_SMALL]';
    rec_array(idx).description := 'Источник финансирования';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_SOURCE_FINANCE(ID,CODE,name,ID_DATA_SOURCE,VERSION_DATE) 
            select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from FINANCING_SOURCE_TYPE@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SECTION_BUDGET [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SECTION_BUDGET';
    rec_array(idx).sql_name := 'SP_SECTION_BUDGET [PURCHASE_SMALL]';
    rec_array(idx).description := 'Раздел/подраздел бюджета';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_SECTION_BUDGET(ID,CODE,name,ID_DATA_SOURCE,VERSION_DATE) 
            select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from EXPENSE_FORM@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_TYPE_EXPENSE [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_TYPE_EXPENSE';
    rec_array(idx).sql_name := 'SP_TYPE_EXPENSE [PURCHASE_SMALL]';
    rec_array(idx).description := 'Вид расходов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_TYPE_EXPENSE(ID,CODE,name,ID_DATA_SOURCE,VERSION_DATE) 
            select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from FUNCTIONAL_EXPENSE_FORM@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KOSGU [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KOSGU';
    rec_array(idx).sql_name := 'SP_KOSGU [PURCHASE_SMALL]';
    rec_array(idx).description := 'КОСГУ';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_KOSGU(ID,CODE,NAME,ID_DATA_SOURCE,VERSION_DATE) 
            select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from ECONOMIC_GRADING_CODE@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_TARGET_CLAUSE [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_TARGET_CLAUSE';
    rec_array(idx).sql_name := 'SP_TARGET_CLAUSE [PURCHASE_SMALL]';
    rec_array(idx).description := 'Целевая статья';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_TARGET_CLAUSE(ID,CODE,name,ID_DATA_SOURCE,VERSION_DATE) 
            select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from PURPOSE_EXPENSE_CLAUSE@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_GRBS [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_GRBS';
    rec_array(idx).sql_name := 'SP_GRBS [PURCHASE_SMALL]';
    rec_array(idx).description := 'ГРБС';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_GRBS(ID,CODE,DESCRIPTION,ID_DATA_SOURCE,VERSION_DATE) 
            select id,code,full_name,V_ID_DATA_SOURCE,V_VERSION_DATE from GRBS_FORM@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_TERM_REASON_TYPE [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_TERM_REASON_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_TERM_REASON_TYPE [PURCHASE_SMALL]';
    rec_array(idx).description := 'Причины расторжения контракта';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CONTRACT_TERM_REASON_TYPE (id,name,id_data_source,version_date)
            select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from es_vocabulary@tkdbn1 where id in (select distinct termination_reason_id from es_execution@tkdbn1);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_TERMINATION_TYPE [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_TERMINATION_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_TERMINATION_TYPE [PURCHASE_SMALL]';
    rec_array(idx).description := 'Типы причин расторжения';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CONTRACT_TERMINATION_TYPE (id,name,id_data_source,version_date)
            select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from es_vocabulary@tkdbn1 where id in (select termination_id from es_execution@tkdbn1);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_STATUS [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_STATUS';
    rec_array(idx).sql_name := 'SP_STATUS [PURCHASE_SMALL]';
    rec_array(idx).description := 'Статусы';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_STATUS(ID,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) 
            select id,name,10,V_ID_DATA_SOURCE,V_VERSION_DATE from es_vocabulary@tkdbn1 where id in (select type_id from es_execution@tkdbn1)-- расторгнут/исполнен этап контракта
            union all
            select id,name,10,V_ID_DATA_SOURCE,V_VERSION_DATE from es_vocabulary@tkdbn1 where id in (select status_id from es_execution@tkdbn1);-- статус исполнения этапа контракта
        

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_AGR_CHANGE_TYPE [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_AGR_CHANGE_TYPE';
    rec_array(idx).sql_name := 'SP_CONTRACT_AGR_CHANGE_TYPE [PURCHASE_SMALL]';
    rec_array(idx).description := 'Обоснование изменения цены контракта';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CONTRACT_AGR_CHANGE_TYPE(ID,NAME,ID_DATA_SOURCE,VERSION_DATE) 
            select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from es_vocabulary@tkdbn1 where id in (select PRICE_CHANGE_REASON_ID from es_agreement@tkdbn1);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_AGREEMENT [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_AGREEMENT';
    rec_array(idx).sql_name := 'T_CONTRACT_AGREEMENT [PURCHASE_SMALL]';
    rec_array(idx).description := 'Дополнительное соглашение';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_CONTRACT_AGREEMENT(ID                      
                                                                ,CONTRACT_ID             
                                                                ,SUMM                    
                                                                ,PREDSUMM                
                                                                ,CONCLUSION_DATE         
                                                                ,ACTION_BEGIN            
                                                                ,ACTION_END              
                                                                ,SIGN_NUMBER             
                                                                ,PRICE_CHANGE_REASON_ID  
                                                                ,NOTICE_DATE             
                                                                ,IS_ELECTRONIC_CONCLUSION
                                                                ,ID_DATA_SOURCE          
                                                                ,VERSION_DATE ) 
        select ID                      
                 ,CONTRACT_ID             
                 ,SUMM                    
                 ,PREDSUMM                
                 ,CONCLUSION_DATE         
                 ,ACTION_BEGIN            
                 ,ACTION_END              
                 ,SIGN_NUMBER             
                 ,PRICE_CHANGE_REASON_ID  
                 ,NOTICE_DATE             
                 ,IS_ELECTRONIC_CONCLUSION,
                 V_ID_DATA_SOURCE,
                 V_VERSION_DATE
                 from ES_AGREEMENT@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_STATUS_2 [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_STATUS';
    rec_array(idx).sql_name := 'SP_STATUS_2 [PURCHASE_SMALL]';
    rec_array(idx).description := 'Статусы';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_STATUS(ID,NAME,id_category,ID_DATA_SOURCE,VERSION_DATE) 
            select id,name,1,V_ID_DATA_SOURCE,V_VERSION_DATE from ES_WF_STATE@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_SINGLE_VENDOR_PURCHASE [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_SINGLE_VENDOR_PURCHASE';
    rec_array(idx).sql_name := 'SP_SINGLE_VENDOR_PURCHASE [PURCHASE_SMALL]';
    rec_array(idx).description := 'Основание закупки у единственного поставщика';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_SINGLE_VENDOR_PURCHASE(ID,NAME,ID_DATA_SOURCE,VERSION_DATE) 
            select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from es_vocabulary@tkdbn1 where id in (select distinct singlesource_id from es_contract@tkdbn1);

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT';
    rec_array(idx).sql_name := 'T_CONTRACT [PURCHASE_SMALL]';
    rec_array(idx).description := 'Контракт';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
          insert into T_CONTRACT(ID      
                                            ,ID_STATUS                 
                                            ,ID_CUSTOMER             
                                            ,ID_SUPPLIER        
                                            ,CONTRACT_NUMBER
                                            ,registry_number_oos
                                            ,CONTRACT_DATE   
                                            ,COST_IN_RUBLE
                                            ,DURATION_START_DATE -- дата начала действия контракта
                                            ,PLAN_DATE_START --Плановая дата начала исполнения контракта 
                                            ,DURATION_END_DATE -- дата окончания действия контракта
                                            ,START_DATE --Дата начала исполнения (фактичекая) - в еаист1 совпадает с плановой датой начала исполнения контракта                                            
                                            ,END_DATE  -- в execution - Дата окончания исполнения(фактическая)
                                            ,ID_SINGLE_VENDOR_PURCHASE  
                                            ,ID_REASONTYPE 
                                            ,ID_TERMINATION_TYPE           
                                            ,PLAN_DATE      -- Плановая дата окончания исполнения (максимальная дата последнего этапа контракта)
                                            ,REG_DATE       
                                            ,SIGN_SUMM      
                                            ,EP_SUMM        
                                            ,EXECUTION_BEGIN  
                                            ,sign_number    
                                           -- ,PAYMENT_SUM                                 
                                            ,ID_DATA_SOURCE
                                            ,VERSION_DATE     ) 
        select ID                   
                ,WF_STATE_ID        
                ,CUSTOMER_ENTITY_ID 
                ,SUPPLIER_ENTITY_ID
                ,SIGN_NUMBER
                ,REESTR_NUMBER_807PP            
                ,SIGN_DATE          
                ,SUMM   
                ,ACTION_BEGIN   
                ,EXECUTION_BEGIN
                ,ACTION_END
                ,EXECUTION_BEGIN                
                ,fed.fact_end
                ,SINGLESOURCE_ID
                ,tr.termination_reason_id
                ,tr.termination_id 
                ,ped.plan_end
                ,REG_DATE       
                ,SIGN_SUMM      
                ,EP_SUMM        
                ,EXECUTION_BEGIN
                ,sign_number 
                --  ,ps.payment_summ - запилить таск
                ,V_ID_DATA_SOURCE
                ,V_VERSION_DATE
        from es_contract@tkdbn1 c
        left join (select max(fact_end_date) fact_end, contract_id from es_execution@tkdbn1 group by contract_id) fed on c.id=fed.contract_id
        left join (select max(plan_end_date) plan_end, contract_id from es_execution@tkdbn1 group by contract_id) ped on c.id=ped.contract_id
        left join (select  termination_reason_id,termination_id,contract_id from es_execution@tkdbn1 where termination_reason_id is not null and type_id=71 and status_id=1203) tr on c.id=tr.contract_id
        -- left join (select nvl(sum(payment_summ),0) payment_summ, contract_id from es_CONTRACT_BUDJET_LIABILITY@tkdbn1 where status_id = 20001 group by contract_id) ps on c.id=ps.contract_id
        ;   

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_FINANSING_CONTRACTS [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_FINANSING_CONTRACTS';
    rec_array(idx).sql_name := 'T_FINANSING_CONTRACTS [PURCHASE_SMALL]';
    rec_array(idx).description := 'Финансирование контракта. Соответствует таблице FINANSING в ЕИАСТ1';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_FINANSING_CONTRACTS(ID                     
                                                                ,ID_CONTRACT          
                                                                ,ID_FINANCING_SOURCE  
                                                                ,ID_PURPOSE           
                                                                ,ID_GRBS              
                                                                ,ID_EXPENSE        
                                                                ,ID_FOLDER           
                                                                ,ID_ECONOMIC          
                                                                ,SUMMA_IN_CURRENCY_NDS
                                                                ,BUDGET_YEAR          
                                                                ,ECONOMIC_SUMM        
                                                                ,ID_DATA_SOURCE
                                                                ,VERSION_DATE
                                                                ,PURPOSE_CODE
                                                                ,GRBS_CODE
                                                                ,EXPENSE_CODE
                                                                ,FOLDER_CODE
                                                                ,ECONOMIC_CODE) 
            select CF.ID                     
                    ,CF.CONTRACT_ID          
                    ,CF.FINANCING_SOURCE_ID  
                    ,CF.PURPOSE_ID --sp_target_clause           
                    ,CF.GRBS_ID --SP_GRBS             
                    ,CF.FUNCTIONAL_ID  --SP_TYPE_EXPENSE      
                    ,CF.EXPENSE_ID   --SP_SECTION_BUDGET        
                    ,CF.ECONOMIC_ID --SP_KOSGU         
                    ,CF.SUMMA_IN_CURRENCY_NDS
                    ,CF.BUDGET_YEAR          
                    ,CF.ECONOMIC_SUMM
                    ,V_ID_DATA_SOURCE 
                    ,V_VERSION_DATE
                    ,cl.CODE PURPOSE_CODE
                    ,GRBS.CODE GRBS_CODE
                    ,sec.CODE EXPENSE_CODE
                    ,exp.CODE FOLDER_CODE
                    ,kos.CODE ECONOMIC_CODE
            from ES_FINANSING@tkdbn1 CF
            LEFT JOIN REPORTS.SP_TARGET_CLAUSE cl
                ON CF.PURPOSE_ID = cl.ID and cl.ID_DATA_SOURCE = V_ID_DATA_SOURCE   AND cl.VERSION_DATE = V_VERSION_DATE
             LEFT JOIN REPORTS.SP_GRBS GRBS
                ON CF.GRBS_ID = GRBS.ID AND GRBS.ID_DATA_SOURCE = V_ID_DATA_SOURCE  AND GRBS.VERSION_DATE = V_VERSION_DATE
             LEFT JOIN REPORTS.SP_TYPE_EXPENSE EXP
                ON CF.FUNCTIONAL_ID = EXP.ID AND EXP.ID_DATA_SOURCE = V_ID_DATA_SOURCE  AND EXP.VERSION_DATE = V_VERSION_DATE
             LEFT JOIN REPORTS.SP_SECTION_BUDGET sec
                ON CF.EXPENSE_ID = sec.ID AND sec.ID_DATA_SOURCE = V_ID_DATA_SOURCE  AND sec.VERSION_DATE = V_VERSION_DATE
             LEFT JOIN REPORTS.SP_KOSGU kos 
                ON CF.ECONOMIC_ID = kos.ID AND kos.ID_DATA_SOURCE = V_ID_DATA_SOURCE  AND kos.VERSION_DATE = V_VERSION_DATE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_CONTRACT_DOCUMENT_CATEGORY [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CONTRACT_DOCUMENT_CATEGORY';
    rec_array(idx).sql_name := 'SP_CONTRACT_DOCUMENT_CATEGORY [PURCHASE_SMALL]';
    rec_array(idx).description := 'Cправочник категорий документов';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into SP_CONTRACT_DOCUMENT_CATEGORY(ID,
                                                  NAME,
                                                  ID_DATA_SOURCE,
                                                  VERSION_DATE) 
            select id,name,V_ID_DATA_SOURCE,V_VERSION_DATE from es_vocabulary@tkdbn1 where ID in (select BASE_CLAIM_ID from ES_CLAIM_WORK@tkdbn1); --VOC_TYPE_ID in (22,23);
        

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- T_CONTRACT_CLAIM [PURCHASE_SMALL]
    idx := idx + 1;
    rec_array(idx).table_name := 'T_CONTRACT_CLAIM';
    rec_array(idx).sql_name := 'T_CONTRACT_CLAIM [PURCHASE_SMALL]';
    rec_array(idx).description := 'Штрафы по контрактам';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 4;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        insert into T_CONTRACT_CLAIM(ID
                                                        ,END_DATE
                                                        ,PAID_COST
                                                        ,ID_CONTRACT
                                                        ,ID_REASONTYPE
                                                        ,PENALTY_DATE
                                                        ,SIGN_DATE
                                                        ,ID_SUPPLIER
                                                        ,ID_RESULT_CLAIM
                                                        ,ID_DATA_SOURCE
                                                        ,VERSION_DATE) 
            select ID
            ,FINISH_DATE
            ,PENALTY_SUMMA
            ,CONTRACT_ID
            ,BASE_CLAIM_ID
            ,PENALTY_DATE
            ,SIGN_DATE
            ,SUPPLIER_ID
            ,RESULT_CLAIM_ID
            ,V_ID_DATA_SOURCE
            ,V_VERSION_DATE
            from ES_CLAIM_WORK@tkdbn1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_METHOD [LOAD_LNK_TABLES]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_METHOD';
    rec_array(idx).sql_name := 'LNK_METHOD [LOAD_LNK_TABLES]';
    rec_array(idx).description := 'Соответствия между элементами SP_METHOD из е1 и е2';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into LNK_METHOD (ID, EAIST1_ID, EAIST2_ID, VERSION_DATE) 
       SELECT ID, EAIST1_ID, EAIST2_ID, V_VERSION_DATE from LNK_METHOD where version_date=V_VERSION_DATE-1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_STATUS_CATEGORY [LOAD_LNK_TABLES]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_STATUS_CATEGORY';
    rec_array(idx).sql_name := 'LNK_STATUS_CATEGORY [LOAD_LNK_TABLES]';
    rec_array(idx).description := 'Соответствия между элементами SP_STATUS_CATEGORY различных источников данных';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into LNK_STATUS_CATEGORY (ID, EAIST1_ID, EAIST2_ID, NAME, VERSION_DATE) 
       SELECT ID, EAIST1_ID, EAIST2_ID, NAME, V_VERSION_DATE from LNK_STATUS_CATEGORY where version_date=V_VERSION_DATE-1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_STATUS [LOAD_LNK_TABLES]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_STATUS';
    rec_array(idx).sql_name := 'LNK_STATUS [LOAD_LNK_TABLES]';
    rec_array(idx).description := 'Соответствия между элементами SP_STATUS';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into LNK_STATUS (ID, EAIST1_ID, EAIST2_ID, PP_ID, STATUS_CATEGORY_ID, NAME, VERSION_DATE) 
       SELECT ID, EAIST1_ID, EAIST2_ID, PP_ID, STATUS_CATEGORY_ID, NAME, V_VERSION_DATE from LNK_STATUS where version_date=V_VERSION_DATE-1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- LNK_SOURCE_FINANCE [LOAD_LNK_TABLES]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_SOURCE_FINANCE';
    rec_array(idx).sql_name := 'LNK_SOURCE_FINANCE [LOAD_LNK_TABLES]';
    rec_array(idx).description := 'Соответствия элеметов SP_SOURCE_FINANCE между источниками данных ';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       insert into LNK_SOURCE_FINANCE (ID, EAIST1_ID, EAIST2_ID, EAIST4_ID, VERSION_DATE) 
       SELECT ID, EAIST1_ID, EAIST2_ID, EAIST4_ID, V_VERSION_DATE from LNK_SOURCE_FINANCE where version_date=V_VERSION_DATE-1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

    -- SP_KPGZ_MERGE [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_KPGZ';
    rec_array(idx).sql_name := 'SP_KPGZ_MERGE [EAIST2]';
    rec_array(idx).description := 'Обновление для задачи ускорения загрузки view SP_KPGZ_TREE_V';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    merge into sp_kpgz dst
    using (
            select
                  key_sort,
                  kpgz_level,
                  rownum rn,
                  id,
                  id_data_source ID_SOURCE,
                  id_parent,
                  id_data_source ID_PARENT_SOURCE, 
                  level id_level,
                  name as formatted_name,
                  code
                from (
                        select
                              key_sort,
                              kpgz_level,
                              id,
                              id_data_source,
                              case when id_parent is not null then id_parent else 0 end id_parent,
                              '('||code||') ' || name name,
                              code
                            from sp_kpgz
                            where trunc(version_date, 'dd') = V_VERSION_DATE and id_data_source = V_ID_DATA_SOURCE
                        union
                        select 1, 1, 0, 2, null, 'ВСЕ КПГЗ', '0' from dual
                     )
                connect by nocycle prior id = id_parent start with id = 0
          ) src
          on (dst.id = src.id and trunc(dst.version_date, 'dd') = V_VERSION_DATE and dst.id_data_source = V_ID_DATA_SOURCE)
          when matched then update set dst.key_sort = src.rn, dst.kpgz_level = src.id_level;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

-- MV_LOT_BIDS [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'MV_LOT_BIDS';
    rec_array(idx).sql_name := 'MV_LOT_BIDS [EAIST2]';
    rec_array(idx).description := 'Подсчет заявок для лотов, для сервиса';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    DBMS_MVIEW.REFRESH('mv_lot_bids');

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

-- SP_TRADING_PLATFORM [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_TRADING_PLATFORM';
    rec_array(idx).sql_name := 'SP_TRADING_PLATFORM [EAIST2]';
    rec_array(idx).description := 'Торговые площадки';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    INSERT INTO SP_TRADING_PLATFORM (
		NUM,
		NAME,
		OPERATOR,
		ADDRESS,
		TYPE,
		ACTIVE,
		VERSION_DATE,
		ID_DATA_SOURCE,
		ID)
	select 
	'' as Номер,
	t.name as Наименование,
	'' as Оператор,
	t.trade_place as Адрес,
	'' as Принадлежность,
	'Да' as Действующая,
	V_VERSION_DATE,
	V_ID_DATA_SOURCE,
	id
	from N_TRADING_PLATFORM@eaist_mos_nsi t
	where deleted_date is null and type <> 1;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

-- SP_TRADING_PLATFORM [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_TRADING_PLATFORM';
    rec_array(idx).sql_name := 'SP_TRADING_PLATFORM [EAIST1]';
    rec_array(idx).description := 'Торговые площадки';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    INSERT INTO SP_TRADING_PLATFORM (
		NUM,
		NAME,
		OPERATOR,
		ADDRESS,
		TYPE,
		ACTIVE,
		VERSION_DATE,
		ID_DATA_SOURCE,
		ID)
	select 
	NUM,
	NAME,
	OPERATOR,
	ADDRESS,
	TYPE,
	ACTIVE,
	V_VERSION_DATE,
	V_ID_DATA_SOURCE,
	id
	from SP_TRADING_PLATFORM where version_date=(SELECT MAX(VERSION_DATE) FROM SP_TRADING_PLATFORM WHERE ID_DATA_SOURCE=V_ID_DATA_SOURCE) and ID_DATA_SOURCE=V_ID_DATA_SOURCE;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

-- SP_OKOPF [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_OKOPF';
    rec_array(idx).sql_name := 'SP_OKOPF [EAIST2]';
    rec_array(idx).description := 'SP_OKOPF';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    insert into SP_OKPF (id, code, name, id_data_source, version_date)
	select id, code, name, v_id_Data_source, V_version_date from n_okopf@eaist_mos_nsi where deleted_date is null;

    -- Привязка кол-ва обработанных строк
    :V_ROWCOUNT := SQL%ROWCOUNT;

END;#';

-- SP_CUSTOMER [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER - UPD [LOAD_ORG_JOINT_1156]';
    rec_array(idx).description := 'Связка заказчиков е1 с е2 руками через LNK_CUSTOMERS_E1_E2_IMP';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
		for c in (select * from lnk_customers_e1_e2_imp) 
				loop
					update sp_customer set inn=c.e2_inn, kpp=c.e2_kpp where inn=c.e1_inn and kpp=c.e1_kpp and id_data_source=V_ID_DATA_SOURCE and version_date=V_VERSION_DATE;  --42 cust 43 rows
				end loop;   

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- SP_CUSTOMER [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER-1 [CLEAN_1156]';
    rec_array(idx).description := 'Чистка справочника заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
	--6 Из справочника удаляются все элементы
	--которые не являются узлами (не содержат лежащих внутри них элементов) и по которым отсутствуют данные в таблицах: ОЗ, ДОЗ, лот, тендер, контракт,лимиты;    

	   delete from sp_customer c where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE
		and not exists(select * from sp_customer o where o.version_date=c.version_date and o.id_data_source=c.id_data_source and o.id_parent=c.id)
		and not exists(select * from T_CONTRACT con where con.version_date=c.version_date and con.id_data_source=c.id_data_source and con.ID_CUSTOMER=c.id)
		and not exists(select * from T_FINANCIAL_LIMIT fl where fl.version_date=c.version_date and fl.id_data_source=c.id_data_source and c.id=fl.CUSTOMER_ID)
		and not exists(select * from T_GKU_PURCHASE_PLAN gpp where gpp.version_date=c.version_date and gpp.id_data_source=c.id_data_source and c.id=gpp.CUSTOMER_ID)
		and not exists(select * from T_GKU_PURCHASE_SCHEDULE gps where gps.version_date=c.version_date and gps.id_data_source=c.id_data_source and c.id=gps.CUSTOMER_ID)
		and not exists(select * from T_LOT l where l.version_date=c.version_date and l.id_data_source=c.id_data_source and c.id=l.CUSTOMER_ID)
		and not exists(select * from T_LOT_SPECIFICATION ls where ls.version_date=c.version_date and ls.id_data_source=c.id_data_source and c.id=ls.ID_CUSTOMER)
		and not exists(select * from T_PURCHASE p where p.version_date=c.version_date and p.id_data_source=c.id_data_source and c.id=p.CUSTOMER_ID)
		and not exists(select * from T_PURCHASE_DETAILED pd where pd.version_date=c.version_date and pd.id_data_source=c.id_data_source and c.id=pd.ID_CUSTOMER)
		and not exists(select * from T_PURCHASE_LIMIT pl where pl.version_date=c.version_date and pl.id_data_source=c.id_data_source and c.id=pl.CUSTOMER_ID)
		and not exists(select * from T_PURCHASE_PLAN pp where pp.version_date=c.version_date and pp.id_data_source=c.id_data_source and c.id=pp.CUSTOMER_ID)
		and not exists(select * from T_PURCHASE_SHEDULE ps where ps.version_date=c.version_date and ps.id_data_source=c.id_data_source and c.id=ps.CUSTOMER_ID)
		and not exists(select * from T_TENDER t where t.version_date=c.version_date and t.id_data_source=c.id_data_source and c.id=t.ID_CUSTOMER_EAIST2); 

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- SP_CUSTOMER [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER-2 [CLEAN_1156]';
    rec_array(idx).description := 'Чистка справочника заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
	--6 Из справочника удаляются все элементы
	--которые не являются узлами (не содержат лежащих внутри них элементов) и по которым отсутствуют данные в таблицах: ОЗ, ДОЗ, лот, тендер, контракт,лимиты;   
	 
		delete from sp_customer c where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE
			and not exists(select * from sp_customer o where o.version_date=c.version_date and o.id_data_source=c.id_data_source and o.id_parent=c.id)--нет подведов
			and not exists(select * from T_CONTRACT con where con.version_date=c.version_date and con.id_data_source=c.id_data_source and c.id=con.ID_CUSTOMER)
			and not exists(select * from T_LOT l where l.version_date=c.version_date and l.id_data_source=c.id_data_source and c.id=l.CUSTOMER_ID)
			and not exists(select * from T_LOT_SPECIFICATION ls where ls.version_date=c.version_date and ls.id_data_source=c.id_data_source and c.id=ls.ID_CUSTOMER)
			and not exists(select * from T_PPZ_LOT ppz where ppz.version_date=c.version_date and ppz.id_data_source=c.id_data_source and c.id=ppz.GENERAL_GOV_CUSTOMER_ENTITY_ID)
			and not exists(select * from T_TENDER t where t.version_date=c.version_date and t.id_data_source=c.id_data_source and c.id=t.ID_CUSTOMER_EAIST2);

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- SP_CUSTOMER [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER-3 [CLEAN_1156]';
    rec_array(idx).description := 'Чистка справочника заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
       --у которых установлен статус "В архиве" (sp_customer.sp_status=4) и по которым отсутствуют ГК с финансированием на 2014 год и далее (>=2014)

        delete from sp_customer c where version_date=V_VERSION_DATE
        and status=4 
        and not exists(select * from sp_customer o where o.version_date=c.version_date and o.id_data_source=c.id_data_source and o.id_parent=c.id)
        and not exists(select * from T_CONTRACT con
                       join t_finansing_contracts fc on fc.version_date=CON.VERSION_DATE and con.id_data_source=fc.id_data_source and con.id=fc.id_contract
                       where con.version_date=c.version_date and con.id_data_source=c.id_data_source and c.id=ID_CUSTOMER and fc.budget_year>=2014);

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- SP_ORGANIZATION_JOINT
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_ORGANIZATION_JOINT';
    rec_array(idx).sql_name := 'SP_ORGANIZATION_JOINT [RELOAD_1156]';
    rec_array(idx).description := 'Перезагрузка сводного справочника заказчиков и поставщиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
	rec_array(idx).id_data_source_aux := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
	delete from SP_ORGANIZATION_JOINT where V_VERSION_DATE = VERSION_DATE;
    commit;

         INSERT INTO SP_ORGANIZATION_JOINT
            select 
                o1.id ID_EAIST1,
                O1.ID_PARENT id_parent_eaist1,
                o1.formatted_name formatted_name_eaist1,
                O1.CONNECT_LEVEL connect_level_eaist1,
                o1.grbs_code grbs_code_eaist1,
                get_grbs (o1.id, 1)
                  "ОИВ, с которым связан, еаист1",
                CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                o2.id ID_EAIST2,
                O2.ID_PARENT id_parent_eaist2,
                o2.formatted_name formatted_name_eaist2,
                O2.CONNECT_LEVEL connect_level_eaist2,
                o2.grbs_code grbs_code_eaist2,
                get_grbs (o2.id, 2)
                  "ОИВ, с которым связан, еаист2",
                CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                NVL (o2.inn, o1.inn) INN,
                NVL (o2.kpp, o1.kpp) KPP,
                o2.OPEN_DATE_D OPEN_DATE,
                o2.CLOSE_DATE_D CLOSE_DATE,
                1 is_customer,
                0 is_supplier,
                V_VERSION_DATE VERSION_DATE 
            from
            (select * from lnk_grbs_code_customer where eaist=1) sp1
            join (select * from lnk_grbs_code_customer where eaist=2) sp2
            on sp1.grbs_code=sp2.grbs_code
            join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE AND version_date = V_VERSION_DATE) o1
            on o1.id=sp1.id_customer
            join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE_AUX AND version_date = V_VERSION_DATE and status NOT IN (3)) o2
            on o2.id=sp2.id_customer
            union all
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   o1.grbs_code grbs_code_eaist1,
                   get_grbs (o1.id, 1)
                      "ОИВ, с которым связан, еаист1",
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   o2.grbs_code grbs_code_eaist2,
                   get_grbs (o2.id, 2)
                      "ОИВ, с которым связан, еаист2",
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   1 is_customer,
                   0 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE) o1
                   JOIN
                      (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
                   
                   LEFT JOIN (select sp1.id_customer id_customer1, sp2.id_customer id_customer2 from
                        (select * from lnk_grbs_code_customer where eaist=1) sp1
                        join (select * from lnk_grbs_code_customer where eaist=2) sp2
                        on sp1.grbs_code=sp2.grbs_code
                        join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE_AUX AND version_date = V_VERSION_DATE and status NOT IN (3)) o2
                        on o2.id=sp2.id_customer) grbs_lnk1
                   ON o1.id=grbs_lnk1.id_customer1
                   
                   LEFT JOIN (select sp1.id_customer id_customer1, sp2.id_customer id_customer2 from
                        (select * from lnk_grbs_code_customer where eaist=1) sp1
                        join (select * from lnk_grbs_code_customer where eaist=2) sp2
                        on sp1.grbs_code=sp2.grbs_code
                        join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE AND version_date = V_VERSION_DATE) o1
                        on o1.id=sp1.id_customer) grbs_lnk2
                   ON o2.id=grbs_lnk2.id_customer2
              WHERE grbs_lnk1.id_customer1 is null and grbs_lnk2.id_customer2 is null
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   o1.grbs_code grbs_code_eaist1,
                   get_grbs (o1.id, 1)
                      "ОИВ, с которым связан, еаист1",
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   o2.grbs_code grbs_code_eaist2,
                   get_grbs (o2.id, 2)
                      "ОИВ, с которым связан, еаист2",
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   1 is_customer,
                   0 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE) o1
                   LEFT JOIN
                      (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
                   LEFT JOIN (select sp1.id_customer id_customer1, sp2.id_customer id_customer2 from
                        (select * from lnk_grbs_code_customer where eaist=1) sp1
                        join (select * from lnk_grbs_code_customer where eaist=2) sp2
                        on sp1.grbs_code=sp2.grbs_code
                        join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE_AUX AND version_date = V_VERSION_DATE and status NOT IN (3)) o2
                        on o2.id=sp2.id_customer) grbs_lnk
                   ON o1.id=grbs_lnk.id_customer1
             WHERE O2.ID IS NULL and grbs_lnk.id_customer1 is null
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   o1.grbs_code grbs_code_eaist1,
                   get_grbs (o1.id, 1)
                      "ОИВ, с которым связан, еаист1",
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   o2.grbs_code grbs_code_eaist2,
                   get_grbs (o2.id, 2)
                      "ОИВ, с которым связан, еаист2",
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   1 is_customer,
                   0 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE) o1
                   RIGHT JOIN
                      (SELECT *
                         FROM sp_customer
                        WHERE id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
                   LEFT JOIN (select sp1.id_customer id_customer1, sp2.id_customer id_customer2 from
                        (select * from lnk_grbs_code_customer where eaist=1) sp1
                        join (select * from lnk_grbs_code_customer where eaist=2) sp2
                        on sp1.grbs_code=sp2.grbs_code
                        join (SELECT * FROM sp_customer WHERE id_data_source = V_ID_DATA_SOURCE AND version_date = V_VERSION_DATE) o1
                        on o1.id=sp1.id_customer) grbs_lnk
                   ON o2.id=grbs_lnk.id_customer2
             WHERE O1.ID IS NULL and grbs_lnk.id_customer2 is null
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   NULL,
                   NULL,
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   NULL,
                   NULL,
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   0 is_customer,
                   1 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1) o1
                   JOIN
                      (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   NULL,
                   NULL,
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   NULL,
                   NULL,
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o1.inn, o2.inn) INN,
                   NVL (o1.kpp, o2.kpp) KPP,
                   o2.OPEN_DATE_D OPEN_DATE,
                   o2.CLOSE_DATE_D CLOSE_DATE,
                   0 is_customer,
                   1 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1) o1
                   LEFT JOIN
                      (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
             WHERE O2.ID IS NULL
            UNION ALL
            SELECT o1.id ID_EAIST1,
                   O1.ID_PARENT id_parent_eaist1,
                   o1.formatted_name formatted_name_eaist1,
                   O1.CONNECT_LEVEL connect_level_eaist1,
                   NULL,
                   NULL,
                   CASE WHEN o1.id IS NULL THEN 0 ELSE 1 END IS_EAIST1,
                   o2.id ID_EAIST2,
                   O2.ID_PARENT id_parent_eaist2,
                   o2.formatted_name formatted_name_eaist2,
                   O2.CONNECT_LEVEL connect_level_eaist2,
                   NULL,
                   NULL,
                   CASE WHEN o2.id IS NULL THEN 0 ELSE 1 END IS_EAIST2,
                   NVL (o2.inn, o2.inn) INN,
                   NVL (o2.kpp, o2.kpp) KPP,
                   o1.OPEN_DATE_D OPEN_DATE,
                   o1.CLOSE_DATE_D CLOSE_DATE,
                   0 is_customer,
                   1 is_supplier,
                   V_VERSION_DATE VERSION_DATE
              FROM    (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1) o1
                   RIGHT JOIN
                      (SELECT *
                         FROM sp_organization
                        WHERE     id_data_source = V_ID_DATA_SOURCE_AUX
                              AND version_date = V_VERSION_DATE
                              AND is_supplier = 1
                              AND status NOT IN (3)) o2
                   ON o1.inn = o2.inn AND o1.kpp = o2.kpp
             WHERE O1.ID IS NULL;

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- SP_CUSTOMER [EAIST1]
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMER';
    rec_array(idx).sql_name := 'SP_CUSTOMER-4 [CLEAN_1156]';
    rec_array(idx).description := 'Чистка справочника заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
	--элементы справочника ЕАИСТ1, у которых нет связки с элементами ЕАИСТ2 и по которым отсутствуют ГК с финансированием на 2014 год и далее;                                          
	--after reload SP_ORGANIZATION_JOINT

    DELETE from sp_customer c where version_date=V_VERSION_DATE and id_data_source=V_ID_DATA_SOURCE
    and not exists(select * from sp_customer o where o.version_date=c.version_date and o.id_data_source=c.id_data_source and o.id_parent=c.id)
    and not exists(select * from SP_ORGANIZATION_JOINT j where j.version_date=c.version_date and j.id_eaist2 is not null and j.id_eaist1=c.id)
    and not exists(select * from T_CONTRACT con
                                             join t_finansing_contracts fc on fc.version_date=CON.VERSION_DATE and con.id_data_source=fc.id_data_source and con.id=fc.id_contract
                                              where con.version_date=c.version_date and con.id_data_source=c.id_data_source and c.id=ID_CUSTOMER and fc.budget_year>=2014);

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- LNK_CUSTOMERS_UNITED [EAIST2]
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CUSTOMERS_UNITED';
    rec_array(idx).sql_name := 'LNK_CUSTOMERS_UNITED [RELOAD_1156]';
    rec_array(idx).description := 'Перезагрузка связей между заказчиками и объединенными заказчиками';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    delete from LNK_CUSTOMERS_UNITED where version_date=V_VERSION_DATE;
    commit;

        INSERT INTO LNK_CUSTOMERS_UNITED (CUSTOMER_ID,ID_DATA_SOURCE,UNITED_CUSTOMER_ID,UNITED_SOURCE_ID,UNITED_CUSTOMER_CID,VERSION_DATE, FORMATTED_NAME)
        SELECT id, id_data_source, id_eaist2, id_data_source_j, id_eaist2||'_'||id_data_source_j, V_VERSION_DATE, formatted_name 
        FROM 
        (SELECT c.id, c.id_data_source, oj.id_eaist2, oj.id_data_source  id_data_source_j, c.formatted_name
        FROM sp_customer c
        INNER JOIN (SELECT id_eaist1, id_eaist2, V_ID_DATA_SOURCE_AUX id_data_source FROM sp_organization_joint WHERE version_date=V_VERSION_DATE and is_customer=1 and id_eaist1 IS NOT NULL and id_eaist2 IS NOT NULL) oj
        ON c.id=oj.id_eaist1 
        WHERE c.id_data_source=V_ID_DATA_SOURCE and c.version_date=V_VERSION_DATE
        UNION ALL
        SELECT c.id, c.id_data_source, oj.id_eaist1, oj.id_data_source  id_data_source_j, c.formatted_name
        FROM sp_customer c
        INNER JOIN (select  id_eaist1, id_eaist2, V_ID_DATA_SOURCE id_data_source from sp_organization_joint WHERE version_date=V_VERSION_DATE and is_customer=1 and id_eaist1 IS NOT NULL and id_eaist2 is null) oj
        on c.id=oj.id_eaist1 
        WHERE c.id_data_source=V_ID_DATA_SOURCE and c.version_date=V_VERSION_DATE
        UNION ALL
        SELECT c.id, c.id_data_source, c.id id_eaist2, c.id_data_source id_data_source_j, c.formatted_name
        FROM sp_customer c
        WHERE c.id_data_source=V_ID_DATA_SOURCE_AUX and c.version_date=V_VERSION_DATE
        UNION ALL
        SELECT c.id, c.id_data_source, oj.id_eaist2, oj.id_data_source   id_data_source_j, c.formatted_name
        FROM sp_customer c
        INNER JOIN (select id_eaist1, id_eaist2, V_ID_DATA_SOURCE_AUX id_data_source from sp_organization_joint WHERE version_date=V_VERSION_DATE and is_customer=1 and id_eaist1 IS NOT NULL and id_eaist2 IS NOT NULL) oj
        on c.id=oj.id_eaist1 
        WHERE c.id_data_source=4 and c.version_date=V_VERSION_DATE
        UNION ALL
        SELECT c.id, c.id_data_source, oj.id_eaist1, oj.id_data_source  id_data_source_j, c.formatted_name
        FROM sp_customer c
        INNER JOIN (select  id_eaist1, id_eaist2, V_ID_DATA_SOURCE id_data_source from sp_organization_joint WHERE version_date=V_VERSION_DATE and is_customer=1 and id_eaist1 IS NOT NULL and id_eaist2 is null) oj
        on c.id=oj.id_eaist1 
        WHERE c.id_data_source=4 and c.version_date=V_VERSION_DATE);

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- GENERATE_TREE_CUSTOMERS
    idx := idx + 1;
    rec_array(idx).table_name := 'GENERATE_TREE_CUSTOMERS';
    rec_array(idx).sql_name := 'GENERATE_TREE_CUSTOMERS [RELOAD_1156]';
    rec_array(idx).description := 'Перестроение дерева';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
        delete from LNK_SOURCES_ORGS where version_date=V_VERSION_DATE;
        delete from LNK_CUSTOMERS_ALL_SOURCES where version_date=V_VERSION_DATE;
        commit;

        SINGLE_CUSTOMERS_SOURCES.GENERATE_TREE_CUSTOMERS(V_VERSION_DATE);

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- SP_CUSTOMERS_TREE
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMERS_TREE';
    rec_array(idx).sql_name := 'SP_CUSTOMERS_TREE [RELOAD_1156]';
    rec_array(idx).description := 'Перезагрузка сводного дерева заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    delete from SP_CUSTOMERS_TREE WHERE version_date = V_VERSION_DATE;
    commit;

          insert into SP_CUSTOMERS_TREE (RN,ID,  ID_SOURCE,  ID_PARENT,ID_PARENT_SOURCE,ID_LEVEL,FORMATTED_NAME,IS_AGGREGATOR,VERSION_DATE, KEY_SORT)
                      SELECT ROWNUM rn,
                             id,
                             id_source,
                             id_parent,
                             id_parent_source,
                             id_level,
                             formatted_name,
                             is_aggregator,
                             V_VERSION_DATE,
                             s_key_sort
                        FROM (    SELECT t.id,
                                         t.id_source,
                                         t.id_parent,
                                         t.id_parent_source,
                                         LEVEL + 1 AS id_level,
                                         formatted_name,
                                         s_key_sort,
                                         is_aggregator,
                                         id_agg,
                                         parent_agg
                                    FROM (SELECT r.id,
                                                 r.id_source,
                                                 r.id_parent,
                                                 r.id_parent_source,
                                                 c.formatted_name,
                                                 c.s_key_sort,
                                                 r.id || '_' || r.id_source AS id_agg,
                                                 r.id_parent || '_' || r.id_parent_source
                                                    AS parent_agg,
                                                 is_aggregator
                                            FROM    LNK_CUSTOMERS_ALL_SOURCES r
                                                 LEFT JOIN
                                                    sp_customer c
                                                 ON c.version_date = V_VERSION_DATE
                                                    AND c.id_data_source = r.id_source
                                                    AND c.id = r.id
                                           WHERE r.version_date = V_VERSION_DATE ) t
                              CONNECT BY NOCYCLE PRIOR id_agg = parent_agg
                              START WITH parent_agg = '0_2')
                  CONNECT BY PRIOR id_agg = parent_agg
                  START WITH parent_agg = '0_2'
           ORDER SIBLINGS BY CASE
                                WHEN id_level IN (2, 3)
                                THEN
                                   LPAD (s_key_sort, 16, '0')
                                ELSE
                                   formatted_name
                             END;

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- LNK_CUSTOMERS_ALL_LEVEL
    idx := idx + 1;
    rec_array(idx).table_name := 'LNK_CUSTOMERS_ALL_LEVEL';
    rec_array(idx).sql_name := 'LNK_CUSTOMERS_ALL_LEVEL [RELOAD_1156]';
    rec_array(idx).description := 'Перезагрузка связей объединенных заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 2;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
    delete from LNK_CUSTOMERS_ALL_LEVEL where version_date=V_VERSION_DATE;
    commit;

        insert into LNK_CUSTOMERS_ALL_LEVEL (ID,ID_PARENT,CONNECT_LEVEL,VERSION_DATE,ID_DATA_SOURCE,CONNECT_LEVEL_PARENT)
        select id, ID_FIRST, id_level, version_date, id_source, ID_LEVEL_FIRST 
        from
          (SELECT id,
                 trim(',' FROM sys_connect_by_path(CASE WHEN LEVEL = 1 THEN id END, ',')) ID_FIRST,
                 id_level, version_date, id_source,
                 trim(',' FROM sys_connect_by_path(CASE WHEN LEVEL = 1 THEN id_level END, ',')) ID_LEVEL_FIRST
          from (
          select id||'_'||id_source id, id_source, id_parent||'_'||id_parent_source id_parent, id_level, version_date, id_parent_source 
          from SP_CUSTOMERS_TREE where version_date=V_VERSION_DATE
          union
          select '0_'||V_ID_DATA_SOURCE, V_ID_DATA_SOURCE, null, 1, V_VERSION_DATE, V_ID_DATA_SOURCE from dual
          ) soj
          connect BY PRIOR id =id_parent )
        where id<>ID_FIRST;

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';

-- SP_CUSTOMERS_TREE
    idx := idx + 1;
    rec_array(idx).table_name := 'SP_CUSTOMERS_TREE';
    rec_array(idx).sql_name := 'SP_CUSTOMERS_TREE [CLEAN_1156]';
    rec_array(idx).description := 'Чистка сводного дерева заказчиков';
    rec_array(idx).execute_order := idx * 100;
    rec_array(idx).id_data_source := 1;
    rec_array(idx).is_actual := 1;
    rec_array(idx).sql_text := start_str || q'#
	--правка дерева, заказчики по е1, которые не имеют подведов в объединенном справочнике и не имеют контрактов с финансированием на budget_year>=2014        
    delete from SP_CUSTOMERS_TREE c where version_date=V_VERSION_DATE and id_source=V_ID_DATA_SOURCE
    and not exists(select * from SP_CUSTOMERS_TREE o where o.version_date=c.version_date and o.id_source=c.id_source and o.id_parent=c.id)
    and not exists(select * from SP_ORGANIZATION_JOINT j where j.version_date=c.version_date and j.id_eaist2 is not null and j.id_eaist1=c.id)
    and not exists(select * from T_CONTRACT con
                                             join t_finansing_contracts fc on fc.version_date=CON.VERSION_DATE and con.id_data_source=fc.id_data_source and con.id=fc.id_contract
                                              where con.version_date=c.version_date and con.id_data_source=c.id_source and c.id=ID_CUSTOMER and fc.budget_year>=2014);

		-- Привязка кол-ва обработанных строк
		:V_ROWCOUNT := sql%rowcount;

END;#';


    -- Вставка запросов
    for i in 1..rec_array.count loop
        insert into f_sp_sql_primary_data_load (
                table_name,
                sql_name,
                description,
                execute_order,
                id_data_source,
                id_data_source_aux,
                is_actual,
                sql_text
            ) values (
                rec_array(i).table_name,
                rec_array(i).sql_name,
                rec_array(i).description,
                rec_array(i).execute_order,
                rec_array(i).id_data_source,
                rec_array(i).id_data_source_aux,
                rec_array(i).is_actual,
                rec_array(i).sql_text
            );
    end loop;
end;
/

-- Подтверждение транзакции
commit;
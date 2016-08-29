--------------------------------------------------------
--  DDL for Package Body N_UTILITS3
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "REPORTS"."N_UTILITS3" AS

  FUNCTION GET_INDICATOR_FIELDS(V_TABLE_NAME IN VARCHAR2, V_INDICATOR_ID NUMBER) RETURN T_FIELDS IS
    V_RESULT_FIELDS T_FIELDS;
    i number;
  BEGIN
    i :=0;
    --Получаем список полей куба второго типа для показателя, в виде массива строк
    for j in (SELECT DISTINCT ATTR_NAME_2LVL FROM (select * from f_sp_indicators where id = V_INDICATOR_ID) IND         --Показатели
                join f_lnk_ind_cube LNKIC ON IND.ID=LNKIC.INDICATOR_ID                                                  --Связи показателей с кубами второго типа          
                join (select * from f_sp_cube_2lvl where table_name=V_TABLE_NAME) CUBES ON CUBES.ID=LNKIC.CUBE_2LVL_ID  --Кубы второго типа
                join f_lnk_cube_group LNKCG on LNKCG.CUBE_2LVL_ID=CUBES.ID and LNKCG.CUBE_1LVL_ID=IND.CUBE_1LVL_ID      --Связи кубов с группировками
                join (select id,ATTR_NAME_2LVL from f_sp_group) GR      --По группировкам можно определить наличие PERIOD_TYPE
                ON LNKCG.GROUP_ID=GR.ID
              union
              select 'VERSION_DATE' from dual)
    LOOP
      i := i+1;
      V_RESULT_FIELDS(i) := j.ATTR_NAME_2LVL;
    END LOOP;
    
    RETURN V_RESULT_FIELDS;
  END GET_INDICATOR_FIELDS;
  
  FUNCTION GET_TABLE_FIELDS(V_TABLE_NAME IN VARCHAR2, V_SOURCE_TABLE IN VARCHAR2) RETURN T_FIELDS IS
    V_RESULT_FIELDS T_FIELDS;
    i number;
  BEGIN
    i :=0;
    --Получаем список полей куба второго типа для показателя, в виде массива строк
    for j in (SELECT DISTINCT ATTR_NAME_2LVL  from f_sp_cube_2lvl cube2
                                              join f_lnk_cube_group LNKCG on LNKCG.CUBE_2LVL_ID=CUBE2.ID
                                              join f_sp_cube_1lvl cube1 on LNKCG.CUBE_1LVL_ID=CUBE1.ID
                                              join f_sp_group GR ON LNKCG.GROUP_ID=GR.ID
                                              WHERE cube2.TABLE_NAME=V_TABLE_NAME and CUBE1.TABLE_NAME=V_SOURCE_TABLE
              union
              select 'VERSION_DATE' from dual)
    LOOP
      i := i+1;
      V_RESULT_FIELDS(i) := j.ATTR_NAME_2LVL;
    END LOOP;
    
    RETURN V_RESULT_FIELDS;
  END GET_TABLE_FIELDS;
  
  FUNCTION GET_T_FIELDS_DIFF(V_FIELDS1 IN T_FIELDS, V_FIELDS2 IN T_FIELDS) RETURN T_FIELDS IS
    V_RESULT_FIELDS T_FIELDS;
    i number;
    j number;
    k number := 0;
    V_EXIST_FLAG BOOLEAN;
  BEGIN
    for i in 1..V_FIELDS1.COUNT LOOP
      V_EXIST_FLAG := false;
      for j in 1..V_FIELDS2.COUNT LOOP
        if V_FIELDS1(i) = V_FIELDS2(j) then
          V_EXIST_FLAG := true;
        end if;
      END LOOP;
      if not V_EXIST_FLAG then
        k := k+1;
        V_RESULT_FIELDS(k) := V_FIELDS1(i);
      end if;
    END LOOP;    
  
    RETURN V_RESULT_FIELDS;
  END GET_T_FIELDS_DIFF;
  
  FUNCTION GET_T_FIELDS_SUMM(V_FIELDS1 IN T_FIELDS, V_FIELDS2 IN T_FIELDS) RETURN T_FIELDS IS
    V_TMP_FIELDS T_FIELDS;
    V_RESULT_FIELDS T_FIELDS;
    V_RES_COUNT NUMBER;
    i number;
  BEGIN
    V_TMP_FIELDS := GET_T_FIELDS_DIFF(V_FIELDS2, V_FIELDS1);
    for i in 1..V_FIELDS1.COUNT LOOP
      V_RESULT_FIELDS(i) := V_FIELDS1(i);
    END LOOP;
    
    V_RES_COUNT := V_RESULT_FIELDS.COUNT;
    for i in 1..V_TMP_FIELDS.COUNT LOOP
      V_RESULT_FIELDS(i+V_RES_COUNT) := V_TMP_FIELDS(i);
    END LOOP;
    
    RETURN V_RESULT_FIELDS;
  END GET_T_FIELDS_SUMM;
  
  FUNCTION GET_T_FIELDS_INTER(V_FIELDS1 IN T_FIELDS, V_FIELDS2 IN T_FIELDS) RETURN T_FIELDS IS
    V_RESULT_FIELDS T_FIELDS;
    i number;
    j number;
    k number := 0;
    V_EXIST_FLAG BOOLEAN;
  BEGIN
    for i in 1..V_FIELDS1.COUNT LOOP
      V_EXIST_FLAG := false;
      for j in 1..V_FIELDS2.COUNT LOOP
        if V_FIELDS1(i) = V_FIELDS2(j) then
          V_EXIST_FLAG := true;
        end if;
      END LOOP;
      if V_EXIST_FLAG then
        k := k+1;
        V_RESULT_FIELDS(k) := V_FIELDS1(i);
      end if;
    END LOOP;    
  
    RETURN V_RESULT_FIELDS;
  END GET_T_FIELDS_INTER;  

  FUNCTION CHECK_DATA(V_VERSION_DATE IN DATE, V_TABLE_NAME IN VARCHAR2) RETURN BOOLEAN IS
    V_ROW_COUNT NUMBER := 0;
    V_SQL CLOB;
    V_CURSOR INTEGER;
    V_IGNORE INTEGER;
  BEGIN
    V_SQL := 'SELECT COUNT(*) CNT  FROM '||V_TABLE_NAME||' WHERE VERSION_DATE=TO_DATE(:V_VERSION_DATE, ''DD.MM.YYYY'')';
    V_CURSOR := DBMS_SQL.OPEN_CURSOR;
    
    DBMS_SQL.PARSE(V_CURSOR, V_SQL, DBMS_SQL.native);
    DBMS_SQL.DEFINE_COLUMN(V_CURSOR, 1, V_ROW_COUNT);
    DBMS_SQL.BIND_VARIABLE(V_CURSOR, ':V_VERSION_DATE', TO_CHAR(V_VERSION_DATE, 'dd.mm.yyyy'));
    V_IGNORE := DBMS_SQL.EXECUTE(V_CURSOR);
    
    IF DBMS_SQL.FETCH_ROWS(V_CURSOR) > 0 THEN
      DBMS_SQL.COLUMN_VALUE(V_CURSOR, 1, V_ROW_COUNT);    
    END IF;
    DBMS_SQL.CLOSE_CURSOR(V_CURSOR);
    
    IF V_ROW_COUNT = 0 THEN
      RETURN TRUE;  
    ELSE
      RETURN FALSE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    IF DBMS_SQL.IS_OPEN(V_CURSOR) THEN
      DBMS_SQL.CLOSE_CURSOR(V_CURSOR);
    END IF;
    RETURN FALSE;
  END  CHECK_DATA;

  --^^Функции для работы с массивами строк^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  
  FUNCTION FORMATE_CONDITION_SQL (V_VERSION_DATE IN DATE, inExpression IN varchar2, cond_level in NUMBER := -1) RETURN VARCHAR2 IS
    TYPE r_expression IS RECORD (
      exp_code varchar(10),
      exp_sql varchar(1000));
    TYPE t_expression IS TABLE OF r_expression
    INDEX BY BINARY_INTEGER;
    
    templateString varchar2(20);
    expArray t_expression;
    
    tmpExpression varchar2(1000);
    resExpression varchar2(1000);
    expCount number;
    stc number :=0;
  begin
    templateString := '\[(c[[:digit:]]*)\]';                --Так в условии выглядит ссылка на другое условие
    expCount := regexp_count(inExpression, templateString); --Определяем количество таковых
    --dbms_output.put_line(inExpression||' '||expCount);
    IF expCount > 0 THEN
      FOR i in 1..expCount LOOP
        begin        
          expArray(i).exp_code := regexp_substr(inExpression, templateString, 1, i);  --Получаем код i-го условия
          --Получаем текст i-го условия(Да, что бы понять рекурсию, нужно понять рекурсию)
          SELECT (listagg(replace(' ('||N_UTILITS3.FORMATE_CONDITION_SQL(V_VERSION_DATE, condition_sql, cond_level)||')','()'), ' AND ') within group (order by condition_sql)) 
          INTO expArray(i).exp_sql 
          FROM f_sp_condition 
          WHERE code=regexp_replace(expArray(i).exp_code, '\[|\]') AND (CONDITION_LEVEL=cond_level or cond_level=-1 or condition_level=-1)
            AND (V_VERSION_DATE between VERS_START_DATE AND VERS_END_DATE);
          --dbms_output.put_line(inExpression||' '||i||' '||expArray(i).exp_code||' '||expArray(i).exp_sql);
        exception when others then
          dbms_output.put_line(SQLERRM);
        end;  
      END LOOP;
      
      --Заменяем ссылки на другие условие непосредственно на сами условия
      tmpExpression := inExpression;
      FOR i in 1..expCount LOOP
        tmpExpression := replace(tmpExpression, expArray(i).exp_code, expArray(i).exp_sql);
      END LOOP;
      resExpression := ' ('||tmpExpression||') ';
        FOR i in 1..2 LOOP
          resExpression := replace(resExpression, '  ', ' ');
          resExpression := replace(resExpression, 'and and', 'and');
          resExpression := replace(resExpression, 'or or', 'or');
          resExpression := replace(resExpression, 'and not and not', 'and not');
          ----resExpression :=regexp_replace(resExpression, '(^ \( and \) $)|(^ \( and not)|( and not \) $)|(^ \( and)|( and \) $)');
          resExpression :=regexp_replace(resExpression, '(^ \( and \) $)');
          resExpression :=regexp_replace(resExpression, '(and|and not|and not and|or) \) $',' )');
          resExpression :=regexp_replace(resExpression, '^ \( (and|and not|and not and|or)',' (');
          resExpression :=replace(replace(resExpression,'( and (','( ('),') and )',') )');
          resExpression := regexp_replace(resExpression, '\( *\)','');
          resExpression := regexp_replace(resExpression, '\(+ *not *\)+','');
        END LOOP;
    ELSE
      resExpression := inExpression;
    END IF;
    RETURN resExpression;
  END FORMATE_CONDITION_SQL;
  
  FUNCTION GET_PARAMS_SUBQUERY(V_VERSION_DATE IN DATE, V_TABLE_NAME IN VARCHAR2, V_SOURCE_TABLE IN VARCHAR2) RETURN CLOB IS 
    V_MAIN_DIMS TYPE_NT_TBL_S;
    V_HIER_TABLES TYPE_NT_TBL_S;
    
    V_SELECT_PART CLOB;
    V_SELECT_AGR_PART CLOB;
    V_SELECT_AGR2_PART CLOB;
    V_WHERE_PART CLOB;
    V_QUERY CLOB;
    V_AGR_QUERY CLOB;
    V_AGR2_QUERY CLOB;
    V_RESULT_QUERY CLOB;
    j NUMBER;
    k NUMBER;
  BEGIN
    V_MAIN_DIMS := TYPE_NT_TBL_S();
    V_HIER_TABLES := TYPE_NT_TBL_S();
    
    select  (listagg((CASE WHEN group_by=attr_name_1lvl then 'DAT.' END)||group_by||' '||attr_name_2lvl, ', ') within group (order by group_by)) sel_str --для селекта
    into V_SELECT_PART
    FROM (select group_by, attr_name_1lvl, attr_name_2lvl
    from f_sp_cube_2lvl cube2
    join f_lnk_cube_group LNKCG on LNKCG.CUBE_2LVL_ID=CUBE2.ID
    join f_sp_cube_1lvl cube1 on LNKCG.CUBE_1LVL_ID=CUBE1.ID
    join f_sp_group GR ON LNKCG.GROUP_ID=GR.ID
    WHERE cube2.TABLE_NAME=V_TABLE_NAME and CUBE1.TABLE_NAME=V_SOURCE_TABLE
    union
    select 'VERSION_DATE', 'VERSION_DATE', 'VERSION_DATE' from dual)
    ;
    
    V_WHERE_PART:=chr(10)||' WHERE (VERSION_DATE = TO_DATE('''||TO_CHAR(V_VERSION_DATE, 'dd.mm.yyyy')||''', ''dd.mm.yyyy''))';

    --Получаем Список полей для агрегации
    FOR i IN (SELECT distinct trim(regexp_substr(MAIN_DIM, '[^;]+', 1, LEVEL)) MAIN_DIM
              FROM (SELECT MAIN_DIM FROM F_SP_CUBE_2LVL WHERE TABLE_NAME=V_TABLE_NAME and MAIN_DIM IS NOT NULL)
              CONNECT BY LEVEL <= regexp_count(MAIN_DIM, ';')+1) 
    LOOP
      V_MAIN_DIMS.extend;
      V_MAIN_DIMS(V_MAIN_DIMS.COUNT) := i.MAIN_DIM;
      --Получаем линковочные таблицы по этим полям
      V_HIER_TABLES.extend;
      SELECT HIERARHY_TABLE INTO V_HIER_TABLES(V_MAIN_DIMS.COUNT) FROM F_LNK_MAINDIM_SOURCE  WHERE COLUMN_NAME=i.MAIN_DIM;     
    END LOOP;    
    
    --Базовый подзапрос
    V_QUERY := 'SELECT DISTINCT '||V_SELECT_PART||' FROM '||V_SOURCE_TABLE||' DAT '||V_WHERE_PART;
    --Конечный запрос
    V_RESULT_QUERY := V_QUERY||chr(10);
    
    
    FOR J IN 1..V_MAIN_DIMS.COUNT LOOP
      V_SELECT_AGR_PART := REPLACE(V_SELECT_PART, 'DAT.'||V_MAIN_DIMS(j), 'LNK.ID_PARENT');
      V_AGR_QUERY := 'SELECT DISTINCT '||V_SELECT_AGR_PART||' FROM 
                    (SELECT * FROM '||V_SOURCE_TABLE||' '||V_WHERE_PART||') DAT
                    JOIN '||V_HIER_TABLES(J)||' LNK 
                    ON DAT.VERSION_DATE=LNK.VERSION_DATE AND DAT.'||V_MAIN_DIMS(J)||'=LNK.ID';
      V_RESULT_QUERY := V_RESULT_QUERY||' UNION '||chr(10)||V_AGR_QUERY||chr(10);
      IF J > 1 THEN
        FOR K IN 1..J-1 LOOP
          V_SELECT_AGR2_PART := REPLACE(V_SELECT_AGR_PART, 'DAT.'||V_MAIN_DIMS(K), 'LNK'||K||'.ID_PARENT');
          V_AGR2_QUERY := 'SELECT DISTINCT '||V_SELECT_AGR2_PART||' FROM 
                    (SELECT * FROM '||V_SOURCE_TABLE||' '||V_WHERE_PART||') DAT
                    JOIN '||V_HIER_TABLES(J)||' LNK 
                    ON DAT.VERSION_DATE=LNK.VERSION_DATE AND DAT.'||V_MAIN_DIMS(J)||'=LNK.ID
                    JOIN '||V_HIER_TABLES(K)||' LNK'||K||' 
                    ON DAT.VERSION_DATE=LNK'||K||'.VERSION_DATE AND DAT.'||V_MAIN_DIMS(K)||'=LNK'||K||'.ID';
          V_RESULT_QUERY := V_RESULT_QUERY||' UNION '||chr(10)||V_AGR2_QUERY||chr(10);
        END LOOP;
      END IF;
    END LOOP;
    
    V_RESULT_QUERY := '(SELECT DISTINCT  '||REGEXP_REPLACE(V_SELECT_PART, 'TRUNC\(([[:alpha:]]*_)*[[:alpha:]]*, ''MM''\)')||' FROM ('||CHR(10)||V_RESULT_QUERY||') DAT) PAR_'||V_SOURCE_TABLE;
    RETURN V_RESULT_QUERY;
  END GET_PARAMS_SUBQUERY;
  
  FUNCTION GET_DEFAULT_VALUE(V_TABLE_NAME IN VARCHAR2, V_COLUMN_NAME IN VARCHAR2) RETURN VARCHAR2 IS
    V_DEFAULT_VAL VARCHAR2(40);
    V_DATA_TYPE VARCHAR2(40);
  BEGIN
    
    --SELECT DATA_TYPE INTO V_DATA_TYPE FROM F_CUBE_1LVL_METADATA META
    --JOIN F_SP_CUBE_1LVL CUBE ON META.CUBE_1LVL_ID=CUBE.ID WHERE CUBE.TABLE_NAME=V_TABLE_NAME AND META.COLUMN_NAME=V_COLUMN_NAME;
    --dbms_output.put_line(v_table_name||' '||v_column_name);
    SELECT distinct DATA_TYPE INTO V_DATA_TYPE FROM USER_TAB_COLUMNS meta    
    LEFT join F_SP_GROUP gr on meta.column_name=gr.attr_name_1lvl
    WHERE TABLE_NAME=V_TABLE_NAME AND (COLUMN_NAME=V_COLUMN_NAME or attr_name_2lvl=V_COLUMN_NAME);
    --dbms_output.put_line(V_DATA_TYPE);
    V_DEFAULT_VAL :=
    CASE
      WHEN V_DATA_TYPE='DATE' THEN 'TO_DATE(''01.01.4000'',''dd.mm.yyyy'')'
      WHEN V_DATA_TYPE='TIMESTAMP(6)' THEN 'TO_DATE(''01.01.4000'',''dd.mm.yyyy'')'
      ELSE '-1'
    END;
    
    RETURN V_DEFAULT_VAL;
  
  END GET_DEFAULT_VALUE;
  
  FUNCTION GET_TABLE_JOIN (V_TABLES IN T_TABLES, V_TABLE_INDEX IN NUMBER) RETURN CLOB IS
    V_JOIN CLOB;
    V_COUNTER NUMBER := 0;
    I NUMBER;
    J NUMBER;
    
    V_CURR_TAB_FIELDS T_FIELDS;
    V_NEAR_TAB_FIELDS T_FIELDS;
    V_TMP_INTER T_FIELDS;
    V_NEAR_TABLE VARCHAR2(40);
    V_DEFAULT_VAL VARCHAR2(40);
  BEGIN 
    --dbms_output.put_line(0);
    V_CURR_TAB_FIELDS := V_TABLES(V_TABLE_INDEX).TABLE_FIELDS;
    
    /*LOOP
      V_COUNTER := V_COUNTER+1;
      V_TMP_INTER := GET_T_FIELDS_INTER(V_CURR_TAB_FIELDS, V_TABLES(V_COUNTER).TABLE_FIELDS);
      IF V_TMP_INTER.COUNT>0 THEN
          V_CURR_TAB_FIELDS := GET_T_FIELDS_DIFF(V_CURR_TAB_FIELDS, V_TABLES(V_COUNTER).TABLE_FIELDS);
          FOR I IN 1..V_TMP_INTER.COUNT LOOP
            V_DEFAULT_VAL := GET_DEFAULT_VALUE(V_TABLES(V_COUNTER).TABLE_NAME, V_TMP_INTER(I));
            V_JOIN := V_JOIN||'NVL(PAR_'||V_TABLES(V_TABLE_INDEX).TABLE_NAME||'.'||V_TMP_INTER(I)||', '||V_DEFAULT_VAL||') = NVL(PAR_'||V_TABLES(V_COUNTER).TABLE_NAME||'.'||V_TMP_INTER(I)||','||V_DEFAULT_VAL||') AND ';
          END LOOP;      
      END IF;    
    EXIT WHEN ((V_COUNTER = V_TABLES.COUNT-1) OR (V_CURR_TAB_FIELDS.COUNT=0));
    END LOOP;*/
    FOR J in 1..V_TABLES.COUNT LOOP
      V_TMP_INTER := GET_T_FIELDS_INTER(V_CURR_TAB_FIELDS, V_TABLES(J).TABLE_FIELDS);
      IF V_TMP_INTER.COUNT>0 THEN
        FOR I IN 1..V_TMP_INTER.COUNT LOOP
          V_DEFAULT_VAL := GET_DEFAULT_VALUE(V_TABLES(j).TABLE_NAME, V_TMP_INTER(I));
          V_JOIN := V_JOIN||'NVL(PAR_'||V_TABLES(V_TABLE_INDEX).TABLE_NAME||'.'||V_TMP_INTER(I)||', '||V_DEFAULT_VAL||') = NVL(PAR_'||V_TABLES(j).TABLE_NAME||'.'||V_TMP_INTER(I)||','||V_DEFAULT_VAL||') AND ';
        END LOOP;      
      END IF;  
    END LOOP;
    --dbms_output.put_line(V_JOIN);
    
    IF LENGTH(V_JOIN)>0 THEN
      V_JOIN := SUBSTR(V_JOIN, 1, LENGTH(V_JOIN)-4);
    END IF;
    RETURN V_JOIN;
  END GET_TABLE_JOIN;
  
  FUNCTION FIND_COLUMN(V_TABLES IN T_TABLES, V_COLUMN_NAME IN VARCHAR2) RETURN CLOB IS
    V_RESULT_COLUMN VARCHAR2(100);
    V_RESULT CLOB;
    I NUMBER;
    J NUMBER;
  BEGIN
   FOR I in 1..V_TABLES.COUNT LOOP
      FOR J in 1..V_TABLES(i).TABLE_FIELDS.COUNT LOOP     
        IF V_TABLES(i).TABLE_FIELDS(j)=V_COLUMN_NAME THEN
          V_RESULT_COLUMN := 'PAR_'||V_TABLES(i).TABLE_NAME||'.'||V_COLUMN_NAME;         
          IF V_RESULT is null THEN             
            V_RESULT := V_RESULT_COLUMN;
          ELSE
            V_RESULT := 'NVL('||V_RESULT_COLUMN||', '||V_RESULT||')';
          END IF;         
        END IF;
      END LOOP;
    END LOOP;
    RETURN chr(10)||V_RESULT;
  END FIND_COLUMN;
  
  FUNCTION GET_PARAMS_QUERY (V_VERSION_DATE IN DATE, V_TABLE_NAME IN VARCHAR2) RETURN CLOB IS
    V_QUERY CLOB;
    V_CUBE_1LVL_NAME VARCHAR2(40);
    V_INDICATORS T_INDICATORS;
    V_IND_COUNTER NUMBER := 0;
    V_INDICATOR_SUBQUERY CLOB;
    V_INDICATOR_JOIN CLOB;
    V_SELECT_PART CLOB;
    V_INSERT_PART CLOB;
    
    V_TABLE_COUNTER NUMBER := 0;   
    
    V_TABLES T_TABLES;
    V_TABLE_JOIN CLOB;
    V_TABLE_SUBQUERY CLOB;
  BEGIN
    FOR i IN (SELECT DISTINCT CUBE1.TABLE_NAME FROM F_SP_CUBE_1LVL CUBE1
                                      JOIN F_LNK_CUBE_GROUP LNK ON CUBE1.ID=LNK.CUBE_1LVL_ID
                                      JOIN F_SP_CUBE_2LVL CUBE2 ON LNK.CUBE_2LVL_ID=CUBE2.ID
                                      WHERE CUBE2.TABLE_NAME=V_TABLE_NAME)
    LOOP
      --dbms_output.put_line(i.TABLE_NAME);
      V_TABLE_COUNTER := V_TABLE_COUNTER + 1;
      V_TABLES(V_TABLE_COUNTER).TABLE_NAME := i.TABLE_NAME;
      V_TABLES(V_TABLE_COUNTER).TABLE_FIELDS := GET_TABLE_FIELDS(V_TABLE_NAME, i.TABLE_NAME);
      V_TABLE_SUBQUERY := GET_PARAMS_SUBQUERY(V_VERSION_DATE, V_TABLE_NAME, i.TABLE_NAME);
      IF V_TABLE_COUNTER <> 1 THEN
        V_TABLE_JOIN := GET_TABLE_JOIN(V_TABLES, V_TABLE_COUNTER);
        V_QUERY := V_QUERY||chr(10)||'FULL JOIN '||V_TABLE_SUBQUERY||chr(10)||'ON '||V_TABLE_JOIN;
      ELSE
        V_QUERY := 'FROM '||V_TABLE_SUBQUERY;
      END IF;       
    END LOOP;
    V_INSERT_PART := 'INSERT INTO '||V_TABLE_NAME||' (';
    V_SELECT_PART := 'SELECT DISTINCT';
    FOR i in (SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME=V_TABLE_NAME AND NOT REGEXP_LIKE(COLUMN_NAME, '(^VALUE)|(^IS_D[[:digit:]]+)'))
    LOOP
      V_INSERT_PART := V_INSERT_PART||i.COLUMN_NAME||', ';
      V_SELECT_PART := V_SELECT_PART||FIND_COLUMN(V_TABLES, i.COLUMN_NAME)||', ';
    END LOOP;
    V_INSERT_PART := SUBSTR(V_INSERT_PART, 1, LENGTH(V_INSERT_PART)-2)||') ';
    V_SELECT_PART := SUBSTR(V_SELECT_PART, 1, LENGTH(V_SELECT_PART)-2);
    V_QUERY := V_INSERT_PART||chr(10)||V_SELECT_PART||chr(10)||V_QUERY;
    --dbms_output.put_line(V_INSERT_PART||chr(10)||V_SELECT_PART);
    RETURN V_QUERY;
  END GET_PARAMS_QUERY;
  
  FUNCTION CHECK_DIMENSION_FLAG(V_TABLE_NAME IN VARCHAR2, V_DIM_FLAG IN VARCHAR2) RETURN BOOLEAN IS
    V_RESULT BOOLEAN;
    V_DIM_STRING VARCHAR2(20);
    V_CURRENT_REPORT NUMBER;
  BEGIN
    V_DIM_STRING := LPAD(V_DIM_FLAG, 20, '0');
    V_CURRENT_REPORT := TO_NUMBER(REPLACE(REGEXP_SUBSTR(V_TABLE_NAME, '_[[:digit:]]*$'), '_'));
    IF SUBSTR(V_DIM_STRING, 20-V_CURRENT_REPORT+1, 1)='1' THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;
  END;
  
  
  
  FUNCTION GET_AGREGATE_SUBQUERY(V_VERSION_DATE IN DATE, V_TABLE_NAME IN VARCHAR2, V_INDICATOR_ID IN NUMBER) RETURN CLOB IS
    V_FROM_PART VARCHAR2(100);
    V_VAL_PART VARCHAR2(40);
    V_NONVAL_PART CLOB;
  
    V_MAIN_DIMS TYPE_NT_TBL_S;
    V_HIER_TABLES TYPE_NT_TBL_S;
    
    V_SELECT_PART CLOB;
    V_SELECT_AGR_PART CLOB;
    V_SELECT_AGR2_PART CLOB;
    V_WHERE_PART CLOB;
    V_QUERY CLOB;
    V_AGR_QUERY CLOB;
    V_AGR2_QUERY CLOB;
    V_RESULT_QUERY CLOB;
    j NUMBER;
    k NUMBER;
  BEGIN
    V_MAIN_DIMS := TYPE_NT_TBL_S();
    V_HIER_TABLES := TYPE_NT_TBL_S();
    --Получаем название куба первого типа
    SELECT CUBES.TABLE_NAME 
    INTO V_FROM_PART 
    FROM F_SP_INDICATORS IND, F_SP_CUBE_1LVL CUBES  
    WHERE IND.CUBE_1LVL_ID=CUBES.ID AND IND.ID=V_INDICATOR_ID;
    
    --Получаем название атрибута - значения
    SELECT 'DAT.'||FLD.ATTRIBUTE_NAME 
    INTO V_VAL_PART
    FROM F_SP_INDICATORS IND, F_SP_AGREGATE_FUNC FNC, F_SP_AGREGATE_FIELD FLD
    WHERE IND.AGR_FUNC_ID=FNC.ID AND IND.AGR_FIELD_ID=FLD.ID AND IND.ID=V_INDICATOR_ID;
    
    --Получаем названия атрибутов дял фильтрации
    FOR i in (SELECT DIMENSION_FLAG, META.COLUMN_NAME
              FROM F_CUBE_1LVL_METADATA META, F_SP_INDICATORS IND 
              WHERE IND.CUBE_1LVL_ID=META.CUBE_1LVL_ID AND IND.ID=V_INDICATOR_ID AND 'DAT.'||META.COLUMN_NAME <> UPPER(V_VAL_PART))
    LOOP 
      IF CHECK_DIMENSION_FLAG(V_TABLE_NAME, i.DIMENSION_FLAG) THEN
        V_NONVAL_PART := V_NONVAL_PART||', DAT.'||i.COLUMN_NAME;
      END IF;
    END LOOP;
    
    --SELECT LISTAGG('DAT.'||META.COLUMN_NAME, ', ') within group (order by META.COLUMN_NAME) COLUMNS_LIST
    --INTO V_NONVAL_PART
    --FROM F_CUBE_1LVL_METADATA META, F_SP_INDICATORS IND 
    --WHERE IND.CUBE_1LVL_ID=META.CUBE_1LVL_ID AND IND.ID=V_INDICATOR_ID AND META.DIMENSION_FLAG=1 AND 'DAT.'||META.COLUMN_NAME <> UPPER(V_VAL_PART);
    
     --получаем условия для данного показателя на данную дату
    SELECT (listagg(lnk.negation||' ('||N_UTILITS2.FORMATE_CONDITION_SQL(V_VERSION_DATE, cond.condition_sql, 1)||')', ' AND ') within group (order by cond.condition_sql)) 
    INTO V_WHERE_PART
    FROM F_SP_CONDITION cond, F_LNK_IND_COND lnk 
    WHERE cond.code=lnk.condition_code and cond.condition_level in (1,-1) and lnk.indicator_id=V_INDICATOR_ID
      and (V_VERSION_DATE between cond.vers_start_date and cond.vers_end_date) and (V_VERSION_DATE between lnk.vers_start_date and lnk.vers_end_date);
      
    V_WHERE_PART := regexp_replace(V_WHERE_PART, '\( *\)','');
    
    --добавляем к условиям дату среза  
    IF (V_WHERE_PART is null OR V_WHERE_PART like ' ') THEN
      V_WHERE_PART:=chr(10)||' WHERE (VERSION_DATE = TO_DATE('''||TO_CHAR(V_VERSION_DATE, 'dd.mm.yyyy')||''', ''dd.mm.yyyy''))';
    ELSE
      V_WHERE_PART:=chr(10)||' WHERE (VERSION_DATE = TO_DATE('''||TO_CHAR(V_VERSION_DATE, 'dd.mm.yyyy')||''', ''dd.mm.yyyy'') AND '||V_WHERE_PART||')'; 
    END IF;
    
    --Получаем Список полей для агрегации
    FOR i IN (SELECT distinct trim(regexp_substr(MAIN_DIM, '[^;]+', 1, LEVEL)) MAIN_DIM
              FROM (SELECT MAIN_DIM FROM F_SP_CUBE_2LVL WHERE TABLE_NAME=V_TABLE_NAME and MAIN_DIM IS NOT NULL)
              CONNECT BY LEVEL <= regexp_count(MAIN_DIM, ';')+1) 
    LOOP
      V_MAIN_DIMS.extend;
      V_MAIN_DIMS(V_MAIN_DIMS.COUNT) := i.MAIN_DIM;
      --Получаем линковочные таблицы по этим полям
      V_HIER_TABLES.extend;
      SELECT HIERARHY_TABLE INTO V_HIER_TABLES(V_MAIN_DIMS.COUNT) FROM F_LNK_MAINDIM_SOURCE  WHERE COLUMN_NAME=i.MAIN_DIM;     
    END LOOP;    
    
    --Базовая строка селекта
    V_SELECT_PART := V_VAL_PART||V_NONVAL_PART;
    --Базовый подзапрос
    V_QUERY := 'SELECT DISTINCT '||V_SELECT_PART||' FROM '||V_FROM_PART||' DAT '||V_WHERE_PART;
    --Конечный запрос
    V_RESULT_QUERY := V_QUERY||chr(10);
    
    
    FOR J IN 1..V_MAIN_DIMS.COUNT LOOP
      V_SELECT_AGR_PART := REPLACE(V_SELECT_PART, 'DAT.'||V_MAIN_DIMS(j), 'LNK.ID_PARENT');
      V_AGR_QUERY := 'SELECT DISTINCT '||V_SELECT_AGR_PART||' FROM 
                    (SELECT * FROM '||V_FROM_PART||' '||V_WHERE_PART||') DAT
                    JOIN '||V_HIER_TABLES(J)||' LNK 
                    ON DAT.VERSION_DATE=LNK.VERSION_DATE AND DAT.'||V_MAIN_DIMS(J)||'=LNK.ID';
      V_RESULT_QUERY := V_RESULT_QUERY||' UNION '||chr(10)||V_AGR_QUERY||chr(10);
      IF J > 1 THEN
        FOR K IN 1..J-1 LOOP
          V_SELECT_AGR2_PART := REPLACE(V_SELECT_AGR_PART, 'DAT.'||V_MAIN_DIMS(K), 'LNK'||K||'.ID_PARENT');
          V_AGR2_QUERY := 'SELECT DISTINCT '||V_SELECT_AGR2_PART||' FROM 
                    (SELECT * FROM '||V_FROM_PART||' '||V_WHERE_PART||') DAT
                    JOIN '||V_HIER_TABLES(J)||' LNK 
                    ON DAT.VERSION_DATE=LNK.VERSION_DATE AND DAT.'||V_MAIN_DIMS(J)||'=LNK.ID
                    JOIN '||V_HIER_TABLES(K)||' LNK'||K||' 
                    ON DAT.VERSION_DATE=LNK'||K||'.VERSION_DATE AND DAT.'||V_MAIN_DIMS(K)||'=LNK'||K||'.ID';
          V_RESULT_QUERY := V_RESULT_QUERY||' UNION '||chr(10)||V_AGR2_QUERY||chr(10);
        END LOOP;
      END IF;
    END LOOP;
    

    RETURN V_RESULT_QUERY;
  END GET_AGREGATE_SUBQUERY;
  
  FUNCTION GET_INDICATOR_QUERY (V_VERSION_DATE IN DATE, V_TABLE_NAME VARCHAR2, V_INDICATOR_ID IN NUMBER) RETURN CLOB IS
    V_INSERT_TAB VARCHAR2(100);
    V_INSERT_PART CLOB;
    V_SELECT_AGR CLOB;
    V_SELECT_PART CLOB;
    V_FROM_PART CLOB;
    V_WHERE_PART CLOB;
    V_GROUP_PART CLOB;
    
    V_SUBFROM_PART VARCHAR2(100);
    V_SUBVAL_PART VARCHAR2(40);
    V_SUBSELECT_PART CLOB;
    
    V_QUERY clob;
    V_RESULT_QUERY CLOB;
    
    rowCounter NUMBER;
    
    TYPE T_QUERY_STRUCT IS RECORD(
      GROUP_STR CLOB,
      SELECT_STR CLOB,
      INSERT_STR CLOB);
    V_QUERY_FIELDS T_QUERY_STRUCT;
  BEGIN
  
    --Получаем поле со значением показателя
    SELECT chr(10)||'SELECT '||V_INDICATOR_ID||' INDICATOR_ID, TO_DATE('''||TO_CHAR(V_VERSION_DATE,'dd.mm.yyyy')||''', ''dd.mm.yyyy'') VERSION_DATE, '||
            FNC.FUNC||'('||IND.DISTINCT_OPER||' '||FLD.ATTRIBUTE_NAME||') VALUE,' 
    INTO V_SELECT_AGR 
    FROM F_SP_INDICATORS IND, F_SP_AGREGATE_FUNC FNC, F_SP_AGREGATE_FIELD FLD
    WHERE IND.AGR_FUNC_ID=FNC.ID AND IND.AGR_FIELD_ID=FLD.ID AND IND.ID=V_INDICATOR_ID;
    
    select  (listagg(group_by, ', ') within group (order by group_by)) gr_str --для группировки
             ,(listagg(group_by||' '||attr_name_2lvl, ', ') within group (order by group_by)) sel_str --для селекта
             ,(listagg(attr_name_2lvl, ', ') within group (order by group_by)) ins_str--для инсерта  
    into V_QUERY_FIELDS          
    from 
      (select * from f_sp_indicators where id=V_INDICATOR_ID) IND
      join f_lnk_ind_cube LNKIC ON IND.ID=LNKIC.INDICATOR_ID
      join (select * from f_sp_cube_2lvl where table_name=V_TABLE_NAME) CUBES ON CUBES.ID=LNKIC.CUBE_2LVL_ID
      join f_lnk_cube_group LNKCG on LNKCG.CUBE_2LVL_ID=CUBES.ID and LNKCG.CUBE_1LVL_ID=IND.CUBE_1LVL_ID
      join f_sp_group GR ON LNKCG.GROUP_ID=GR.ID;
    
    V_FROM_PART := chr(10)||' FROM '||'('||GET_AGREGATE_SUBQUERY(V_VERSION_DATE,V_TABLE_NAME, V_INDICATOR_ID)||') ';
    v_group_part := chr(10)||' GROUP BY '||V_QUERY_FIELDS.GROUP_STR; 
    V_select_part := V_SELECT_AGR||' '||V_QUERY_FIELDS.SELECT_STR;
    V_QUERY := V_SELECT_PART||V_FROM_PART||V_GROUP_PART;
    RETURN V_QUERY;
  END GET_INDICATOR_QUERY;
  
  FUNCTION GET_MERGE_QUERY(V_VERSION_DATE IN DATE, V_TABLE_NAME IN VARCHAR2, V_INDICATOR_ID IN NUMBER) RETURN CLOB IS
    V_QUERY CLOB;
    V_INDICATOR_SUBQUERY CLOB;
    V_ON_PART CLOB;
    V_IND_FIELDS T_FIELDS;
    i NUMBER;
    V_DEFAULT_VAL VARCHAR2(40);
  BEGIN
    V_INDICATOR_SUBQUERY := GET_INDICATOR_QUERY(V_VERSION_DATE, V_TABLE_NAME, V_INDICATOR_ID);
    V_IND_FIELDS := GET_INDICATOR_FIELDS(V_TABLE_NAME, V_INDICATOR_ID);
    FOR i in 1..V_IND_FIELDS.COUNT LOOP
      if i <> 1 then
        V_ON_PART := V_ON_PART||' AND ';
      end if;
      --V_ON_PART := V_ON_PART||'TRG.'||V_IND_FIELDS(i)||'='||'SRC.'||V_IND_FIELDS(i);
      V_DEFAULT_VAL := GET_DEFAULT_VALUE(V_TABLE_NAME, V_IND_FIELDS(i));
      V_ON_PART := V_ON_PART||'NVL(TRG.'||V_IND_FIELDS(i)||', '||V_DEFAULT_VAL||
                    ') =NVL(SRC.'||V_IND_FIELDS(i)||','||V_DEFAULT_VAL||')';
    END LOOP;
    
    
    V_QUERY := 'MERGE INTO '||V_TABLE_NAME||' TRG 
                    USING ('||V_INDICATOR_SUBQUERY||') SRC
                    ON ('||V_ON_PART||')
                    WHEN MATCHED THEN UPDATE
                    SET VALUE'||V_INDICATOR_ID||' = SRC.VALUE';
    RETURN V_QUERY;
  END GET_MERGE_QUERY;
  
  
  
  PROCEDURE LOAD_CUBE_2LVL(V_VERSION_DATE IN DATE, V_TABLE_NAME VARCHAR2) IS
    V_QUERY CLOB;
    v_rowCounter number;
    v_errNumber number:=0;
    v_errMessage varchar2(4000);
    v_counter number;
  BEGIN
  
    LOAD_PARAMS(V_VERSION_DATE,V_TABLE_NAME);
    
    LOAD_FLAGS(V_VERSION_DATE,V_TABLE_NAME);
    
      --создаем индексы для мержа
      for i in (select * from f_sp_sql_create_indexes where is_actual=1)
      loop
        begin
        --проверяем наличие индекса
        select count(*) into v_counter from user_indexes where table_owner='REPORTS' and table_name=i.table_name and index_name=i.index_name;
            if v_counter=0
                then execute immediate i.sql_text;
            end if;
        exception when others
            then
                v_errNumber := sqlcode;
                v_errMessage := sqlerrm;
                insert into t_error_log(error_num,error_message,description,name_object,version_date) 
                    values(v_errNumber,v_errMessage,'Ошибка создания индекса'||i.index_name,i.table_name,v_version_date);
                commit;    
        end;              
      end loop;    
    
    FOR i IN (SELECT IND.ID ID FROM (SELECT ID FROM F_SP_INDICATORS WHERE CALCULATE_METHOD=1) IND
              JOIN (SELECT * FROM F_LNK_IND_CUBE) LNK ON IND.ID=LNK.INDICATOR_ID
              JOIN (SELECT * FROM F_SP_CUBE_2LVL WHERE TABLE_NAME=V_TABLE_NAME) CUBE ON CUBE.ID=LNK.CUBE_2LVL_ID)
    LOOP
      BEGIN
        V_QUERY := GET_MERGE_QUERY(V_VERSION_DATE, V_TABLE_NAME, i.ID);
--        insert into tmp_sql_cube_2lvl (date_rec,cube_name,id_indicator,sql_text) values (sysdate,V_TABLE_NAME,i.ID,V_QUERY);--генерация запросов для оптимизации
--        commit;
        execute immediate(V_QUERY);
        
        v_rowCounter := SQL%ROWCOUNT;
        COMMIT;
        
        INSERT INTO T_ERROR_LOG (error_message, name_object,description,row_count)
        SELECT CASE WHEN v_rowCounter = 0 THEN '0 rows inserted' ELSE '' END error_message,
               V_TABLE_NAME||' INDICATOR:'||i.ID name_object,
               'Загрузка данных успешно завершена' description,
               v_rowCounter row_count 
        FROM DUAL;
        COMMIT;
        
      EXCEPTION WHEN OTHERS THEN 
        ROLLBACK; 
        v_errNumber := SQLCODE;
        v_errMessage := SQLERRM;
        INSERT INTO T_ERROR_LOG (ERROR_NUM,ERROR_MESSAGE,NAME_OBJECT) VALUES (v_errNumber,v_errMessage, V_TABLE_NAME||' INDICATOR:'||i.ID);
        COMMIT;
      END;
    END LOOP;
    
    CALCULATE_FORMULA_INDICATORS(V_VERSION_DATE, V_TABLE_NAME);
  END LOAD_CUBE_2LVL;
  
  PROCEDURE LOAD_CUBE_2LVL_INDICATOR(V_VERSION_DATE IN DATE, V_TABLE_NAME VARCHAR2, V_INDICATOR_ID IN NUMBER) IS
    V_QUERY CLOB;
    v_rowCounter number;
    v_errNumber number:=0;
    v_errMessage varchar2(4000);
  BEGIN   
    V_QUERY := GET_MERGE_QUERY(V_VERSION_DATE, V_TABLE_NAME, V_INDICATOR_ID);
    execute immediate(V_QUERY);
    
    v_rowCounter := SQL%ROWCOUNT;
    COMMIT;
    
    INSERT INTO T_ERROR_LOG (error_message, name_object,description,row_count)
    SELECT CASE WHEN v_rowCounter = 0 THEN '0 rows inserted' ELSE '' END error_message,
           V_TABLE_NAME||' INDICATOR:'||V_INDICATOR_ID name_object,
           'Загрузка данных успешно завершена' description,
           v_rowCounter row_count 
    FROM DUAL;
    COMMIT;
  EXCEPTION WHEN OTHERS THEN 
    ROLLBACK; 
    v_errNumber := SQLCODE;
    v_errMessage := SQLERRM;
    INSERT INTO T_ERROR_LOG (ERROR_NUM,ERROR_MESSAGE,NAME_OBJECT) VALUES (v_errNumber,v_errMessage, V_TABLE_NAME||' INDICATOR:'||V_INDICATOR_ID);
    COMMIT;   
  END LOAD_CUBE_2LVL_INDICATOR;
  
------------------------------------------------------ LOAD_FLAGS ------------------------------  
  PROCEDURE LOAD_FLAGS(V_VERSION_DATE IN DATE, V_TABLE_NAME IN VARCHAR2) IS
    V_QUERY CLOB;
    v_rowCounter number;
    v_errNumber number:=0;
    v_errMessage varchar2(4000);
    vn number;
    V_GROUP varchar2 (4000);
    V_FIELD varchar2 (10);
  BEGIN

    --собираем все детализации данного отчета
    for i in (
      select distinct lic.cube_2lvl_id, si.cube_1lvl_id 
      from f_sp_cube_2lvl c2l
      inner join f_lnk_ind_cube lic on c2l.id=lic.cube_2lvl_id
      inner join f_sp_indicators si on lic.indicator_id=si.id and si.calculate_method=1
      where c2l.table_name=V_TABLE_NAME)
    loop
      vn:=1;
      V_GROUP:='';
      V_FIELD:='IS_D'||TO_CHAR(i.cube_1lvl_id);
      V_QUERY:='';
      
      --собираем все поля детализации
      for j in (
        select sg.attr_name_2lvl column_name
        from f_sp_cube_2lvl c2l
        inner join f_lnk_cube_group lcg on c2l.id=lcg.cube_2lvl_id
        inner join f_sp_group sg on lcg.group_id=sg.id
        where c2l.id=i.cube_2lvl_id and lcg.cube_1lvl_id=i.cube_1lvl_id)
      loop
        if vn>1 then
          V_GROUP:=V_GROUP||', ';
        end if;
        V_GROUP:=V_GROUP||j.column_name;
        vn:=vn+1;
      end loop;
      
      --составляем мерж для флага
      V_QUERY:='MERGE INTO '||V_TABLE_NAME||' cub'||chr(10)||
      'USING (SELECT '||V_GROUP||', MIN(ROWID) MIR'||chr(10)||
      'FROM '||V_TABLE_NAME||chr(10)||
      'WHERE VERSION_DATE=TO_DATE('''||TO_CHAR(V_VERSION_DATE, 'dd.mm.yyyy')||''', ''dd.mm.yyyy'')'||chr(10)||
      'GROUP BY '||V_GROUP||') mint'||chr(10)|| 
      'ON (cub.ROWID=mint.MIR AND cub.VERSION_DATE=TO_DATE('''||TO_CHAR(V_VERSION_DATE, 'dd.mm.yyyy')||''', ''dd.mm.yyyy''))'||chr(10)|| 
      'WHEN MATCHED THEN UPDATE SET cub.'||V_FIELD||'=1';
    
--        insert into tmp_sql_cube_2lvl (date_rec,cube_name,id_indicator,sql_text) values (sysdate,V_TABLE_NAME,'PARAMS_QUERY',V_QUERY);--генерация запросов для оптимизации
--        commit;
        execute immediate(V_QUERY);
      
      v_rowCounter := SQL%ROWCOUNT;
      COMMIT;
      
      INSERT INTO T_ERROR_LOG (error_message, name_object,description,row_count)
      SELECT CASE WHEN v_rowCounter = 0 THEN '0 rows inserted' ELSE '' END error_message,
             V_TABLE_NAME||' FLAG '||V_FIELD name_object,
             'Загрузка данных успешно завершена' description,
             v_rowCounter row_count 
      FROM DUAL;
      COMMIT;
    end loop;
  
  EXCEPTION WHEN OTHERS THEN 
    ROLLBACK; 
    v_errNumber := SQLCODE;
    v_errMessage := SQLERRM;
    INSERT INTO T_ERROR_LOG (ERROR_NUM,ERROR_MESSAGE,NAME_OBJECT) VALUES (v_errNumber,v_errMessage, V_TABLE_NAME||' FLAGS');
    COMMIT;
  END LOAD_FLAGS;

------------------------------------------------------------------------------------------------
  
  PROCEDURE LOAD_PARAMS(V_VERSION_DATE IN DATE, V_TABLE_NAME IN VARCHAR2) IS
    V_QUERY CLOB;
    v_rowCounter number;
    v_errNumber number:=0;
    v_errMessage varchar2(4000);
  BEGIN
    V_QUERY := GET_PARAMS_QUERY(V_VERSION_DATE, V_TABLE_NAME);
    dbms_output.put_line(v_query);
--        insert into tmp_sql_cube_2lvl (date_rec,cube_name,id_indicator,sql_text) values (sysdate,V_TABLE_NAME,'PARAMS_QUERY',V_QUERY);--генерация запросов для оптимизации
--        commit;
        execute immediate(V_QUERY);
    
    v_rowCounter := SQL%ROWCOUNT;
    COMMIT;
    
    INSERT INTO T_ERROR_LOG (error_message, name_object,description,row_count)
    SELECT CASE WHEN v_rowCounter = 0 THEN '0 rows inserted' ELSE '' END error_message,
           V_TABLE_NAME||' PARAMS' name_object,
           'Загрузка данных успешно завершена' description,
           v_rowCounter row_count 
    FROM DUAL;
    COMMIT;
  EXCEPTION WHEN OTHERS THEN 
    ROLLBACK; 
    v_errNumber := SQLCODE;
    v_errMessage := SQLERRM;
    INSERT INTO T_ERROR_LOG (ERROR_NUM,ERROR_MESSAGE,NAME_OBJECT) VALUES (v_errNumber,v_errMessage, V_TABLE_NAME||' PARAMS');
    COMMIT;
  END LOAD_PARAMS;
   
  PROCEDURE CALCULATE_FORMULA_INDICATORS(V_VERSION_DATE IN DATE, V_TABLE_NAME IN VARCHAR2) IS
    V_QUERY CLOB;
    V_SET_PART CLOB;
    
    V_FORMULA CLOB;
    V_IND_COUNT NUMBER;
    V_TMP_STR VARCHAR2(40);
    J NUMBER;
    
    v_rowCounter number;
    v_errNumber number:=0;
    v_errMessage varchar2(4000);
  BEGIN
    FOR i IN (SELECT IND.ID, IND.CALCULATE_FORMULA FROM (SELECT ID, CALCULATE_FORMULA FROM F_SP_INDICATORS WHERE CALCULATE_METHOD=2) IND
              JOIN (SELECT * FROM F_LNK_IND_CUBE) LNK ON IND.ID=LNK.INDICATOR_ID
              JOIN (SELECT * FROM F_SP_CUBE_2LVL WHERE TABLE_NAME=V_TABLE_NAME) CUBE ON CUBE.ID=LNK.CUBE_2LVL_ID)
    LOOP
      --dbms_output.put_line(i.ID);
      V_FORMULA := i.CALCULATE_FORMULA;
      V_TMP_STR := REGEXP_SUBSTR(V_FORMULA, '\/\[[[:digit:]]*\]');
      V_FORMULA := REPLACE(V_FORMULA, V_TMP_STR, REPLACE(V_TMP_STR , '/', '/nullif(')||', 0)');
      V_FORMULA := REPLACE(V_FORMULA, '[', 'VALUE');
      V_FORMULA := REPLACE(V_FORMULA, ']');
      
      V_SET_PART := V_SET_PART||'VALUE'||i.ID||' = '||V_FORMULA||', ';
      
    END LOOP;
    IF V_SET_PART IS NOT NULL THEN
      V_SET_PART := SUBSTR(V_SET_PART, 1, LENGTH(V_SET_PART)-2);
      V_QUERY := 'UPDATE '||V_TABLE_NAME||' SET '||V_SET_PART||' WHERE VERSION_DATE=TO_DATE('''||TO_CHAR(V_VERSION_DATE, 'dd.mm.yyyy')||''', ''dd.mm.yyyy'')';
      dbms_output.put_line(v_query);
      EXECUTE IMMEDIATE(V_QUERY);
      v_rowCounter := SQL%ROWCOUNT;
      COMMIT;    
      INSERT INTO T_ERROR_LOG (error_message, name_object,description,row_count)
      SELECT CASE WHEN v_rowCounter = 0 THEN '0 rows inserted' ELSE '' END error_message,
             V_TABLE_NAME name_object,
             'Загрузка Формульных показателей прошла успешно' description,
             v_rowCounter row_count 
      FROM DUAL;
      COMMIT;
    END IF;
  EXCEPTION WHEN OTHERS THEN 
    ROLLBACK; 
    v_errNumber := SQLCODE;
    v_errMessage := SQLERRM;
    INSERT INTO T_ERROR_LOG (ERROR_NUM,ERROR_MESSAGE,NAME_OBJECT) VALUES (v_errNumber,v_errMessage,V_TABLE_NAME||' формульные');
    COMMIT;  
  END CALCULATE_FORMULA_INDICATORS;
  
  FUNCTION GET_NOTNULL_ROWS_COUNT(V_VERSION_DATE IN DATE, V_TABLE_NAME IN VARCHAR2) RETURN NUMBER IS
    V_QUERY CLOB;
    V_WHERE_PART CLOB;
    V_ROWS_COUNT NUMBER;
    
    V_CURSOR INTEGER;
    V_IGNORE INTEGER;
  BEGIN
    V_QUERY := 'SELECT COUNT(*) FROM '||V_TABLE_NAME||' WHERE VERSION_DATE=TO_DATE('''||TO_CHAR(V_VERSION_DATE, 'dd.mm.yyyy')||''', ''dd.mm.yyyy'') AND (';
    FOR i IN (SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE REGEXP_LIKE(COLUMN_NAME, '^VALUE') AND TABLE_NAME=V_TABLE_NAME)
    LOOP
      V_WHERE_PART := V_WHERE_PART||i.COLUMN_NAME||' IS NOT NULL OR ';
    END LOOP;
    V_WHERE_PART := SUBSTR(V_WHERE_PART, 1, LENGTH(V_WHERE_PART)-4)||')';
    V_QUERY := V_QUERY||V_WHERE_PART;
    dbms_output.put_line(v_query);
    
    V_CURSOR := DBMS_SQL.OPEN_CURSOR;
    
    DBMS_SQL.PARSE(V_CURSOR, V_QUERY, DBMS_SQL.native);
    DBMS_SQL.DEFINE_COLUMN(V_CURSOR, 1, V_ROWS_COUNT);
    V_IGNORE := DBMS_SQL.EXECUTE(V_CURSOR);
    
    IF DBMS_SQL.FETCH_ROWS(V_CURSOR) > 0 THEN
      DBMS_SQL.COLUMN_VALUE(V_CURSOR, 1, V_ROWS_COUNT);    
    END IF;
    DBMS_SQL.CLOSE_CURSOR(V_CURSOR);
    RETURN V_ROWS_COUNT;
  END GET_NOTNULL_ROWS_COUNT;
  
/*  PROCEDURE UPDATE_S_BLOCK_ATTRS IS
    V_IS_PERIOD NUMBER;
    V_IS_BUDGET_YEAR NUMBER;
    V_IS_PLAN_PERIOD NUMBER;
    V_IS_PLAN_YEAR NUMBER;
    V_LOOP_COUNT NUMBER;
    V_COUNTER NUMBER;
  BEGIN
    --Блоки с расчетными показателями
    FOR i IN (SELECT DISTINCT
        B.ID,
        MAX(CASE WHEN REGEXP_LIKE(lower(CONDITION_SQL), '(\:period_date)|(\:period_end_date)') THEN 1 ELSE 0 END) OVER (PARTITION BY IND.ID) IS_PERIOD,
        MAX(CASE WHEN REGEXP_LIKE(lower(CONDITION_SQL), '\:budget_year') THEN 1 ELSE 0 END) OVER (PARTITION BY IND.ID) IS_BUDGET_YEAR,
        MAX(CASE WHEN REGEXP_LIKE(lower(CONDITION_SQL), '\:plan_period') THEN 1 ELSE 0 END) OVER (PARTITION BY IND.ID) IS_PLAN_PERIOD,
        MAX(CASE WHEN REGEXP_LIKE(lower(CONDITION_SQL), '\:plan_year') THEN 1 ELSE 0 END) OVER (PARTITION BY IND.ID) IS_PLAN_YEAR
      FROM F_SP_INDICATORS IND
      JOIN F_LNK_IND_COND LNK ON LNK.INDICATOR_ID=IND.ID
      JOIN F_SP_CONDITION COND ON LNK.CONDITION_CODE=COND.CODE
      JOIN S_SP_BLOCK B ON IND.ID=B.INDICATOR_ID
      WHERE COND.CONDITION_LEVEL=2 AND IND.CALCULATE_METHOD=1)
    LOOP
      UPDATE S_SP_BLOCK SET IS_PERIOD=i.IS_PERIOD, IS_BUDGET_YEAR=i.IS_BUDGET_YEAR, IS_PLAN_PERIOD=i.IS_PLAN_PERIOD, IS_PLAN_YEAR=i.IS_PLAN_YEAR WHERE ID=i.ID;
      COMMIT;
    END LOOP;
    
    --формульные показатели
    FOR i IN (SELECT DISTINCT
        B.ID,
        MAX(CASE WHEN REGEXP_LIKE(lower(CONDITION_SQL), '(\:period_date)|(\:period_end_date)') THEN 1 ELSE 0 END) OVER (PARTITION BY IND.ID) IS_PERIOD,
        MAX(CASE WHEN REGEXP_LIKE(lower(CONDITION_SQL), '\:budget_year') THEN 1 ELSE 0 END) OVER (PARTITION BY IND.ID) IS_BUDGET_YEAR,
        MAX(CASE WHEN REGEXP_LIKE(lower(CONDITION_SQL), '\:plan_period') THEN 1 ELSE 0 END) OVER (PARTITION BY IND.ID) IS_PLAN_PERIOD,
        MAX(CASE WHEN REGEXP_LIKE(lower(CONDITION_SQL), '\:plan_year') THEN 1 ELSE 0 END) OVER (PARTITION BY IND.ID) IS_PLAN_YEAR
      FROM F_SP_INDICATORS IND
      JOIN F_SP_INDICATORS S_IND ON REGEXP_LIKE(IND.CALCULATE_FORMULA, '\['||S_IND.ID||'\]')
      JOIN F_LNK_IND_COND LNK ON LNK.INDICATOR_ID=S_IND.ID
      JOIN F_SP_CONDITION COND ON LNK.CONDITION_CODE=COND.CODE
      JOIN S_SP_BLOCK B ON IND.ID=B.INDICATOR_ID
      WHERE COND.CONDITION_LEVEL=2 AND IND.CALCULATE_METHOD=2)
    LOOP
      UPDATE S_SP_BLOCK SET IS_PERIOD=i.IS_PERIOD, IS_BUDGET_YEAR=i.IS_BUDGET_YEAR, IS_PLAN_PERIOD=i.IS_PLAN_PERIOD, IS_PLAN_YEAR=i.IS_PLAN_YEAR WHERE ID=i.ID;
      COMMIT;
    END LOOP;
     
    --Блоки контейнеры 
    SELECT  max(level)-1 INTO V_LOOP_COUNT FROM S_SP_BLOCK B connect by  prior id=parent_id start with parent_id is null;
    FOR V_COUNTER IN 1..V_LOOP_COUNT LOOP
      FOR i IN (SELECT  B.ID,
                        MAX(S_B.IS_PERIOD) OVER (PARTITION BY B.ID) IS_PERIOD,
                        MAX(S_B.IS_BUDGET_YEAR) OVER (PARTITION BY B.ID) IS_BUDGET_YEAR,
                        MAX(S_B.IS_PLAN_PERIOD) OVER (PARTITION BY B.ID) IS_PLAN_PERIOD,
                        MAX(S_B.IS_PLAN_YEAR) OVER (PARTITION BY B.ID) IS_PLAN_YEAR
                  FROM S_SP_BLOCK B
                  JOIN S_SP_BLOCK S_B ON B.ID=S_B.PARENT_ID
                  WHERE B.INDICATOR_ID IS NULL )
      LOOP
        UPDATE S_SP_BLOCK SET IS_PERIOD=i.IS_PERIOD, IS_BUDGET_YEAR=i.IS_BUDGET_YEAR, IS_PLAN_PERIOD=i.IS_PLAN_PERIOD, IS_PLAN_YEAR=i.IS_PLAN_YEAR WHERE ID=i.ID;
        COMMIT;
      END LOOP;
    END LOOP;
  END UPDATE_S_BLOCK_ATTRS;*/

END N_UTILITS3;

/

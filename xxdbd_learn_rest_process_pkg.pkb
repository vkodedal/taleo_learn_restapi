CREATE OR REPLACE package body xxdbd_learn_rest_process_pkg is
--
-- Change History 
-------------------------------------------------------------------------------------------------------------
-- Version     Changed By                       Change Date                  Comments
-------------------------------------------------------------------------------------------------------------
-- 1.1         Vidyadhar Kodedala             15-Jun-2018            Initial Creation 

      g_benefit_action_id number;
      g_request_id number;
      TERM_THRESHOLD number;
      g_proc                   VARCHAR2 (80);
      TYPE g_cache_person_process_object IS RECORD (
      non_person_cd               ben_person_actions.non_person_cd%TYPE,
      person_action_id        ben_person_actions.person_action_id%TYPE,
      object_version_number   ben_person_actions.object_version_number%TYPE
     );

    TYPE g_cache_person_process_rec IS TABLE OF g_cache_person_process_object
      INDEX BY BINARY_INTEGER;

    g_cache_person_process   g_cache_person_process_rec;
       
function get_supervisor_flag(p_emp_num in varchar2) return varchar2 is
    v_sup varchar2(10);
    cursor csr_sup is
    select 'Y' from dual where exists (select null from xxdbd.XXDBD_FSN_LEARN_STG_TBL sup where sup.supervisor_id=p_emp_num or sup.hr_partner_id=p_emp_num or sup.learning_mentor_id=p_emp_num or matrix_manager_id=p_emp_num);
begin
    v_sup := null;
    open csr_sup;
    fetch csr_sup into v_sup;
    close csr_sup;
    if nvl(v_sup,'x') = 'Y' then
        return 'Yes';
    else 
        return 'No';
   end if; 
end;

procedure main(p_errbuff out varchar2, p_retcode out number) is


begin

    fnd_file.put_line(fnd_file.log,'Entering main..');
    
    fnd_file.put_line(fnd_file.log,'Update flags '||to_char(sysdate, 'dd-mon-yyyy HH24:MI:SS'));
    
    update_flags;
    
    fnd_file.put_line(fnd_file.log,'Process users '||to_char(sysdate, 'dd-mon-yyyy HH24:MI:SS'));
    
    process_users;    
    
    fnd_file.put_line(fnd_file.log,'Process supervisors '||to_char(sysdate, 'dd-mon-yyyy HH24:MI:SS'));
    
    process_supervisors;
    
    fnd_file.put_line(fnd_file.log,'Process supervisees '||to_char(sysdate, 'dd-mon-yyyy HH24:MI:SS'));
    
    process_supervisees;
    
    fnd_file.put_line(fnd_file.log,'Process custom fields '||to_char(sysdate, 'dd-mon-yyyy HH24:MI:SS'));
    
    process_custom_fields_MT;
    
    fnd_file.put_line(fnd_file.log,'Leaving main');
    fnd_file.put_line(fnd_file.log,'----------------------------------------------------------');

exception when others then

    p_errbuff := substr(sqlerrm,1,2000);
    p_retcode := -20116;
    fnd_file.put_line(fnd_file.log,'Exception :'||p_errbuff);
     

end main;


procedure update_flags is
     v_sup_flag varchar2(10);
      cursor csr_stg is
        select oracle_id from XXDBD.XXDBD_FSN_LEARN_STG_TBL where oracle_id is not null;
begin

    update  xxdbd.XXDBD_FSN_LEARN_STG_TBL 
    set NEW_HIRES_GROUP_FLAG = (case when trunc(sysdate)-to_date(LATEST_START_DATE,'DD-MON-YYYY') >90 then 'No' else 'Yes' end)
    where length(latest_start_date) = 11;
    
    fnd_file.put_line(fnd_file.log,'Updating Supervisor Group '||to_char(sysdate, 'dd-mon-yyyy HH24:MI:SS'));
    
    v_sup_flag := null;
    for rec_stg in csr_stg loop
        v_sup_flag := get_supervisor_flag(rec_stg.oracle_id);
        update  xxdbd.XXDBD_FSN_LEARN_STG_TBL stg
        set SUPERVISORS_GROUP_FLAG=v_sup_flag
        where oracle_id=rec_stg.oracle_id;
    end loop;
    commit;
    
end;
    

procedure process_users is

       
        cursor csr_new_hires is
        select oracle_id from XXDBD.XXDBD_FSN_LEARN_STG_TBL
        minus
        select Oracle_id from xxdbd.xxdbd_learn_user where status IN ('PROCESSED','UPDATED','NEW') --AND ACCOUNT_EXPIRATION IS NULL
        ;

        cursor csr_terms is
        select Oracle_id from xxdbd.xxdbd_learn_user where status IN ('PROCESSED','UPDATED','NEW') AND ACCOUNT_EXPIRATION IS NULL
        minus
        select oracle_id from XXDBD.XXDBD_FSN_LEARN_STG_TBL
        ;
         
        cursor csr_user_updates is
        select ORACLE_ID ,FIRST_NAME ,LAST_NAME ,decode(GENDER,'M','male','F','female') GENDER,to_char(to_date(LATEST_START_DATE,'DD-MON-YYYY'), 'YYYY-MM-DD"T"HH24:MI:SS') HIRE_DATE,TITLE ,HOME_CITY ,HOME_STATE ,HOME_ZIP ,HOME_COUNTRY , (CASE WHEN INSTR(EMAIL,' ') >0 OR LENGTH(EMAIL)>50 OR INSTR(EMAIL,'@') = 0THEN NULL ELSE EMAIL END) EMAIL from XXDBD.XXDBD_FSN_LEARN_STG_TBL where length(latest_start_date) = 11 
        minus
        select ORACLE_ID ,FIRST_NAME ,LAST_NAME ,GENDER ,HIRE_DATE ,JOB_TITLE ,HOME_CITY ,HOME_STATE ,HOME_ZIP ,HOME_COUNTRY ,DECODE(EMAIL,'invalid_email@dieboldnixdorf.com', NULL, EMAIL) EMAIL from xxdbd.xxdbd_learn_user where status IN ('PROCESSED','UPDATED','NEW')
        ;
        
        cursor csr_learn_threshold is
        select to_number(meaning) from fnd_lookup_values where lookup_type='DBD_HR_LEARN_REST_CONFIG' and lookup_code='TERM_THRESHOLD';
         
        term_count number;
begin


    
     fnd_file.put_line(fnd_file.log,'Inserting new hires '||to_char(sysdate, 'dd-mon-yyyy HH24:MI:SS'));
     
    for hire_rec in csr_new_hires loop
        insert into xxdbd.xxdbd_learn_user (
        ORACLE_ID          
        ,FIRST_NAME         
        ,LAST_NAME          
        ,GENDER             
        ,HIRE_DATE          
        ,JOB_TITLE          
        ,HOME_CITY          
        ,HOME_STATE         
        ,HOME_ZIP           
        ,HOME_COUNTRY       
        ,EMAIL                        
        ,STATUS 
        )
        select ORACLE_ID ,FIRST_NAME ,LAST_NAME ,decode(GENDER,'M','male','F','female') ,to_char(to_date(LATEST_START_DATE,'DD-MON-YYYY'), 'YYYY-MM-DD"T"HH24:MI:SS')  ,TITLE ,HOME_CITY ,HOME_STATE ,HOME_ZIP ,HOME_COUNTRY ,EMAIL ,'NEW' 
        from XXDBD.XXDBD_FSN_LEARN_STG_TBL
        where oracle_id=hire_rec.ORACLE_ID;
    end loop;
    
    commit;
    
    fnd_file.put_line(fnd_file.log,'Marking terminations  '||to_char(sysdate, 'dd-mon-yyyy HH24:MI:SS'));
    term_count := 0;
    FOR term_rec in csr_terms loop
        update  xxdbd.xxdbd_learn_user set status='EXPIRED' where oracle_id=term_rec.oracle_id;
        term_count:=term_count+1;
    end loop;
    
    OPEN csr_learn_threshold;
    FETCH csr_learn_threshold INTO TERM_THRESHOLD;
    CLOSE csr_learn_threshold;
    
    if term_count > NVL(TERM_THRESHOLD,1000) then    
        rollback;
        raise_application_error('Exceeded termination threshold limit.',-20001); 
    end if;
    
    fnd_file.put_line(fnd_file.log,'Marking user updates '||to_char(sysdate, 'dd-mon-yyyy HH24:MI:SS'));
    
    for upd_rec in csr_user_updates loop
        insert_or_update_user(      p_oracle_id                  =>upd_rec.ORACLE_ID  
                                                ,p_first_name              =>upd_rec.FIRST_NAME         
                                                ,p_last_name               =>upd_rec.LAST_NAME          
                                                ,p_gender                  =>upd_rec.GENDER             
                                                ,p_hire_date               =>upd_rec.HIRE_DATE          
                                                ,p_job_title               =>upd_rec.TITLE          
                                                ,p_home_city               =>upd_rec.HOME_CITY          
                                                ,p_home_state              =>upd_rec.HOME_STATE         
                                                ,p_home_zip                =>upd_rec.HOME_ZIP           
                                                ,p_home_country            =>upd_rec.HOME_COUNTRY       
                                                ,p_email                   =>upd_rec.EMAIL                 
                                                ,p_status                  =>'UPDATED'
                                                );
    end loop;
    
    
end;



procedure insert_or_update_user(   p_oracle_id          IN VARCHAR2 
                                                    ,p_first_name         IN VARCHAR2 
                                                    ,p_last_name          IN VARCHAR2 
                                                    ,p_gender             IN VARCHAR2 
                                                    ,p_hire_date          IN VARCHAR2 
                                                    ,p_job_title          IN VARCHAR2 
                                                    ,p_home_city          IN VARCHAR2 
                                                    ,p_home_state         IN VARCHAR2 
                                                    ,p_home_zip           IN VARCHAR2 
                                                    ,p_home_country       IN VARCHAR2 
                                                    ,p_email              IN VARCHAR2 
                                                    ,p_account_expiration IN VARCHAR2 
                                                    ,p_user_id            IN NUMBER
                                                    ,p_status             IN VARCHAR2 
                                                     ) is

v_exists varchar2(1);
cursor csr_user_exists(c_oracle_id in varchar2) is
select 'Y' from xxdbd.xxdbd_learn_user where oracle_id=c_oracle_id;

begin

    v_exists := 'N';
    open  csr_user_exists(p_oracle_id); 
    fetch csr_user_exists into v_exists;
    close csr_user_exists;

    if v_exists='Y' then
                update xxdbd.xxdbd_learn_user
                set 
                 first_name              = p_first_name         
                ,last_name               = p_last_name          
                ,gender                  = p_gender             
                ,hire_date               = p_hire_date          
                ,job_title               = p_job_title          
                ,home_city               = p_home_city          
                ,home_state              = p_home_state         
                ,home_zip                = p_home_zip           
                ,home_country            = p_home_country       
                ,email                   = p_email              
                ,account_expiration      = (CASE WHEN NVL(NVL(hire_date, p_hire_date),'X') = NVL(p_hire_date,'X') THEN p_account_expiration ELSE NULL END) 
                ,user_id                 = nvl(p_user_id, user_id)            
                ,status                  = p_status  
                where oracle_id=p_oracle_id
                  ;
     else
            insert into xxdbd.xxdbd_learn_user (
                                                    oracle_id          
                                                    ,first_name         
                                                    ,last_name          
                                                    ,gender             
                                                    ,hire_date          
                                                    ,job_title          
                                                    ,home_city          
                                                    ,home_state         
                                                    ,home_zip           
                                                    ,home_country       
                                                    ,email  
                                                    ,user_id                      
                                                    ,status 
                                                    )
                                       values(p_oracle_id          
                                                    ,p_first_name         
                                                    ,p_last_name          
                                                    ,p_gender             
                                                    ,p_hire_date          
                                                    ,p_job_title          
                                                    ,p_home_city          
                                                    ,p_home_state         
                                                    ,p_home_zip           
                                                    ,p_home_country       
                                                    ,p_email  
                                                    ,p_user_id              
                                                    ,p_status 
                                                    );            
     
     end if;  

end insert_or_update_user;

procedure process_custom_fields_MT is

begin
     fnd_file.put_line(fnd_file.log,'Entering process_custom_fields_MT');
        insert_actions('PC', fnd_global.conc_request_id);
        submit_request(10, 'process_custom_fields');
     fnd_file.put_line(fnd_file.log,'Exiting process_custom_fields_MT');   
end;


procedure process_custom_fields(p_membership_id in number) is

        cursor csr_changed_fields(c_mship_id number) is
        select fields.id,  value
          from xxdbd.XXDBD_FSN_LEARN_STG_TBL
        unpivot
        (
          value
            for field_name in (
            "LOCATION",
        "ORGANIZATION",
        "SUPERVISOR_ID",
        "COST_CENTER",
        "JOB_STATE",
        "PT_FT",
        "JOB_GROUP",
        "HR_PARTNER_NAME",
        "HR_PARTNER_ID",
        "LATEST_START_DATE",
        "ALTERNATE_TITLE",
        "GRADE",
        "REGION",
        "DIEBOLD_MASTER",
        "LEARNING_MENTOR_ID",
        "LEARNING_MENTOR_NAME",
        "MATRIX_MANAGER_ID",
        "MATRIX_MANAGER_NAME",
        "VEHICLE",
        "TECHNICIAN_TYPE",
        "LC_ADMIN",
        "BRANCH",
        "MIDDLE_INITIAL",
        "EMPLOYEE_TYPE",
        "LANGUAGE",
        "ACQUISITION",
        "ALL_USERS_GROUP_FLAG",
        "SUPERVISORS_GROUP_FLAG",
        "NEW_HIRES_GROUP_FLAG",
        --"All Users Group Flag",
        --"Supervisors Group Flag",
        --"New Hires Group Flag",
        "SYSTEM_ACCESS_SUSPENSION",
        "PRIOR_SUPERVISOR_ID",
        --"TemporaryValue-UserMapping",
        "ORACLE",
        "SAP",
        "TBD3",
        "TBD4",
        "TBD5",
        "TBD6",
        "TBD7",
        "TBD8",
        "TBD9",
        "TBD10",
        "TBD11",
        "TBD12",
        "TBD13",
        "TBD14",
        "TBD15",
        "TEST",
        "GIN_NUMBER",
        "BUSINESS_GROUP_NAME",
        "SYSTEM_STATUS",
        "LINE_OF_BUSINESS",
        "FUNCTION",
        "SUB_FUNCTION",
        "DEPARTMENT",
        "SUB_DEPARTMENT",
        "ADP_PERSON_TYPE"
        )
        )  stg, xxdbd_learn_user usr
                ,XXDBD.XXDBD_LEARN_CUSTOM_FIELDS fields
        where stg.oracle_id=usr.oracle_id
           and field_name=upper(fields.name) 
           and usr.membership_id= c_mship_id
           and usr.ACCOUNT_EXPIRATION is null
           and usr.status IN ('PROCESSED','UPDATED') 
           minus
           select field_id, field_value from xxdbd.xxdbd_learn_custom_field_val where membership_id= c_mship_id
           ;

        begin
                                            
                for field_row in csr_changed_fields(p_membership_id) loop
                    ins_or_upd_field_vals(p_membership_id, field_row.id, field_row.value);
                end loop;

             COMMIT;
        end process_custom_fields;
        
procedure ins_or_upd_field_vals(p_mship_id in number, p_field_id in number, p_value in varchar2) is

v_fval varchar2(240); 
cursor csr_fval_exists(c_mship_id number, c_field_id number) is
select field_value from xxdbd.xxdbd_learn_custom_field_val where MEMBERSHIP_ID=c_mship_id and FIELD_ID=c_field_id;

begin

    v_fval := null;
    open  csr_fval_exists(p_mship_id, p_field_id); 
    fetch csr_fval_exists into v_fval;
    close csr_fval_exists;

    if v_fval is null then    
        insert into xxdbd_learn_custom_field_val(membership_id, field_id, field_value, status) values(p_mship_id, p_field_id, p_value, 'NEW'); 
    elsif nvl(v_fval,'x') <> nvl(p_value,'x') THEN
        update xxdbd_learn_custom_field_val set field_value=p_value, status='UPDATED' where  MEMBERSHIP_ID=p_mship_id and FIELD_ID=p_field_id and id is not null;
    end if;
end  ins_or_upd_field_vals;        


PROCEDURE process_supervisors is
    cursor csr_sup is
        select distinct usr.oracle_id,  
                  usr.membership_id
          from XXDBD_FSN_LEARN_STG_TBL stg, 
                  xxdbd_learn_user usr
        where (stg.SUPERVISOR_ID=usr.oracle_id or stg.HR_PARTNER_ID = usr.oracle_id or stg.learning_mentor_id =  usr.oracle_id or stg.MATRIX_MANAGER_ID =usr.oracle_id) 
            and usr.status IN ('PROCESSED','UPDATED') 
            and usr.ACCOUNT_EXPIRATION IS NULL           
            and usr.membership_id is not null
            and not exists (select oracle_id from xxdbd_learn_sup_accounts sup where status in ('NEW', 'PROCESSED') and sup.oracle_id=usr.oracle_id) 
            ;
            
           cursor csr_del_sup is
           select oracle_id from xxdbd_learn_sup_accounts sup where status='PROCESSED' 
           minus
            select usr.oracle_id
              from XXDBD_FSN_LEARN_STG_TBL stg, 
                      xxdbd_learn_user usr
            where (stg.SUPERVISOR_ID=usr.oracle_id or stg.HR_PARTNER_ID = usr.oracle_id or stg.learning_mentor_id =  usr.oracle_id or stg.MATRIX_MANAGER_ID =usr.oracle_id) 
                and usr.status IN ('PROCESSED','UPDATED') 
                and usr.ACCOUNT_EXPIRATION IS NULL           
                and usr.membership_id is not null
            ;     
begin

    for rec_sup in csr_sup loop
        insert into xxdbd.xxdbd_learn_sup_accounts(MEMBERSHIP_ID, ORACLE_ID, STATUS) values(rec_sup.membership_id, rec_sup.oracle_id, 'NEW');
    end loop;
    
    for rec_sup in csr_del_sup loop
        update xxdbd.xxdbd_learn_sup_accounts set status='EXPIRED' where oracle_id=rec_sup.oracle_id;
    end loop;
    
    commit;
    
end process_supervisors;


PROCEDURE process_supervisees is

        cursor csr_supsee is
        select  usr.user_id,
               usr.oracle_id,  
               stg.supervisor_id sup_id,
               sup.sup_account_id,
               'MGR' sup_type
      from XXDBD_FSN_LEARN_STG_TBL stg, 
              xxdbd_learn_user usr,
              xxdbd_learn_sup_accounts sup
    where stg.oracle_id=usr.oracle_id 
        and usr.status IN ('PROCESSED','UPDATED') 
        and usr.ACCOUNT_EXPIRATION IS NULL           
        and usr.user_id is not null
        and stg.supervisor_id=sup.oracle_id
        and sup.sup_account_id is not null
        and sup.status = 'PROCESSED'
    union
    select  usr.user_id,
               usr.oracle_id,  
               stg.hr_partner_id sup_id,
               sup.sup_account_id,
               'HRP' sup_type
      from XXDBD_FSN_LEARN_STG_TBL stg, 
              xxdbd_learn_user usr,
              xxdbd_learn_sup_accounts sup
    where stg.oracle_id=usr.oracle_id 
        and usr.status IN ('PROCESSED','UPDATED') 
        and usr.ACCOUNT_EXPIRATION IS NULL           
        and usr.user_id is not null
        and stg.hr_partner_id=sup.oracle_id
        and sup.sup_account_id is not null
        and sup.status = 'PROCESSED'
    union
    select  usr.user_id,
               usr.oracle_id,  
               stg.learning_mentor_id sup_id,
               sup.sup_account_id,
               'LMTR' sup_type
      from XXDBD_FSN_LEARN_STG_TBL stg, 
              xxdbd_learn_user usr,
              xxdbd_learn_sup_accounts sup
    where stg.oracle_id=usr.oracle_id 
        and usr.status IN ('PROCESSED','UPDATED') 
        and usr.ACCOUNT_EXPIRATION IS NULL           
        and usr.user_id is not null
        and stg.learning_mentor_id=sup.oracle_id
        and sup.sup_account_id is not null
        and sup.status = 'PROCESSED'
    union
    select  usr.user_id,
               usr.oracle_id,  
               stg.matrix_manager_id sup_id,
               sup.sup_account_id,
               'MTRX' sup_type
      from XXDBD_FSN_LEARN_STG_TBL stg, 
              xxdbd_learn_user usr,
              xxdbd_learn_sup_accounts sup
    where stg.oracle_id=usr.oracle_id 
        and usr.status IN ('PROCESSED','UPDATED') 
        and usr.ACCOUNT_EXPIRATION IS NULL           
        and usr.user_id is not null
        and stg.matrix_manager_id=sup.oracle_id   
        and sup.sup_account_id is not null
        and sup.status = 'PROCESSED'
        ;
    
CURSOR csr_del_supsee is
        select oracle_id, supervisor_id from xxdbd.xxdbd_learn_supervisees supsee
        where status='PROCESSED'
        minus
        (select oracle_id, supervisor_id from xxdbd.xxdbd_fsn_learn_stg_tbl where supervisor_id is not null
        union
        select oracle_id, hr_partner_id from xxdbd.xxdbd_fsn_learn_stg_tbl where hr_partner_id is not null
        union
        select oracle_id, learning_mentor_id from xxdbd.xxdbd_fsn_learn_stg_tbl where learning_mentor_id is not null
        union
        select oracle_id, matrix_manager_id from xxdbd.xxdbd_fsn_learn_stg_tbl where matrix_manager_id is not null
        )
    ;    
    i number :=0;
begin

        for rec_supsee in csr_supsee loop
            
            delete from xxdbd.xxdbd_learn_supervisees where user_id=rec_supsee.user_id and sup_account_id=rec_supsee.sup_account_id and status='DELETED';
             
            insert into xxdbd.xxdbd_learn_supervisees(USER_ID,SUP_ACCOUNT_ID,ORACLE_ID,SUPERVISOR_ID,SUP_TYPE,STATUS) 
            select rec_supsee.user_id, rec_supsee.sup_account_id, rec_supsee.oracle_id, rec_supsee.sup_id, rec_supsee.sup_type, 'NEW' from dual
            where not exists (select null from xxdbd.xxdbd_learn_supervisees where  status in ('NEW', 'PROCESSED') and oracle_id= rec_supsee.oracle_id and supervisor_id=rec_supsee.sup_id);
            
            i := i+1;
                
            if i mod 100 = 0 then
                commit;
            end if;
                
        end loop;
        
        for rec_supsee in csr_del_supsee loop        
            update  xxdbd.xxdbd_learn_supervisees set status='EXPIRED' where oracle_id= rec_supsee.oracle_id and supervisor_id=rec_supsee.supervisor_id;
        end loop;
        
        commit;
        
end process_supervisees;    


    PROCEDURE insert_person_actions (
       p_per_actn_id_array   IN   g_number_type,
      p_per_id              IN   g_ID_type,
      p_benefit_action_id   IN   NUMBER
   )
   IS
      l_num_rows   NUMBER := p_per_actn_id_array.COUNT;
   BEGIN
      g_proc := 'insert_person_actions';

      FORALL l_count IN 1 .. p_per_actn_id_array.COUNT

         INSERT INTO ben_person_actions
                     (person_action_id,
                      person_id,
                      benefit_action_id,
                      action_status_cd,
                      non_person_cd,
                      object_version_number)
         VALUES (
                      p_per_actn_id_array (l_count),
                     -1,
                      p_benefit_action_id,
                      'U',
                      p_per_id (l_count),
                      1);

        INSERT INTO ben_batch_ranges
                  (range_id,
                   benefit_action_id,
                   range_status_cd,
                   starting_person_action_id,
                   ending_person_action_id,
                   object_version_number)
        VALUES   (
                   ben_batch_ranges_s.NEXTVAL,
                   p_benefit_action_id,
                   'U',
                   p_per_actn_id_array (1),
                   p_per_actn_id_array (l_num_rows),
                   1);


   END insert_person_actions;
  
   
   procedure insert_actions(p_object_type in varchar2, p_request_id in number) is
   
         l_benefit_action_id       NUMBER;
      l_object_version_number   NUMBER;      
      l_num_ranges              NUMBER                              := 0;
      l_num_persons             NUMBER                              := 0;
      l_silent_error            EXCEPTION;
      l_num_rows                NUMBER                              := 0;
      l_person_action_ids       g_number_type             := g_number_type();
      l_person_ids              g_ID_type             := g_ID_type();
      p_proc varchar2(240) := 'insert_actions';
      
      cursor csr_person is
      select oracle_id id from xxdbd_learn_user where STATUS <> 'PROCESSED'   and  p_object_type='U'
      UNION
      select to_char(user_id) id from xxdbd_learn_user where membership_id is null and user_id is not null and status='PROCESSED' and ACCOUNT_EXPIRATION is null  and  p_object_type='M'
      UNION
      select DISTINCT TO_CHAR(MEMBERSHIP_ID) id from XXDBD_LEARN_SUP_ACCOUNTS  where STATUS <> 'PROCESSED'   and  p_object_type='A'
      UNION
      select oracle_id id from xxdbd_learn_sup_accounts where sup_account_id is not null and status='EXPIRED'  and  p_object_type='ES'
      UNION
      SELECT DISTINCT oracle_id  id FROM XXDBD_LEARN_SUPERVISEES where STATUS <> 'PROCESSED'   and  p_object_type='S'
      UNION
      SELECT DISTINCT TO_CHAR(MEMBERSHIP_ID) id FROM XXDBD_LEARN_CUSTOM_FIELD_VAL where STATUS <> 'PROCESSED'   and  p_object_type='C'
      UNION
      select TO_CHAR(ID) ID  from xxdbd_learn_custom_field_val where status='UPDATED'   and  p_object_type='UC'
      UNION
       select TO_CHAR(membership_id) ID from xxdbd_learn_user where trim(membership_id) is not null   and  p_object_type='PC'
      ;
    
   begin
         g_benefit_action_id := null;
         
         fnd_file.put_line(fnd_file.log,p_proc||' : '||to_char(sysdate,'DD-MON-RRRR HH24:MI:SS'));
         
        delete from BEN_BATCH_RANGES where BENEFIT_ACTION_ID in (select BENEFIT_ACTION_ID from ben_benefit_actions where process_date < sysdate -30 and bft_attribute1 = 'LEARN');
        delete from BEN_PERSON_ACTIONS where BENEFIT_ACTION_ID in (select BENEFIT_ACTION_ID from ben_benefit_actions where process_date < sysdate -30 and bft_attribute1 = 'LEARN');
        delete from ben_benefit_actions where process_date < sysdate -30 and bft_attribute1 = 'LEARN';
         
         fnd_file.put_line(fnd_file.log,p_proc||': ' || p_object_type||' : '||to_char(sysdate,'DD-MON-RRRR HH24:MI:SS'));
         
         ben_benefit_actions_api.create_perf_benefit_actions
                         (p_benefit_action_id           => l_benefit_action_id,
                          p_process_date                => trunc(sysdate),
                          p_mode_cd                     => 'W',
                          p_derivable_factors_flag      => 'NONE',
                          p_validate_flag               => 'N',
                          p_debug_messages_flag         => 'N' ,
                          p_business_group_id           => 1,
                          p_no_programs_flag            => 'N',
                          p_no_plans_flag               => 'N',
                          p_audit_log_flag              => 'N',
                          p_pgm_id                     => -100,
                          p_person_id                   => NULL,
                          p_object_version_number       => l_object_version_number,
                          p_effective_date              => trunc(sysdate),
                          p_request_id                  => p_request_id, --fnd_global.conc_request_id,
                          p_program_application_id      => fnd_global.prog_appl_id,
                          p_program_id                  => fnd_global.conc_program_id,
                          p_program_update_date         => SYSDATE,
                          p_bft_attribute1              => 'LEARN',
                          p_bft_attribute3              =>p_object_type
     );

      fnd_file.put_line(fnd_file.log,'Benefit Action Id is ' || l_benefit_action_id);
      
      benutils.g_benefit_action_id := l_benefit_action_id;
      g_benefit_action_id := l_benefit_action_id;

        for rec_per in csr_person loop

                    l_num_rows := l_num_rows + 1;
                    l_num_persons := l_num_persons + 1;
                    l_person_action_ids.EXTEND (1);
                    l_person_ids.EXTEND (1);

              SELECT ben_person_actions_s.NEXTVAL
                      INTO l_person_action_ids (l_num_rows)
                      FROM DUAL;

                    l_person_ids (l_num_rows) := rec_per.id;
                    
                     IF l_num_rows = 10 THEN
                       l_num_ranges := l_num_ranges + 1;

                       insert_person_actions
                                         (p_per_actn_id_array      => l_person_action_ids,
                                          p_per_id                 => l_person_ids,
                                          p_benefit_action_id      => l_benefit_action_id
                                         );
                       l_num_rows := 0;
                       l_person_action_ids.DELETE;
                       l_person_ids.DELETE;
                    END IF;
                    
        end loop;
        
        l_num_ranges := l_num_ranges + 1;
        
            if l_num_rows > 0 then
                           insert_person_actions
                                             (p_per_actn_id_array      => l_person_action_ids,
                                              p_per_id                 => l_person_ids,
                                              p_benefit_action_id      => l_benefit_action_id
                                             );
                           l_num_rows := 0;
                           l_person_action_ids.DELETE;
                           l_person_ids.DELETE;
            end if;
   commit;
   end;
   
   
   PROCEDURE do_multithread (
  errbuf                OUT NOCOPY      VARCHAR2,
  retcode               OUT NOCOPY      NUMBER,
  p_proc                 in varchar2,
  p_benefit_action_id   in number,
  p_request_id             in number
  ) is
  
      l_range_id                 NUMBER;
      l_record_number            NUMBER                := 0;
      l_start_person_action_id   NUMBER                := 0;
      l_end_person_action_id     NUMBER                := 0;
      
 CURSOR c_range_for_thread (v_benefit_action_id IN NUMBER)
     IS
      SELECT  ran.range_id, ran.starting_person_action_id,
                    ran.ending_person_action_id
      FROM ben_batch_ranges ran
      WHERE ran.range_status_cd = 'U'
      AND ran.benefit_action_id = v_benefit_action_id
      AND ROWNUM < 2
      FOR UPDATE OF ran.range_status_cd;

     CURSOR c_person_for_thread (
         v_benefit_action_id        IN   NUMBER,
         v_start_person_action_id   IN   NUMBER,
         v_end_person_action_id     IN   NUMBER
     )
     IS
      SELECT   ben.non_person_cd, ben.person_action_id, ben.object_version_number
      FROM ben_person_actions ben
      WHERE ben.benefit_action_id = v_benefit_action_id
      AND ben.action_status_cd <> 'P'
      AND ben.person_action_id BETWEEN v_start_person_action_id
                               AND v_end_person_action_id
      ORDER BY ben.person_action_id;
      l_request_id number;     
 begin
 
 
 g_request_id         :=p_request_id;
 g_benefit_action_id := p_benefit_action_id;
 l_request_id := FND_GLOBAL.CONC_REQUEST_ID;
 
     fnd_file.put_line(fnd_file.log,'g_request_id '||g_request_id);
     fnd_file.put_line(fnd_file.log,'g_benefit_action_id '||g_benefit_action_id);
     
     fnd_file.put_line(fnd_file.log,p_proc||' : '||to_char(sysdate,'DD-MON-RRRR HH24:MI:SS'));

         LOOP
         
                 begin
                        OPEN c_range_for_thread (g_benefit_action_id);
                         FETCH c_range_for_thread
                          INTO l_range_id, l_start_person_action_id, l_end_person_action_id;
                         EXIT WHEN c_range_for_thread%NOTFOUND;
                        CLOSE c_range_for_thread;


                        IF (l_range_id IS NOT NULL)
                        THEN
                            fnd_file.put_line(fnd_file.log,   'Range with range_id '
                                     || l_range_id
                                     || ' with Starting person action id '
                                     || l_start_person_action_id
                                    );
                           fnd_file.put_line (fnd_file.log,   ' and Ending Person Action id '
                                     || l_end_person_action_id
                                     || ' is selected'
                                    );
                                    
                            UPDATE ben_batch_ranges ran
                               SET ran.range_status_cd = 'P'
                             WHERE ran.range_id = l_range_id;
                
                            COMMIT;
                            
                            END IF; 
                        exception when others then
                                                             
                             fnd_file.put_line(fnd_file.log,'Error updating range : '||sqlerrm);
                        end;

                        OPEN c_person_for_thread (g_benefit_action_id,
                                   l_start_person_action_id,
                                   l_end_person_action_id
                                 );
                    l_record_number := 0;
                    LOOP
                      FETCH c_person_for_thread
                        INTO g_cache_person_process (l_record_number + 1).non_person_cd,
                              g_cache_person_process (l_record_number + 1).person_action_id,
                              g_cache_person_process (l_record_number + 1).object_version_number;
                                                            
                      EXIT WHEN c_person_for_thread%NOTFOUND;
                      l_record_number := l_record_number + 1;
                    END LOOP;
                    CLOSE c_person_for_thread;

                FOR l_cnt IN 1 .. l_record_number
                LOOP                                      
                        update ben_person_actions set  action_status_cd = 'P', chunk_number=l_request_id where  person_action_id=g_cache_person_process (l_cnt).person_action_id ;               
                        execute immediate 'BEGIN xxdbd_learn_rest_process_pkg.'|| p_proc||'(:p_membership_id); END; ' USING g_cache_person_process (l_cnt ).non_person_cd;
                END LOOP;                 
                  
                COMMIT;
          
          END LOOP;
    
    commit;

 end do_multithread;     



procedure submit_request(p_threads in number, p_proc in varchar2) is
l_request_id number;
begin

 g_request_id         := FND_GLOBAL.CONC_REQUEST_ID;
 
      ben_batch_utils.g_num_processes := 0;
     ben_batch_utils.g_processes_tbl.DELETE;
     
           fnd_file.put_line(fnd_file.log,   'Time before launching the threads '
               || TO_CHAR (SYSDATE, 'yyyy/mm/dd:hh:mi:ssam'));
     
for i in 1..p_threads loop
 
  l_request_id :=
               fnd_request.submit_request (application      => 'XXDBDPER',
                                           program          => 'XXDBD_LEARN_CUSTOM_FV_MT',
                                           description      => NULL,
                                           sub_request      => FALSE,
                                           argument1        => p_proc,
                                           argument2        => g_benefit_action_id,
                                           argument3        => g_request_id
                                          );
            ben_batch_utils.g_num_processes :=
                                           ben_batch_utils.g_num_processes + 1;
            ben_batch_utils.g_processes_tbl (ben_batch_utils.g_num_processes) :=
                                                                  l_request_id;
   commit;  
end loop;
           
ben_batch_utils.check_all_slaves_finished (p_rpt_flag => TRUE);

end;
   
   function get_benefit_action_id return number is
   begin
        return g_benefit_action_id;
   end get_benefit_action_id;
   
end xxdbd_learn_rest_process_pkg;
/
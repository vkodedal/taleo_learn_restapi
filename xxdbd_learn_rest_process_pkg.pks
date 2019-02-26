CREATE OR REPLACE package  xxdbd_learn_rest_process_pkg is
--
-- Change History 
-------------------------------------------------------------------------------------------------------------
-- Version     Changed By                       Change Date                  Comments
-------------------------------------------------------------------------------------------------------------
-- 1.1         Vidyadhar Kodedala             15-Jun-2018            Initial Creation 

TYPE g_ID_type IS VARRAY (200) OF varchar2(80);
TYPE g_number_type IS VARRAY (200) OF number;

procedure main(p_errbuff out varchar2, p_retcode out number);

function get_benefit_action_id return number ;

function get_supervisor_flag(p_emp_num in varchar2) return varchar2;

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
                                                    ,p_account_expiration IN VARCHAR2 DEFAULT NULL
                                                    ,p_user_id            IN NUMBER DEFAULT NULL
                                                    ,p_status             IN VARCHAR2 
                                                     ); 

procedure update_flags;

procedure process_users;

procedure process_custom_fields(p_membership_id in number) ;

procedure process_custom_fields_MT;

PROCEDURE process_supervisors;

PROCEDURE process_supervisees;

procedure ins_or_upd_field_vals(p_mship_id in number, p_field_id in number, p_value in varchar2);

PROCEDURE insert_person_actions (
       p_per_actn_id_array   IN   g_number_type,
      p_per_id              IN   g_ID_type,
      p_benefit_action_id   IN   NUMBER
);

procedure insert_actions(p_object_type in varchar2, p_request_id in number);

procedure submit_request(p_threads in number, p_proc in varchar2);

PROCEDURE do_multithread (
  errbuf                OUT NOCOPY      VARCHAR2,
  retcode               OUT NOCOPY      NUMBER,
  p_proc                 in varchar2,
  p_benefit_action_id   in number,
  p_request_id             in number
  );
                                                  
end xxdbd_learn_rest_process_pkg;
/
whenever sqlerror continue
drop table xxdbd.xxdbd_learn_supervisees;
create table xxdbd.xxdbd_learn_supervisees(
USER_ID NUMBER,
SUP_ACCOUNT_ID VARCHAR2(60),
ORACLE_ID VARCHAR2(30),
SUPERVISOR_ID VARCHAR2(30),
SUP_TYPE VARCHAR2(30),
STATUS VARCHAR2(30),
Creation_date date,
Last_update_date date
);

drop synonym xxdbd_learn_supervisees;
create synonym xxdbd_learn_supervisees for xxdbd.xxdbd_learn_supervisees;

CREATE OR REPLACE TRIGGER xxdbd_learn_supervisees_WHO before insert or update ON xxdbd.xxdbd_learn_supervisees for each row
declare
 l_sysdate DATE := sysdate;
begin

  if inserting and  
      :new.creation_date is null then
    :new.creation_date   := l_sysdate;
  end if;
  :new.last_update_date  := l_sysdate;

end;

/
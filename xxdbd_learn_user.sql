whenever sqlerror continue
drop table xxdbd.xxdbd_learn_user;
create table xxdbd.xxdbd_learn_user(
Oracle_ID varchar2(30),
First_Name varchar2(150),
Last_Name varchar2(150),
Gender varchar2(10),
HIRE_DATE varchar2(30),
JOB_TITLE varchar2(240),
Home_City varchar2(240),
Home_State varchar2(240),
Home_Zip varchar2(30),
Home_Country varchar2(240),
Email varchar2(240),
Account_Expiration varchar2(240),
USER_ID Number,
Membership_ID Number,
STATUS VARCHAR2(30),
Creation_date date,
Last_update_date date
);

drop synonym xxdbd_learn_user;
create synonym xxdbd_learn_user for xxdbd.xxdbd_learn_user;
create index xxdbd_learn_user_pk on xxdbd.xxdbd_learn_user(Oracle_ID);

CREATE OR REPLACE TRIGGER xxdbd_learn_user_WHO before insert or update ON xxdbd.xxdbd_learn_user for each row
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

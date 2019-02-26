whenever sqlerror continue
drop table xxdbd.xxdbd_learn_sup_accounts;
create table xxdbd.xxdbd_learn_sup_accounts(
SUP_ACCOUNT_ID VARCHAR2(60),
MEMBERSHIP_ID NUMBER,
ORACLE_ID VARCHAR2(30),
STATUS VARCHAR2(30),
Creation_date date,
Last_update_date date
);

drop synonym xxdbd_learn_sup_accounts;
create synonym xxdbd_learn_sup_accounts for xxdbd.xxdbd_learn_sup_accounts;

CREATE OR REPLACE TRIGGER xxdbd_learn_sup_accounts_WHO before insert or update ON xxdbd.xxdbd_learn_sup_accounts for each row
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
whenever sqlerror continue
drop table xxdbd.xxdbd_learn_custom_field_val;
create table xxdbd.xxdbd_learn_custom_field_val(
ID number,
FIELD_ID number,
MEMBERSHIP_ID NUMBER,
FIELD_VALUE varchar2(240),
STATUS VARCHAR2(30),
Creation_date date,
Last_update_date date
);

drop synonym xxdbd_learn_custom_field_val;
create synonym xxdbd_learn_custom_field_val for xxdbd.xxdbd_learn_custom_field_val;

create index xxdbd_learn_custom_fv_pk1 on xxdbd.xxdbd_learn_custom_field_val(FIELD_ID, MEMBERSHIP_ID);

CREATE INDEX XXDBD_LEARN_CUSTOM_FV_PK2 on xxdbd.xxdbd_learn_custom_field_val(membership_id);

CREATE OR REPLACE TRIGGER xxdbd_learn_custom_fval_WHO before insert or update ON xxdbd.xxdbd_learn_custom_field_val for each row
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
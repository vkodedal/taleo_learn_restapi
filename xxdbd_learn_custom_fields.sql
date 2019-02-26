whenever sqlerror continue

drop table xxdbd.xxdbd_learn_custom_fields;
drop synonym xxdbd_learn_custom_fields;

create table xxdbd.xxdbd_learn_custom_fields(
ID number,
Name varchar2(240),
status varchar2(30)
);

create synonym xxdbd_learn_custom_fields for xxdbd.xxdbd_learn_custom_fields;

CREATE INDEX XXDBD_LEARN_CUSTOM_FIELDS_PK1 ON XXDBD.XXDBD_LEARN_CUSTOM_FIELDS(ID);
           
CREATE INDEX XXDBD_LEARN_CUSTOM_FIELDS_PK2 ON XXDBD.XXDBD_LEARN_CUSTOM_FIELDS(UPPER(NAME));


import sys
con_str=None
THREAD_COUNT=10
DEBUG = False
PREPROCESS = True
url = 'https://xxxx.learn.taleo.net/learn.rest/v1'
p_process_type = 'ALL'

if len(sys.argv) > 1:
    con_str = sys.argv[1]
    p_target = sys.argv[2]
    p_preprocess = sys.argv[3]
    p_process_type = sys.argv[4]
    p_threads = sys.argv[5]
    p_debug = sys.argv[6]
    p_request_id = sys.argv[7]
    print('Program parameters:')
    for i in range(len(sys.argv)):
        if i != 1:
            print('sys.argv[{0}]'.format(str(i)), sys.argv[i])


if p_debug == 'Y':
    DEBUG = True

if p_threads is not None:
    THREAD_COUNT = int(p_threads)

if p_target == 'P':
    url = 'https://xxxxxx.learn.taleo.net/learn.rest/v1'

if p_preprocess == 'N':
    PREPROCESS = False

SUCCESS=1
FAILURE=0
headers = {'Content-type': 'application/json;charset=UTF-8', 'Accept': 'text/plain', 'X-Learn-Access-Token': None}

usersApi='/users'
membershipApi = '/users/{0}/memberships'
custFieldValuesApi = '/memberships/{0}/customFieldValues'
updCustFieldValuesApi = '/memberships/{0}/customFieldValues/{1}'
createMshipApi = '/memberships'
mshipApi='/memberships/{0}'
createSupApi='/learnCenters/{0}/supervisorAccounts'
delSupApi='/learnCenters/{0}/supervisorAccounts/{1}'
createSuperviseeApi='/learnCenters/{0}/supervisorAccounts/{1}/supervisees'
selSuperviseeApi='/learnCenters/{0}/supervisorAccounts/{1}/supervisees/{2}'
limit='?limit=50'
searchUser="?q={username:{$eq: '%s'}}"
userName="WEBAPI_UserIntegration"
password="xxxxxxx"
usr_query="""
select ORACLE_ID,FIRST_NAME,LAST_NAME,GENDER,HIRE_DATE,JOB_TITLE,HOME_CITY,HOME_STATE,HOME_ZIP,HOME_COUNTRY,EMAIL, USER_ID,  to_char(sysdate, 'YYYY-MM-DD"T"HH24:MI:SS') today, status from xxdbd.xxdbd_learn_user where status IN ('NEW', 'UPDATED', 'EXPIRED') AND ORACLE_ID = '{0}'
"""

membership_query="""
select user_id from xxdbd_learn_user where membership_id is null and user_id is not null and status='PROCESSED' and ACCOUNT_EXPIRATION is null
"""
mship_json_string = """
{{
    "learnCenterId": "{0}",
    "userId": "{1}",
    "status": "{2}"
}}
"""
new_memberships_query="""
select distinct membership_id from xxdbd_learn_custom_field_val where status='NEW'
"""
new_fvals_query="""
select field_id, field_value from xxdbd_learn_custom_field_val where status='NEW' and membership_id={0}
"""
upd_fvals_query="""
select ID, membership_id, field_value from xxdbd_learn_custom_field_val where status='UPDATED' AND ID = {0}
"""
fval_json_string = """
{{
    "customFieldId": "{0}",
    "value": "{1}"
}}
"""
upd_fval_json_string = """
{{
    "value": "{0}"
}}
"""
sup_query="""
select membership_id, oracle_id from xxdbd_learn_sup_accounts where status='NEW'
"""

sup_noaccount_query="""
select membership_id from xxdbd_learn_sup_accounts where sup_account_id is null and membership_id is not null
"""
sup_json_string = """
{{
    "membershipId": "{0}"
}}
"""
exp_sup_query="""
select sup_account_id, oracle_id from xxdbd_learn_sup_accounts where sup_account_id is not null and status='EXPIRED' and oracle_id='{0}'
"""
supervisee_query="""
select user_id, sup_account_id, status,
(SELECT FIRST_NAME||' '||LAST_NAME FROM  XXDBD.XXDBD_LEARN_USER U WHERE U.ORACLE_ID=S.SUPERVISOR_ID AND ROWNUM=1) MGR_NAME, SUP_TYPE 
from xxdbd_learn_supervisees s where status in ('NEW', 'EXPIRED') and oracle_id = '{0}'
"""
supervisee_json_string = """
{{
    "userId": "{0}"
}}
"""

report_query='''
select lkp.meaning object_type, bpa.NON_PERSON_CD id, action_text, chunk_number thread_id
  from apps.ben_benefit_actions bba, 
          apps.ben_person_actions bpa,
          apps.fnd_lookup_values lkp 
where bba.request_id={0} 
   and bba.benefit_action_id=bpa.benefit_action_id 
   and bpa.ACTION_STATUS_CD='E'
   and bba.bft_attribute3=lkp.lookup_code
   and lkp.lookup_type='DBD_LEARN_PRC_TYPE'
'''

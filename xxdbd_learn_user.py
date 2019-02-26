import cx_Oracle
import json
import re
import datetime
import traceback
import xxdbd_learn_config as cfg
from xxdbd_learn_session import getSession
from xxdbd_learn_utl import *

class User:
    db = None

    errorMsg = None

    def __init__(self, db):
        self.db = db


    user_json_string = """
    {{
      "username": "{0}",
      "firstName": "{1}",
      "lastName": "{2}",
      "gender":"{3}",
      "hireDate":"{4}",
      "jobTitle": "{5}",
      "city":"{6}",
      "state":"{7}",
      "postalCode": "{8}",
      "country": "{9}",
      "email": "{10}",
      "passwordExpiryDate":"{11}",
      "includeInSearchFlag":true,
      "validUntilDate": "None"
    }}
    """

    mship_json_string = """
    {{
      "status": "{0}"
    }}
    """

    term_json_string = """
    {{
      "validUntilDate": "{0}"
    }}
    """

    sup_json_string = """
        {{
          "managerName": "{0}"
        }}
        """

    def findUser(self, oracleId):
        s = getSession()
        searchURL = cfg.url + cfg.usersApi + cfg.searchUser % (str(oracleId))
        if cfg.DEBUG:
            print('Searching for Oracle Id ', str(oracleId), searchURL)
        try:
            r = s.get(searchURL, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if r.ok:
                jData = json.loads(r.content)
                items = jData['items']
                count = jData['count']
                if count == 1:
                    return items[0]
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            print(traceback.format_exc())
        return None

    def createUser(self, row):
        try:
            s = getSession()
            restURL = cfg.url + cfg.usersApi
            if cfg.DEBUG:
                print('Creating User account for Oracle Id ', row[0])
            v_email = row[10]

            if v_email is not None and len(v_email) > 50:
                v_email = v_email[1:37]+'@DONOTUSE.COM'

            if v_email is None or ' ' in v_email or not re.match(r"[^@]+@[^@]+\.[^@]+", v_email):
                v_email = row[1].replace(' ','')+'.'+row[2].replace(' ','')+'@DONOTUSE.COM'
                self.errorMsg = 'Invalid Email.'
            jData = self.user_json_string.format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                            row[9], v_email, datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            if cfg.DEBUG:
                print(restURL)
            jsjData = json.loads(jData.encode('utf-8'), object_pairs_hook=dict_clean)
            r = s.post(restURL, data=None, json=jsjData, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if r.ok:
                jData = json.loads(r.content)
                return jData, self.errorMsg
            else:
                errortxt = json.loads(r.content)
                self.errorMsg = 'User creation failed.'+' '+ str(row[0]) +' '+ str(r.status_code) +' '+ errortxt['title']
        except Exception as x:
            self.errorMsg = 'Exception :('+ x.__class__.__name__
            print(traceback.format_exc())
        if self.errorMsg is not None:
            print(self.errorMsg.encode('utf-8').decode('utf-8'))
        return None, self.errorMsg

    def updateUser(self, row):
        try:
            s = getSession()
            v_oracle_id = row[0]
            v_email = row[10]
            v_user_id = row[11]
            v_sysdate = row[12]
            if v_email is None or ' ' in v_email or len(v_email) > 50 or not re.match(r"[^@]+@[^@]+\.[^@]+", v_email):
                v_email = 'invalid_email@dieboldnixdorf.com'
            restURL = cfg.url + cfg.usersApi + '/' + str(v_user_id)
            membershipId = -1
            membershipStatus = None
            if v_user_id is not None:
                membershipId, membershipStatus = self.findMembership(v_user_id)
            else:
                print('Missing User Id for oracle Id ', v_oracle_id)

            if row[13] == 'EXPIRED':
                if cfg.DEBUG:
                    print('Terminating Oracle Id ', v_oracle_id, restURL)
                jData = self.term_json_string.format(v_sysdate)
                if membershipId is not None and membershipId > 0 :
                    self.updateMembership(membershipId, 'removed')
                else:
                    print('Missing membership Id for oracle Id ', v_oracle_id)
            else:
                if cfg.DEBUG:
                    print('Updating Oracle Id ', v_oracle_id, restURL)
                jData = self.user_json_string.format(v_oracle_id, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                                row[9],
                                                     v_email, None)
                if membershipStatus is not None and membershipStatus == 'removed':
                    self.updateMembership(membershipId, 'approved')

            jsjData = json.loads(jData.encode('utf-8'), object_pairs_hook=dict_clean)
            r = s.patch(restURL, data=None, json=jsjData, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if r.ok:
                jData = json.loads(r.content, object_pairs_hook=dict_clean)
                if cfg.DEBUG:
                    print('User Updated.')
                return jData, self.errorMsg
            else:
                errortxt = json.loads(r.content)
                self.errorMsg = 'User not Updated. Failed with status code :'+ str(r.status_code)+' '+ errortxt['title']+' '+ str(row[0])
        except Exception as x:
            self.errorMsg = 'Exception :(' + x.__class__.__name__
            print(traceback.format_exc())
        print(self.errorMsg.encode('utf-8').decode('utf-8'))
        return None, self.errorMsg

    def updateManagerName(self, user_id, manager_name):
        try:
            s = getSession()
            restURL = cfg.url + cfg.usersApi + '/' + str(user_id)
            jData = self.sup_json_string.format(manager_name).encode('utf-8')
            r = s.patch(restURL, data=jData, json=None, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if r.ok:
                if cfg.DEBUG:
                    print('Supervisor name updated for user ', user_id, manager_name)
                return cfg.SUCCESS, self.errorMsg
            else:
                errortxt = json.loads(r.content)
                self.errorMsg = 'Supervisor name update failed ' + str(user_id) +' '+ manager_name+ ' ' + errortxt['title']
        except Exception as x:
            self.errorMsg = 'Exception :(' + x.__class__.__name__
            print(traceback.format_exc())
        print(self.errorMsg.encode('utf-8') )
        return None, self.errorMsg

    def updateUserDB(self, Oracle_id, user):

        try:
            learn_user_id = user['id']
            learn_oracle_id = Oracle_id
            if 'firstName' in user:
                learn_first_name = user['firstName']
            else:
                learn_first_name = None
            if 'lastName' in user:
                learn_last_name = user['lastName']
            else:
                learn_last_name = None
            if 'gender' in user:
                learn_gender = user['gender']
            else:
                learn_gender = None
            if 'hireDate' in user:
                learn_hire_date = user['hireDate']
            else:
                learn_hire_date = None
            if 'jobTitle' in user:
                learn_job_title = user['jobTitle']
            else:
                learn_job_title = None
            if 'city' in user and user['city'] != 'None':
                learn_home_city = user['city']
            else:
                learn_home_city = ''
            if 'state' in user and user['state'] != 'None':
                learn_home_state = user['state']
            else:
                learn_home_state = None
            if 'postalCode' in user and user['postalCode'] != 'None':
                learn_home_zip = user['postalCode']
            else:
                learn_home_zip = None
            if 'country' in user:
                learn_home_country = user['country']
            else:
                learn_home_country = None
            if 'email' in user:
                learn_email = user['email']
            else:
                learn_email = None
            learn_status = 'PROCESSED'
            if 'validUntilDate' in user:
                learn_account_exp = user['validUntilDate']
            else:
                learn_account_exp = None
            named_params = [learn_oracle_id,learn_first_name,learn_last_name,learn_gender,learn_hire_date,learn_job_title,
                            learn_home_city,learn_home_state,learn_home_zip,learn_home_country,learn_email,learn_account_exp,
                            learn_user_id,learn_status]

            self.db.executeProcedure('xxdbd_learn_rest_process_pkg.insert_or_update_user', named_params)
            if cfg.DEBUG:
                print('User updated in DB. Oracle Id :', learn_oracle_id, learn_user_id)
        except Exception as x:
                print('Exception :(', x.__class__.__name__)
                print(x.args, Oracle_id)

    def updateMembershipDB(self, userId, membershipId):

        try:
            self.db.executeStmt('update xxdbd.xxdbd_learn_user set membership_id=:m where user_id=:u', {'m':membershipId,'u':userId})
            if cfg.DEBUG:
                print('Membership updated for user ',userId)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(error.code)
            print(error.message)
            print(error.context)

    def createMembership(self, userId):
        jData = cfg.mship_json_string.format(178410, userId, 'approved')
        restURL = cfg.url+cfg.createMshipApi
        if cfg.DEBUG:
            print(restURL)
        try:
            s = getSession()
            r = s.post(restURL, data=jData, json=None, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if (r.ok):
                jData = json.loads(r.content)
                if 'id' in jData:
                    if cfg.DEBUG:
                        print('Membership created.')
                    membershipId = jData['id']
                    return membershipId, self.errorMsg
                else:
                    self.errorMsg = 'Membership creation failed.'
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            error, = x.args
            self.errorMsg = error
        print(self.errorMsg)
        return None, self.errorMsg

    def updateMembership(self, mshipId, status):
        restURL = cfg.url + cfg.mshipApi.format(str(mshipId))
        jData = self.mship_json_string.format(status)
        if cfg.DEBUG:
            print('Update membership ', str(mshipId))
        try:
            s = getSession()
            r = s.patch(restURL, data=jData, json=None, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if r.ok:
                if cfg.DEBUG:
                    print('Membership status updated - ', mshipId, 'to',status )
                return cfg.SUCCESS, self.errorMsg
            else:
                errortxt = json.loads(r.content)
                self.errorMsg = 'Membership status update failed ' + str(mshipId) + ' ' + status + ' ' + errortxt['title']

        except Exception as x:
            self.errorMsg = 'Exception :(' + x.__class__.__name__
            print(traceback.format_exc())
        print(self.errorMsg.encode('utf-8'))
        return None, self.errorMsg

    def findMembership(self, userId):
        restURL = cfg.url + cfg.membershipApi.format(str(userId))
        if cfg.DEBUG:
            print('Searching membership of user', str(userId))
        try:
            s = getSession()
            r = s.get(restURL, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            membershipId = None
            if r.ok:
                jData = json.loads(r.content)
                items = jData['items']
                count = jData['count']

                if count > 0:
                    for x in items:
                        if x['learnCenterId'] == 178410:
                            membershipId = x['id']
                            membershipStatus =x['status']
                            return membershipId, membershipStatus
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            print(traceback.format_exc())
        return None, None
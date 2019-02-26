import cx_Oracle
import json
import traceback
import xxdbd_learn_config as cfg
from xxdbd_learn_session import getSession




class Supervisee:

    db = None
    errorMsg = None

    def __init__(self, db):
        self.db = db

    def createSupervisee(self, user_id, sup_account_id):
        s = getSession()
        restURL = cfg.url + cfg.createSuperviseeApi.format(str(178410), str(sup_account_id))
        jData = cfg.supervisee_json_string.format(str(user_id))
        try:
            r = s.post(restURL, data=jData, json=None, auth=(cfg.userName, cfg.password),
                        headers=cfg.headers)
            if r.ok:
                jData = json.loads(r.content)
                if 'userId' in jData:
                    if cfg.DEBUG:
                        print('Supervisee created -', str(user_id), str(sup_account_id))
                    return cfg.SUCCESS, self.errorMsg
                else:
                    self.errorMsg = 'Supervisee creation failed - '+' '+ str(user_id)+' '+ str(sup_account_id)
            elif r.status_code == 409:
                jData = json.loads(r.content)
                self.errorMsg = jData['title']
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            self.errorMsg = traceback.format_exc()
        if self.errorMsg is not None:
            print(self.errorMsg)
        return cfg.FAILURE, self.errorMsg

    def deleteSupervisee(self, user_id, sup_account_id):
        s = getSession()
        restURL = cfg.url + cfg.selSuperviseeApi.format(str(178410), str(sup_account_id), str(user_id))
        try:
            r = s.delete(restURL, auth=(cfg.userName, cfg.password))
            if r.status_code == 204:
                if cfg.DEBUG:
                    print('Supervisee deleted ', user_id, sup_account_id)
                return cfg.SUCCESS, self.errorMsg
            elif r.status_code == 404:
                print('Supervisee not found ', user_id, sup_account_id)
                return cfg.SUCCESS, self.errorMsg
            else:
                self.errorMsg = 'Error while deleting supervisee:status code:'+ str(r.status_code)+' '+ str(user_id) +' '+ str(sup_account_id)
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            error, = x.args
            self.errorMsg = error
        if self.errorMsg is not None:
            print(self.errorMsg)
        return cfg.FAILURE, self.errorMsg

    def updateSuperviseeDB(self, user_id, sup_act_id, status):
        try:
            self.db.executeStmt('update xxdbd.xxdbd_learn_supervisees set status=:s where user_id=:i and sup_account_id = :a',
                           {'i': user_id, 'a' : sup_act_id, 's' : status})
            if cfg.DEBUG:
                print('Supervisee status set to',status, 'for', user_id, sup_act_id)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(error.code)
            print(error.message)
            print(error.context)

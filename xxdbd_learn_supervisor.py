import cx_Oracle
import json
import traceback
import xxdbd_learn_config as cfg
from xxdbd_learn_session import getSession



class Supervisor:

    db = None
    errorMsg = None

    def __init__(self, db):
        self.db = db

    def findSupAccount(self, userId):
        s = getSession()
        actId = None
        restURL = cfg.url + cfg.mshipApi.format(str(userId))
        try:
            r = s.get(restURL, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if r.ok:
                jData = json.loads(r.content)
                if 'supervisorAccountId' in jData:
                    actId = jData['supervisorAccountId']
                    return actId
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            print(traceback.format_exc())
        return None

    def createSupAccount(self, userId):
        s = getSession()
        actId = None
        try:
            restURL = cfg.url + cfg.createSupApi.format(str(178410))
            jData = cfg.sup_json_string.format(userId)
            if cfg.DEBUG:
                print(restURL)
            r2 = s.post(restURL, data=jData, json=None, auth=(cfg.userName, cfg.password),
                       headers=cfg.headers)
            jData = json.loads(r2.content)
            if (r2.ok):
                if 'id' in jData:
                    if cfg.DEBUG:
                        print('Account created.')
                    actId = jData['id']
                else:
                    self.errorMsg = 'Account creation failed.'
            else:
                if 'Title' in jData:
                    self.errorMsg = jData['Title']
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            self.errorMsg = traceback.format_exc()
        if self.errorMsg is not None:
            print(self.errorMsg)
        return actId, self.errorMsg

    def delSupAccount(self, actId):
        s = getSession()
        restURL = cfg.url + cfg.delSupApi.format(str(178410), actId)
        try:
            r = s.delete(restURL, auth=(cfg.userName, cfg.password))
            if r.status_code == 204:
                if cfg.DEBUG:
                    print('Supervisor Account deleted ',actId)
                return cfg.SUCCESS, self.errorMsg
            elif r.status_code == 404:
                print('Supervisor Account not found ', actId)
                return cfg.SUCCESS, self.errorMsg
            else:
                self.errorMsg = 'Error while deleting account:status code:'+' '+  str(r.status_code)

        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            error, = x.args
            self.errorMsg = error
        if self.errorMsg is not None:
            print(self.errorMsg)
        return cfg.FAILURE, self.errorMsg

    def updSupAccountDB(self, actId, userId):
        updtStmt = 'update xxdbd.xxdbd_learn_sup_accounts set sup_account_id = :s, status=\'PROCESSED\'where membership_id=:m '
        updtParams = {'m': userId, 's': actId}

        self.db.executeStmt(updtStmt, updtParams)
        if cfg.DEBUG:
            print('Supervisor account Id updated for membership', userId)

    def delSupAccountDB(self, actId):
        updtStmt = 'update xxdbd.xxdbd_learn_sup_accounts set status=\'DELETED\' WHERE sup_account_id = :s'
        updtParams = {'s': actId}

        self.db.executeStmt(updtStmt, updtParams)
        if cfg.DEBUG:
            print('Supervisor account status set to DELETED')
import cx_Oracle
import json
import xxdbd_learn_config as cfg
from xxdbd_learn_session import getSession
import traceback

class CustomFieldValues:

    db = None
    errorMsg = None

    def __init__(self, db):
        self.db = db

    def findFieldVals(self, mshipId):
        restURL = cfg.url + cfg.custFieldValuesApi.format(str(mshipId))
        if cfg.DEBUG:
            print('Searching custom field values of memeberhsip:', str(mshipId))
        try:
            s = getSession()
            r = s.get(restURL, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if r.ok:
                jData = json.loads(r.content)
                items = jData['items']
                count = jData['count']
                if count is None:
                    count = 0
                return count, items
            else:
                print('Search failed.')
                print(r.text)
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            print(traceback.format_exc())
        return 0, None

    def createALLFieldVals(self, mshipId, fvalIds):
        cur = self.db.executeQuery(cfg.new_fvals_query.format(mshipId))
        for row in cur:
            if fvalIds is not None:
                if row[0] in fvalIds:
                    self.updateFieldValDB(mshipId, row[0], fvalIds[row[0]])
                    continue
            self.createFieldVal(mshipId, row[0], row[1])


    def createFieldVal(self, mshipId, field_id, field_value):
        restURL = cfg.url + cfg.custFieldValuesApi.format(str(mshipId))
        jData = cfg.fval_json_string.format(field_id, field_value)
        if cfg.DEBUG:
            print(restURL)
        try:
            s = getSession()
            r = s.post(restURL, data=jData, json=None, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if (r.ok):
                jData = json.loads(r.content)
                if 'id' in jData:
                    if cfg.DEBUG:
                        print('Field value created.')
                    fv_id = jData['id']
                    self.updateFieldValDB(mshipId, field_id, fv_id)
                else:
                    print('Custom field value creation failed.',str(mshipId), str(field_id), str(fv_id) )
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            error, = x.args
            print(error)


    def updateFieldVal(self, v_mship_id, v_id, v_value):
        restURL = cfg.url + cfg.updCustFieldValuesApi.format(str(v_mship_id), str(v_id))
        jData = cfg.upd_fval_json_string.format(v_value)
        error = ''
        if cfg.DEBUG:
            print('Updating custom field value:', str(v_id), str(v_mship_id), v_value)
            print(restURL)
        try:
            s = getSession()
            r = s.put(restURL, data=jData, json=None, auth=(cfg.userName, cfg.password), headers=cfg.headers)
            if r.ok:
                jData = json.loads(r.content)
                if 'id' in jData:
                    if cfg.DEBUG:
                        print('Field value updated.')
                    return v_id, None
                else:
                    print('Field value update failed.', str(v_id), str(v_mship_id), v_value)
        except Exception as x:
            print('Exception :(', x.__class__.__name__)
            error, = x.args
            print(error)
        return None, error

    def updateFieldValDB(self, mshipId, field_id, fv_id):
        try:
            self.db.executeStmt('update xxdbd.xxdbd_learn_custom_field_val set id=:i, status=\'PROCESSED\' where membership_id=:m and field_id=:f', {'i':fv_id, 'm':mshipId,'f':field_id})
            if cfg.DEBUG:
                print('Field Id updated for field ',fv_id)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(error.code)
            print(error.message)
            print(error.context)

    def updateFieldValStatusDB(self, fv_id):
        try:
            self.db.executeStmt('update xxdbd.xxdbd_learn_custom_field_val set status=\'PROCESSED\' where id=:i', {'i':fv_id})
            if cfg.DEBUG:
                print('Field value status set to PROCESSED for',fv_id)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(error.code)
            print(error.message)
            print(error.context)

import datetime
import threading
import csv
import os
import traceback
import xxdbd_learn_config as cfg
from xxdbd_learn_database import Database
from xxdbd_learn_user import User
from xxdbd_learn_cust_fvals import CustomFieldValues
from xxdbd_learn_supervisor import Supervisor
from xxdbd_learn_supervisee import Supervisee
from xxdbd_learn_session import setToken
import xxdbd_learn_multithread as mt


def startThreads(bnft_actn_id, fcall):
    threads = []
    for i in range(cfg.THREAD_COUNT):
        t1 = threading.Thread(target=getattr(mt, 'do_multithread'), args=(bnft_actn_id, fcall), name=str(i))
        # starting thread 1
        t1.start()
        threads.append(t1)

    for t in threads:
        t.join()
    print(datetime.datetime.now() - tm, datetime.datetime.now())


def processUser(oracleid, db):
    user = User(db)
    errorMsg = None
    newUserCur = db.executeQuery(cfg.usr_query.format(oracleid))
    for row in newUserCur:
        userRec = None
        if row[13] == 'NEW' or row[11] is None:
            userRec = user.findUser(row[0])
            if userRec is None:
                userRec, errorMsg = user.createUser(row)
        else:
            userRec, errorMsg = user.updateUser(row)
        if userRec is not None:
            user.updateUserDB(row[0], userRec)
    return errorMsg


def processMembership(userId, db):
    user = User(db)
    errorMsg = None
    membershipId, membershipStatus = user.findMembership(userId)
    if membershipId is None:
        membershipId, errorMsg = user.createMembership(userId)
    if membershipId is not None:
        user.updateMembershipDB(userId, membershipId)
    return errorMsg


def processSupervisor(membershipId, db):
    sup = Supervisor(db)
    errorMsg = None
    actId = sup.findSupAccount(membershipId)
    if actId is None:
        actId, errorMsg = sup.createSupAccount(membershipId)
    if actId is not None:
        sup.updSupAccountDB(actId, membershipId)
    return errorMsg


def deleteSupervisor(oracleId, db):
    sup = Supervisor(db)
    errorMsg = None
    delCur = db.executeQuery(cfg.exp_sup_query.format(oracleId))
    for row in delCur:
        actId = row[0]
        actstatus, errorMsg = sup.delSupAccount(actId)
        if actstatus == cfg.SUCCESS:
            sup.delSupAccountDB(actId)
    return errorMsg


def processSupervisee(oracleId, db):
    supervisee = Supervisee(db)
    errorMsg = None
    status = None
    cur = db.executeQuery(cfg.supervisee_query.format(oracleId))

    for row in cur:
        user_id = row[0]
        sup_account_id = row[1]
        status = row[2]
        sup_name = row[3]
        sup_type = row[4]
        if status == 'NEW':
            status, errorMsg = supervisee.createSupervisee(user_id, sup_account_id)
            if sup_type == 'MGR':
                user = User(db)
                status2, errorMsg2 = user.updateManagerName(user_id, sup_name)
                if errorMsg2 is not None:
                    if errorMsg is not None:
                        errorMsg = errorMsg + errorMsg2
                    else:
                        errorMsg = errorMsg2
            supervisee.updateSuperviseeDB(user_id, sup_account_id, 'PROCESSED')
        else:
            status, errorMsg = supervisee.deleteSupervisee(user_id, sup_account_id)
            if status == cfg.SUCCESS:
                supervisee.updateSuperviseeDB(user_id, sup_account_id, 'DELETED')
    return errorMsg


def processNewFields(membershipId, db):
    fv = CustomFieldValues(db)
    errorMsg = None
    count, items = fv.findFieldVals(membershipId)
    if count == 0:
        # create all the custom field values from staging table
        fv.createALLFieldVals(membershipId, None)
    else:
        # Find the custom field value Id and populate in staging
        fvalIds = dict()
        for item in items:
            fvalIds[item['customFieldId']] = item['id']
        fv.createALLFieldVals(membershipId, fvalIds)
    return errorMsg


def processUpdFields(userId, db):
    fv = CustomFieldValues(db)
    errorMsg = None
    updFieldValsCur = db.executeQuery(cfg.upd_fvals_query.format(userId))
    for row in updFieldValsCur:
        v_id, errorMsg = fv.updateFieldVal(row[1], row[0], row[2])
        if v_id is not None:
            fv.updateFieldValStatusDB(v_id)
    return errorMsg


def preprocessCustomFields(memberhsipId, db):
    db.executeProcedure('xxdbd_learn_rest_process_pkg.process_custom_fields', [memberhsipId])


def threadedProcess(objectType, fcall):
    print('Insert actions ', datetime.datetime.now() - tm, datetime.datetime.now())
    db.executeProcedure('xxdbd_learn_rest_process_pkg.insert_actions',[objectType, cfg.p_request_id])
    bnft_actn_id = db.executeNumFunction('xxdbd_learn_rest_process_pkg.get_benefit_action_id', ())
    print('Start threads ', datetime.datetime.now() - tm, datetime.datetime.now())
    startThreads(bnft_actn_id,fcall)
    print('Completed process',objectType,  datetime.datetime.now() - tm, datetime.datetime.now())


def preprocess(objectType):
    print('Preprocessing started.', objectType, datetime.datetime.now() - tm, datetime.datetime.now())
    if objectType == 'U' and cfg.PREPROCESS:
        # Execute this procedure to populate xxdbd_learn_user from XXDBD_FSN_LEARN_STG_TBL
        # Fetches New hires, Updates and Terminations
        db.executeProcedure('xxdbd_learn_rest_process_pkg.process_users')
    elif objectType == 'A' and cfg.PREPROCESS:
        # Process Supervisor accounts
        db.executeProcedure('xxdbd_learn_rest_process_pkg.process_supervisors')
    elif objectType == 'S' and cfg.PREPROCESS:
        # Process Supervisees
        db.executeProcedure('xxdbd_learn_rest_process_pkg.process_supervisees')
    elif objectType == 'C' and cfg.PREPROCESS:
        db.executeProcedure('xxdbd_learn_rest_process_pkg.update_flags')
        threadedProcess('PC',preprocessCustomFields)

    print('Preprocessing completed.', objectType, datetime.datetime.now() - tm, datetime.datetime.now())


def process(objectType, fcall):
    preprocess(objectType)
    threadedProcess(objectType, fcall)


def generateReport():
    print('Generate report for the request :', cfg.p_request_id)
    count = 0
    try:
        errCur = db.executeQuery(cfg.report_query.format(cfg.p_request_id))
        column_names = [i[0] for i in errCur.description]
        path = os.environ['XXDBD_INTERFACE'] + '/HRIS'
        filename = os.path.join(path, 'dbd_learn_rest_rpt.csv' )
        fp = open(filename, 'w')
        out = csv.writer(fp, lineterminator='\n')
        out.writerow(column_names)
        for row in errCur:
            out.writerow(row)
            count+=1
        fp.close()
    except Exception as x:
        print('Exception :(', x.__class__.__name__)
        print(traceback.format_exc())

    print('Report generated.', count, 'lines written to file.')


tm = datetime.datetime.now()

print('Process started : ', tm)
#Connect to Learn system and set Token in header
setToken()

# Create a database Object to execute queries, statement etc
db = Database()
fcalls = {'U': processUser, 'M': processMembership, 'A': processSupervisor, 'ES': deleteSupervisor, 'S': processSupervisee, 'C':processNewFields, 'UC': processUpdFields}
calList = ['U', 'M', 'A', 'ES', 'S', 'C', 'UC']

if cfg.p_process_type != 'ALL':
    calList.clear()
    calList.append(cfg.p_process_type)
    if cfg.p_process_type == 'A':
        calList.append('ES')
    if cfg.p_process_type == 'C':
        calList.append('UC')

for c in calList:
    process(c, fcalls[c])

#Once all the processes are completed, generate an error report
generateReport()





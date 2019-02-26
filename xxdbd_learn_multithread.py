from xxdbd_learn_database import Database
import xxdbd_learn_config as cfg
import threading


def do_multithread(bnft_actn_id, fcall):
    db = Database()

    c_range_for_thread = '''     
         SELECT  ran.range_id, ran.starting_person_action_id,
                       ran.ending_person_action_id
         FROM ben_batch_ranges ran
         WHERE ran.range_status_cd = 'U'
         AND ran.benefit_action_id = {0}
         AND ROWNUM < 2      
         FOR UPDATE OF ran.range_status_cd
         '''

    c_person_for_thread = '''
    SELECT   ben.non_person_cd, ben.person_action_id
          FROM ben_person_actions ben
          WHERE ben.benefit_action_id = {0}
          AND ben.action_status_cd <> 'P'
          AND ben.person_action_id BETWEEN {1}
                                   AND {2}
          ORDER BY ben.person_action_id
    '''

    # Loop over ranges
    while True:
        rangeCur=db.executeQuery(c_range_for_thread.format(bnft_actn_id))
        row = rangeCur.fetchone()
        if row is None:
            exit(0)
        else:
            rangeid = row[0]
            startid = row[1]
            endid = row[2]

            if rangeid is not None:
                db.executeStmt('UPDATE ben_batch_ranges ran SET ran.range_status_cd = \'P\' WHERE ran.range_id = :r',{'r': rangeid})
                percur = db.executeQuery(c_person_for_thread.format(bnft_actn_id, startid, endid))

                for perrow in percur:
                    id = perrow[0]
                    actid=perrow[1]
                    errorMsg = fcall(id, db)
                    status = 'P'
                    if errorMsg is not None:
                        status = 'E'
                    db.executeStmt('update ben_person_actions set  action_status_cd = :s, chunk_number=:c, action_text=:e where  person_action_id=:a', {'s': status, 'a': actid, 'c': int(threading.current_thread().name), 'e' : errorMsg })





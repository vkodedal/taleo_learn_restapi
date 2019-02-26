import cx_Oracle
import xxdbd_learn_config as cfg


class Database:
    _instance = None
    connection=None
    '''
    def __new__(cls):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            print('New DB Instance created.')
        return cls._instance
    '''
    def __init__(self):
        if self.connection is None:
            self.connection = cx_Oracle.connect(cfg.con_str)
            print('New connection created')
        else:
            self.connection.ping()

    def __del__(self):
        try:
            self.connection.close()
            print('Connections closed')
        except:
            print('Connections already closed')

    def executeQuery(self, query):
        self.connection.ping()
        cur = self.connection.cursor()
        cur.execute(query)
        return cur

    def executeProcedure(self, proc, params=[]):
        self.connection.ping()
        updCur = self.connection.cursor()
        try:
            updCur.callproc(proc, params)
            self.connection.commit()
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if hasattr(error, 'code'):
                print(error.code)
            if hasattr(error, 'message'):
                print(error.message)
            if hasattr(error, 'context'):
                print(error.context)
        updCur.close()

    def executeStmt(self, statement, params):
        self.connection.ping()
        updCur = self.connection.cursor()
        try:
            updCur.execute(statement, params)
            self.connection.commit()
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if hasattr(error, 'code'):
                print(error.code)
            if hasattr(error, 'message'):
                print(error.message)
            if hasattr(error, 'context'):
                print(error.context)
        updCur.close()

    def executeNumFunction(self, func, params):
        self.connection.ping()
        cur = self.connection.cursor()
        try:
            val = cur.callfunc(func, cx_Oracle.NUMBER, params)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if hasattr(error, 'code'):
                print(error.code)
            if hasattr(error, 'message'):
                print(error.message)
            if hasattr(error, 'context'):
                print(error.context)
        cur.close()
        return val
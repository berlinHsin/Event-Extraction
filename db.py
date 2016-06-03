import MySQLdb

def connect():
    db = MySQLdb.connect('localhost','root','Kid95400','security')
    return db 

def fetchData(db,start,end) :
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    sql    = 'select * from `tweet` where unix_timestamp(`create_time`) between {} and {}'.format(start,end)
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

def usersScore(db,period) :
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    sql    = 'select * from `users` where unix_timestamp(`period`) = {}'.format(period)
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

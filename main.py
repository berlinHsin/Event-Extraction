import sys , time , datetime , db , lib , eva , os
import numpy as np

#START = time.mktime(datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d").timetuple())
#END   = time.mktime(datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d").timetuple())
#datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
START = float(sys.argv[1])
END   = float(sys.argv[2])
topics = int(sys.argv[3])
pid   = int(sys.argv[4])

conn  = db.connect()
datas = db.fetchData(conn,START,END)

start_token = datetime.datetime.fromtimestamp(START).strftime('%Y-%m-%d')
end_token = datetime.datetime.fromtimestamp(END).strftime('%Y-%m-%d')

docs  = [ data['text'] for data in datas ]

if len(docs) == 0 :
    print("No Data")
    os._exit(0)

model , vocab = lib.formTopics(docs,topics)
topic_word = model.topic_word_

doc_topic = [ np.argmax(d) for d in model.doc_topic_ ]
users     = [ data['user_id'] for data in datas]

scores = db.usersScore(conn,START)
usersScore = dict()
for s in scores :
    usersScore.update({s['user_id']:s['score']})

topicsScore = eva.scoreTopics(doc_topic,users,usersScore)
usersScore  = eva.scoreUsers(doc_topic,users,topicsScore,usersScore)

cursor = conn.cursor()

topics_id = list()
for i , topic_dist in enumerate(topic_word) :
    topic_words = np.array(vocab)[np.argsort(topic_dist)][:-8:-1]
    contribute  = sorted(topic_dist,reverse=True)[:8]
    sql = " insert into `topics` (`topic_id`,`create_time`,`end_time`,`words`,`contribute`,`importance`,`pid`) values ('{}','{}','{}','{}','{}','{}','{}')"
    sql = sql.format(i,start_token,end_token,",".join(topic_words),','.join([ str(cont) for cont in contribute]),topicsScore[i],pid)
    cursor.execute(sql)
    topics_id.append(conn.insert_id())
conn.commit()
print(topics_id)

cursor = conn.cursor()
for i , topic in enumerate(doc_topic) :
    sql = " insert into `topic_doc` (`tweet_id`,`topic_id`,`pid`) values ('{}','{}','{}')"
    sql = sql.format(datas[i]['tweet_id'],topics_id[topic],pid)
    cursor.execute(sql)
conn.commit()

cursor = conn.cursor()
for user in usersScore :
    value = usersScore[user]
    sql = "insert into `users` (`user_id`,`score`,`period`,`pid`) values ('{}','{}','{}','{}')"
    sql = sql.format(user,value,start_token,pid)
    cursor.execute(sql)
conn.commit()

os.system("python confirm.py {} {} {}".format(start_token,end_token,pid))

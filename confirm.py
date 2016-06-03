import db , sys , MySQLdb , json

create_time = sys.argv[1]
end_time = sys.argv[2]
pid = sys.argv[3]

f = open('config.json')
parameters = json.load(f)
alpha = parameters['alpha']
defaultValue = parameters['default']

def avg(datas):
    """
        (list) -> (float)
    """
    return float( sum(datas) ) / len(datas)

def hIndex(datas) :
    """
        (list) -> (int)
    """
    count = 0 
    for value in datas :
        if value > count :
            count += 1
        else :
            return count
    return count

def getPairs(datas) :
    pairs = []
    for i,v1 in enumerate(datas) :
      for v2 in datas[i+1:]:
        pairs.append( (v1,v2) )
    return pairs 

def correlation(origin,compare):
    same ,diff= 0,0 
    origins = getPairs(origin)
    compares = getPairs(compare)

    for origin in origins :
        if origin in compares :
            same += 1 
        else :
            diff += 1
    return float( same-diff ) / (same+diff)


sql = "select id , importance from topics where create_time = '{}' and end_time ='{}' and pid = {}"
sql = sql.format(create_time,end_time,pid)

conn = db.connect()
cursor = conn.cursor()
cursor.execute(sql)
result = cursor.fetchall()

topics = [ (topic[0],topic[1]) for topic in result ]
topic_retweet , topic_favorite = dict(),dict()

for topic in topics :
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = "select tweet.favorite_count , tweet.retweet_count from topic_doc , tweet"
    sql += " where topic_doc.topic_id = {} and topic_doc.tweet_id = tweet.tweet_id"
    sql = sql.format(topic[0])
    cursor.execute(sql)
    result = cursor.fetchall()
    retweet = sorted([ tweet['retweet_count'] for tweet in result],reverse=True)
    favorite = sorted([ tweet['favorite_count'] for tweet in result],reverse=True)
    topic_retweet.update({topic[0]:retweet})
    topic_favorite.update({topic[0]:favorite})

retweet_avg_list , favorite_avg_list = list() , list()
retweet_h_list , favorite_h_list = list() , list()
count = list()
tmp_count = list()

for index , importance in topics :
    count.append( (index,len(topic_retweet[index])) )
    retweet_avg_list.append( (index, avg(topic_retweet[index])) )
    favorite_avg_list.append( (index,avg(topic_favorite[index])) )
    retweet_h_list.append((index,hIndex(topic_retweet[index])) )
    favorite_h_list.append((index,hIndex(topic_favorite[index])))
    tmp_count.append(len(topic_retweet[index]))

importance_list = [ topic[0] for topic in sorted(topics,key=lambda d:d[1],reverse=True)]
retweet_avg_list = [ topic[0] for topic in sorted(retweet_avg_list,key=lambda d:d[1],reverse=True) ]
favorite_avg_list = [ topic[0] for topic in sorted(favorite_avg_list,key=lambda d:d[1],reverse=True) ]
retweet_h_list = [ topic[0] for topic in sorted(retweet_h_list,key=lambda d:d[1],reverse=True) ]
favorite_h_list = [ topic[0] for topic in sorted(favorite_h_list,key=lambda d:d[1],reverse=True) ]
count = [ topic[0] for topic in sorted(count,key=lambda d:d[1],reverse=True) ]


impo_r_avg = correlation(importance_list,retweet_avg_list)
impo_f_avg = correlation(importance_list,favorite_avg_list)
impo_r_h = correlation(importance_list,retweet_h_list)
impo_f_h = correlation(importance_list,favorite_h_list)
impo_count = correlation(importance_list,count)

cursor = conn.cursor()
sql = "insert into `confirm` (`create_time`,`end_time`,`pid`,`ret_avg`,`fav_avg`,`ret_h`,`fav_h`,`count`,`alpha`,`default`) values('{}','{}',{},{},{},{},{},{},{},{})"
sql = sql.format(create_time,end_time,pid,impo_r_avg,impo_f_avg,impo_r_h,impo_f_h,impo_count,alpha,defaultValue)
cursor.execute(sql)
conn.commit()

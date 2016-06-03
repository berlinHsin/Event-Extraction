#encoding:utf-8
import collections , json

f = open('config.json')
parameters = json.load(f)
alpha = parameters['alpha']
defaultValue = parameters['default']

def usersPostCount(docs,users):
    states = dict()
    for i , topic in enumerate(docs) :
        if users[i] in states :
            states[ users[i] ][ topic ] += 1
        else :
            states.update({users[i]:[0]*12})
            states[ users[i] ][topic] += 1
    return states

def usersRateCal(docs,users):
    states = usersPostCount(docs,users)

    result = dict()
    for user in states :
        summation = sum(states[user])
        tmp = [ float(value)/summation for value in states[user] ]
        result.update({user:tmp})

    return result

def usersTopicsRateCal(docs,users):
    states = usersPostCount(docs,users) 
    doc_count = collections.Counter(docs)
    
    for user in states :
        rate = states[user]
        for i , value in enumerate(rate) :
            value = float(value)/doc_count[i]
            states[user][i] = value

    return states

def scoreTopics(docs,users,usersScore,topicsCount=12) :
    usersRate = usersRateCal(docs,users)
    topics = [0.]*topicsCount
    
    for i,topic in enumerate(topics) :
        score = 0.
        for user in usersRate :
            rate = usersRate[user]
            if rate[i] != 0 :
                if user in usersScore :
                    score += rate[i]*usersScore[user]
                else :
                    score += rate[i]*defaultValue
        topics[i] = score
    maximum = max(topics)
    topics  = [ score/maximum for score in topics ]
    return topics

def scoreUsers(docs,users,topicScore,usersScore) :
    usersRate = usersRateCal(docs,users)
    usersTopicsRate = usersTopicsRateCal(docs,users)

    topicSize = max(docs)

    currentUsersScore = dict()
    
    for user in usersRate :
        score = 0.
        if user in usersScore :
            pastValue = usersScore[user]
        else :
            pastValue = defaultValue
        for i in range(topicSize+1) :
            score += (1-alpha)*usersRate[user][i]*( topicScore[i] - pastValue*usersRate[user][i] )
            score += (alpha)*usersTopicsRate[user][i]*topicScore[i]
        currentUsersScore.update( { user:score })
   
    maximum = max(currentUsersScore.values())

    for user in currentUsersScore :
        value = currentUsersScore[user]
        currentUsersScore.update( { user : value/maximum })

    return currentUsersScore
    

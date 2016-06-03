import lda , re , collections , nltk , glob
import numpy as np

lemma     = nltk.wordnet.WordNetLemmatizer()

def readStopWords():
    stopwords = []
    txts = glob.glob('dict/*.txt') 
    for txt in txts :
        with open(txt,'r') as f :
            for line in f :
                stopwords.append(line[:-1])
    stopwords = list(set(stopwords))
    return stopwords

def docWash(texts,stopwords):
    # input texts 
    # remove all meanless words and return counted text
    result = []
    p = re.compile(u"[a-zA-Z]{2,}")
    for text in texts :
        sentence = []
        nouns = ['NN','NNP','NNPS','NNS']
        tokens = nltk.word_tokenize(unicode(text,'utf-8'))
        tagged = nltk.pos_tag(tokens)
        for word in tagged :
            if word[1] in nouns :
                sentence.append(word[0])
        q = " ".join(sentence)
        r = re.sub(u'http.*\s{0,1}','',q)
        r = re.sub(u'rt','',r)
        r = re.sub(u'RT','',r)
        r = re.sub(u'#','',r)
        r = p.findall(r)
        r = [ lemma.lemmatize(w).lower() for w in r if w.lower() not in stopwords ]
        r = collections.Counter(r).most_common()
        result.append(r)
    return result

def formVocab(docs):
    vocab = set()
    for doc in docs :
        for word in doc :
            vocab.add(word[0])
    return list(vocab)

def formModel(docs,vocab):
    tf = np.zeros( (len(docs),len(vocab)) ,dtype=np.int)
    for i , term in enumerate(docs) :
        for t in term :
            index = vocab.index(t[0])
            tf[i][index] = t[1]
    return tf

def formTopics(docs,topics=12) :
    # input a list of documents 
    stopwords = readStopWords()
    doc_washed = docWash(docs,stopwords)
    vocab = formVocab(doc_washed)
    tf = formModel(doc_washed,vocab)

    model = lda.LDA( n_topics = topics ,n_iter = 1000 , random_state = 1)
    model.fit(tf)

    return model , vocab 

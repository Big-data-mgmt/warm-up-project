import re
from pyspark import SparkContext
import string
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
sc = SparkContext("local", "program app")

category = []

def stringtodict(counts):
    d = {}
    for count in counts :
        splitKey = count[0].split('#')
        if splitKey[0] not in d:
            d[splitKey[0]] = [splitKey[1], count[1]]
            print('\"' + splitKey[1] + '\" appeared in ' + str(count[1]) + ' articles under category \"' + splitKey[0] + '\"')
        if len(d) == 10:
            return d
    return d

def remove_accents(raw_text):
        #Removes common accent characters.

    raw_text = re.sub(u"[àáâãäå]", 'a', raw_text)
    raw_text = re.sub(u"[èéêë]", 'e', raw_text)
    raw_text = re.sub(u"[ìíîï]", 'i', raw_text)
    raw_text = re.sub(u"[òóôõö]", 'o', raw_text)
    raw_text = re.sub(u"[ùúûü]", 'u', raw_text)
    raw_text = re.sub(u"[ýÿ]", 'y', raw_text)
    raw_text = re.sub(u"[ß]", 'ss', raw_text)
    raw_text = re.sub(u"[ñ]", 'n', raw_text)
    return raw_text 

def cleanUp(word):
    word = word.encode("ascii", "ignore").decode() # remove unicode characters which were showing up as frequent chars
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    word = regex.sub('', word)
    word = word.strip('"')
    word = word.strip('\'')
    word = word.strip('`')
    word = word.strip('”')
    word = word.strip('’')
    word = word.strip('“')
    return remove_accents(word)

def urlparse(line):
    global category
    category = ['/'.join(line.split()[1].split('/')[6:][:-1])] #splits URL and gives only category
    
def removeStopWords(tokens, stop_words):
    filteredWords = []
    for w in tokens:
        cleanWord = cleanUp(w.lower())
        if cleanWord not in stop_words:
            filteredWords.append(cleanWord)
    return [''.join(category) + '#' + w for w in filteredWords if w not in '']
    
def parseLine(line, stop_words):
    if 'URL: http://www.nytimes.com/' in line:
        urlparse(line)
        return []
    return removeStopWords(word_tokenize(line), stop_words)

def categorycount():
    stop_words = set(stopwords.words('english')) 
    stop_words.add('said')
    stop_words.add('mr')
    text_file = sc.textFile("nytimes_news_articles.txt")
    counts = text_file.flatMap(lambda line:parseLine(line, stop_words)).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b, 1).map(lambda x: (x[1],x[0])).sortByKey(0, 1).map(lambda x: (x[1],x[0])).collect()
    print('**********************************************************************')
    stringtodict(counts)
    print('**********************************************************************')

categorycount()
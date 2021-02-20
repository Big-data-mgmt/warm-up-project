from pyspark import SparkContext
import re
import string
from pyspark import SparkConf
import nltk
from nltk.corpus import stopwords
nltk
from nltk.tokenize import word_tokenize
import matplotlib.pyplot as plt
from wordcloud import WordCloud
sc = SparkContext("local", "program app")


def wordcloud(counts):
    d = stringtodict(counts)
    wordcloud = WordCloud(width = 1000, height = 1000, 
                background_color ='white', 
                min_font_size = 10).generate_from_frequencies(d)
    plt.figure(figsize = (8, 8), facecolor = None)
    plt.imshow(wordcloud) 
    plt.axis("off") 
    plt.tight_layout(pad = 0) 
    plt.show() 

def cleanUp(word):
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    word = regex.sub('', word)
    word = word.strip('"')
    word = word.strip('\'')
    word = word.strip('`')
    word = word.strip('”')
    word = word.strip('’')
    word = word.strip('“')
    return word
    
def removeStopWords(tokens, stop_words):
    return [w.lower() for w in tokens if not w.lower() in stop_words]
    

def stringtodict(counts):
    d = {}
    for count in counts :
        d[count[0]] = count[1]
    return d

    

def wordcount():
    stop_words = set(stopwords.words('english')) 
    text_file = sc.textFile("nytimes_news_articles.txt") #reading file
    counts = text_file.flatMap(lambda line: removeStopWords(word_tokenize(line), stop_words)).map(lambda word: (cleanUp(word), 1)).reduceByKey(lambda a, b: a + b, 1).map(lambda x: (x[1],x[0])).sortByKey(0, 1).map(lambda x: (x[1],x[0])).take(100) 
    #counts.saveAsTextFile("C:\\Users\\bhagy\\OneDrive\\Desktop\\BIGDATA\\Warmup project\\archive\\counts")
    #print(counts)
    wordcloud(counts)

    
 
wordcount()

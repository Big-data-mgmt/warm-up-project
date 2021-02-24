from pyspark import SparkContext
import re
import string
from pyspark import SparkConf
import matplotlib.pyplot as plt
from wordcloud import WordCloud
sc = SparkContext("local", "program app")
import os
import shutil

# Code to remove other content from the URL lines from text file
def removeContent(original_file, output_file, condition):
    with open(original_file, 'rb') as read_obj, open(output_file, 'wb') as write_obj:
        for line in read_obj:
            if condition(line) == True:
                strObj = line.decode('UTF-8')
                last = strObj.rfind('/')
                strObj = strObj[38: last+1:]
                line = str.encode(strObj)
                write_obj.write(line + b'\n')
            
def formatFile(file_name, output_file, word):
    removeContent(file_name, output_file, lambda x : word in x )
   
# Create wordcloud for the given counts of categories   
def wordCloud(counts):
    d = stringToDictionary(counts)
    wordcloud = WordCloud(width = 1000, height = 1000, 
                background_color ='white', 
                min_font_size = 10).generate_from_frequencies(d)
    plt.figure(figsize = (8, 8), facecolor = None)
    plt.imshow(wordcloud) 
    plt.axis("off") 
    plt.tight_layout(pad = 0) 
    plt.show() 

# Convert string to dictionary
def stringToDictionary(counts):
    d = {}
    for count in counts :
        d[count[0]] = count[1]
    return d

# main function to create cloud of top 5 news category
def topCategory():
    output_file = 'output.txt'
    formatFile('nytimes_news_articles.txt', output_file, b'http://www.nytimes.com/')
    text_file = sc.textFile(output_file) #reading file
    counts = text_file.map(lambda x:(x, 1)).reduceByKey(lambda a, b: a + b, 1).sortBy(lambda x: x[1],ascending=False).take(5)
    wordCloud(counts)

topCategory()
from mrjob.job import MRJob
import mrjob
import sqlite3
import math
import os


def textAnalyzer(text):
    if len(text) == 0:
        return -1, -1, -1, -1
    vowels = 'aeiouy'
    #
    words = 1
    sentences = 0
    longWords = 0
    complexWords = 0
    syllables = 0
    wordLengthCount = 0
    syllablesInWord = 0
    lastChar = ""
    for c in text:
        if c == " ":
            if lastChar == "e":
                syllablesInWord -= 1
            if syllablesInWord <= 0:
                syllablesInWord = 1
            syllables += syllablesInWord
            if syllablesInWord > 2:
                complexWords += 1
            if wordLengthCount > 6:
                longWords += 1
            wordLengthCount = 0
            syllablesInWord = 0
            words += 1
        elif c == ".":
            sentences += 1
        else:
            if c in vowels and lastChar not in vowels:
                syllablesInWord += 1
            wordLengthCount += 1
            lastChar = c
            if wordLengthCount == 1 and c in vowels:
                syllablesInWord += 1
    if lastChar == "e":
        syllablesInWord -= 1
    if syllablesInWord <= 0:
        syllablesInWord = 1
    syllables += syllablesInWord
    if syllablesInWord > 2:
        complexWords += 1
    if wordLengthCount > 6:
        longWords += 1
    if sentences==0:
        return -1,-1,-1,-1
    # print(words,sentences,longWords,syllables,complexWords)
    SMOG = 1.043 * math.sqrt(complexWords * (30 / sentences)) + 3.1291
    GFI = 0.4 * ((words / sentences) + 100 * (complexWords / words))
    FK = 206.835 - 1.015 * (words / sentences) - 84.6 * (syllables / words)
    LIX = words / sentences + (longWords * 100) / words
    return SMOG, GFI, FK, LIX


def preProcessing(dir,filename):
    with open(dir+filename, 'r', encoding='utf8') as f:
        lines = f.readlines()
    no_backslash = []
    for i in range(len(lines)):
        if not lines[i].split('\n')[0] == '':
            no_backslash.append(lines[i].split('\n')[0])

    sql_ids = [[no_backslash[0].split('"')[1], no_backslash[0].split('"')[3], no_backslash[0].split('"')[5]]]
    topics = [no_backslash[2]]
    topic_count = 0
    j = 2
    while j < len(no_backslash) - 1:
        if no_backslash[j][0:10] == '</doc>':
            temp = no_backslash[j + 1].split('"')
            sql_ids.append([temp[1], temp[3], temp[5]])
            j += 3
            topics.append(no_backslash[j])
            topic_count += 1
            j += 1
        else:
            topics[topic_count] += no_backslash[j]
            j += 1
    return topics, sql_ids


def insert(c,db,ids,name,url,s1,s2,s3,s4):
    # Insert a row of data
    db.execute("INSERT INTO Wiki VALUES (?,?,?,?,?,?,?,NULL)",(ids,name,url,s1,s2,s3,s4))
    #db.commit()

class DataProcessing(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def configure_options(self):
        #Define input file, output file and number of iteration
        super(DataProcessing, self).configure_options()
        self.add_file_option('--database')
        self.add_passthru_arg('--d')
    def mapper(self, _, line):
        yield line, None

    def reducer_init(self):
        # make sqlite3 database available to reducer
        self.sqlite_conn = sqlite3.connect(self.options.database)
        self.c=self.sqlite_conn.cursor()


    def reducer(self, fileName, _):
        topics, sql_ids = preProcessing(self.options.d,fileName)

        for sql_id, topic in zip(sql_ids, topics):
            score = textAnalyzer(topic)
            insert(self.c,self.sqlite_conn,int(sql_id[0]), sql_id[2], sql_id[1], score[0], score[1], score[2], score[3])
        self.sqlite_conn.commit()
        self.sqlite_conn.close()
        print(fileName)
        yield None, None
if __name__ == '__main__':
    DataProcessing.run()
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

def calcPointsForFile(filename):
    with open(os.path.dirname(os.path.realpath(__file__))+filename, 'r', encoding='utf8') as f:
        lines = f.readlines()
    no_backslash = []
    for i in range(len(lines)):
        if not lines[i].split('\n')[0] == '':
            no_backslash.append(lines[i].split('\n')[0])

    topics = [no_backslash[2]]
    topic_count = 0
    j = 2
    while j < len(no_backslash) - 1:
        if no_backslash[j][0:10] == '</doc>':
            j += 3
            topics.append(no_backslash[j])
            topic_count += 1
            j += 1
        else:
            topics[topic_count] += no_backslash[j]
            j += 1
    for i in range(0, len(topics)):
        textAnalyzer(topics[i])

class DataProcessing(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    #def configure_options(self):
    #    #Define input file, output file and number of iteration
    #    super(DataProcessing, self).configure_options()
    #    self.add_file_option('--database')
    def mapper(self, _, line):
        lines=line.split("\n")
        for l in lines:
            yield l, _

    #def reducer_init(self):
    #    # make sqlite3 database available to reducer
    #    self.sqlite_conn = sqlite3.connect(self.options.database)


    def reducer(self, fileName, _):
        calcPointsForFile(fileName)
        print(fileName)
        yield None, None
if __name__ == '__main__':
    DataProcessing.run()
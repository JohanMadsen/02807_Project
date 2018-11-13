# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 14:39:51 2018

@author: peter
"""
import sqlite3

def create():
    try:
        # Create table
        c.execute('''CREATE TABLE Wiki
             (ID INTEGER primary key, name text, url text, score1 real,score2 real, score3 real, score4 real, cluster_id INTEGER)''')
    except:
        pass
def insert(ids,name,url,s1,s2,s3,s4):
    # Insert a row of data
    c.execute("INSERT INTO Wiki VALUES (?,?,?,?,?,?,?,NULL)",(ids,name,url,s1,s2,s3,s4))

def select(verbose=True):
    sql='SELECT * FROM Wiki'
    recs=c.execute(sql)
    if verbose:
        for row in recs:
            print(row)

conn = sqlite3.connect('WIKI.sqlite')

c = conn.cursor()
create()

from wiki_text import calcScoresForFile,textAnalyzer

topics,sql_ids=calcScoresForFile('C:/Users/peter/Desktop/wiki_00.txt')
n=len(sql_ids)
scores=[]
for j in range(n):
    scores.append(textAnalyzer(topics[j]))
for i in range(n):
    insert(int(sql_ids[i][0]),sql_ids[i][2],sql_ids[i][1],scores[i][0],scores[i][1],scores[i][2],scores[i][3])    
    conn.commit()

#print all the stuff we have
#select()
    
# close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()

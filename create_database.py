# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 15:24:20 2018

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
    try:
        c.execute(
            "CREATE TABLE clusters(ID INTEGER PRIMARY KEY,score1 real,score2 real,score3 real,score4 real, sumScore real)")
    except:
        pass
    try:
        c.execute(
            "CREATE TABLE users(ID INTEGER PRIMARY KEY ,name text,articles blob)")
    except:
        pass
    
conn = sqlite3.connect('WIKI.sqlite')

c = conn.cursor()
create()
conn.close()

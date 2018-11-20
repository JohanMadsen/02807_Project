import sqlite3
import sys
def insert(ids,name,url,s1,s2,s3,s4):
    # Insert a row of data
    c.execute("INSERT INTO Wiki VALUES (?,?,?,?,?,?,?,NULL)",(int(ids),str(name),url,s1,s2,s3,s4))




sys.stdin.reconfigure(encoding='utf-8')

conn = sqlite3.connect('WIKI.sqlite')
c = conn.cursor()
for line in sys.stdin:
    split=line.split("¤")
    insert(split[0],split[1],split[2],split[3],split[4],split[5],split[6])
conn.commit()
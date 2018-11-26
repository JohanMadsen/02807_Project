import sqlite3
import sys


def dist(P,C):
    return sum([abs(p - float(c)) ** 2 for p,c in zip(P, C)])


sys.stdin.reconfigure(encoding='utf-8')

conn = sqlite3.connect('WIKI.sqlite')
cursor = conn.cursor()
centroids=[]
for line in sys.stdin:
    line=line[:-1]
    split=line.split(",")
    centroids.append(split)

cursor.execute("SELECT ID,(score1-(SELECT min(score1) from Wiki))/((SELECT max(score1) from Wiki)-(SELECT min(score1) from Wiki)),(score2-(SELECT min(score2) from Wiki))/((SELECT max(score2) from Wiki)-(SELECT min(score2) from Wiki)),(score3-(SELECT min(score3) from Wiki))/((SELECT max(score3) from Wiki)-(SELECT min(score3) from Wiki)),(score4-(SELECT min(score4) from Wiki))/((SELECT max(score4) from Wiki)-(SELECT min(score4) from Wiki)) from Wiki")
points=cursor.fetchall()
for point in points:
    min = -1
    mindist = 1000000000
    d = 0
    for i in range(len(centroids)):
        d = dist(point[1:], centroids[i])
        if d < mindist:
            mindist = d
            min = i
    cursor.execute("UPDATE WIKI set cluster_id=? WHERE ID=?",(min,point[0]))
conn.commit()

count=0
for cen in centroids:
    sum = 0
    for i in range(len(cen)):
        if i==2:
            sum+=1-float(cen[i])
        else:
            sum+=float(cen[i])
    print(count,sum)
    count+=1
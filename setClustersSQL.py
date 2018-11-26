import sqlite3
import sys


def dist(P,C):
    return sum([abs(p - float(c)) ** 2 for p,c in zip(P, C)])


sys.stdin.reconfigure(encoding='utf-8')

conn = sqlite3.connect('WIKI.sqlite')
c = conn.cursor()
centroids=[]
for line in sys.stdin:
    line=line[:-1]
    split=line.split(",")
    centroids.append(split)
points=c.execute("SELECT ID,(score1-(SELECT min(score1) from Wiki))/((SELECT max(score1) from Wiki)-(SELECT min(score1) from Wiki)),(score2-(SELECT min(score2) from Wiki))/((SELECT max(score2) from Wiki)-(SELECT min(score2) from Wiki)),(score3-(SELECT min(score3) from Wiki))/((SELECT max(score3) from Wiki)-(SELECT min(score3) from Wiki)),(score4-(SELECT min(score4) from Wiki))/((SELECT max(score4) from Wiki)-(SELECT min(score4) from Wiki)) from Wiki")
for point in points:
    min = -1
    mindist = 1000000000
    d = 0
    for i in range(len(centroids)):
        d = dist(point[1:], centroids[i])
        if d < mindist:
            mindist = d
            min = i
    print(point[0],mindist,min)

conn.commit()
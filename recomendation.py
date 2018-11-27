import sqlite3
import sys
import pickle
import random
def dist(P,C):
    return sum([abs(p - float(c)) ** 2 for p,c in zip(P, C)])

id = sys.argv[1]

conn = sqlite3.connect('WIKI.sqlite')
cursor = conn.cursor()
cursor.execute("SELECT * from clusters")
centroids=cursor.fetchall()

cursor.execute("SELECT * from users where ID=?",(id))
line=cursor.fetchone()
list=pickle.loads(line[2])
print(list)
cursor.execute(("SELECT (score1-(SELECT min(score1) from Wiki))/((SELECT max(score1) from Wiki)-(SELECT min(score1) from Wiki)),(score2-(SELECT min(score2) from Wiki))/((SELECT max(score2) from Wiki)-(SELECT min(score2) from Wiki)),(score3-(SELECT min(score3) from Wiki))/((SELECT max(score3) from Wiki)-(SELECT min(score3) from Wiki)),(score4-(SELECT min(score4) from Wiki))/((SELECT max(score4) from Wiki)-(SELECT min(score4) from Wiki)) from Wiki WHERE ID IN (%s)")%",".join('?'*len(list)), list)
points=cursor.fetchall()
n = len(points)
center=[0,0,0,0]
for point in points:
    for i in range(len(point)):
        center[i] += point[i]
center = [c / n for c in center]

min = -1
mindist = 1000000000
d = 0
for i in range(len(centroids)):
    d = dist(center, centroids[i][1:5])
    if d < mindist:
        mindist = d
        min = i
recomendCentroid=-1
distToClosestHarderCentroid=1000000000
for centroid in centroids:
    if centroid[5]-centroids[min][5]>0 and centroid[5]-centroids[min][5]<distToClosestHarderCentroid:
        distToClosestHarderCentroid=centroid[5]-centroids[min][5]
        recomendCentroid=centroid[0]

newlist=list.copy()
newlist.append(recomendCentroid)
cursor.execute(("SELECT ID,name,url from Wiki WHERE ID NOT IN (%s) AND cluster_id=%s")%(",".join('?'*(len(list))),"?"), newlist)

articlesToRecommend=cursor.fetchall()

articleRecommended =random.sample(articlesToRecommend,1)
print(articleRecommended[0][1:])
list.append(articleRecommended[0][0])
while len(list)>20:
    list.pop(0)
data=pickle.dumps(list)
cursor.execute("UPDATE users set articles=? WHERE ID=?",(data,id))
conn.commit()
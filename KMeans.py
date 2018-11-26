from mrjob.job import MRJob
from mrjob.job import MRStep
import mrjob
import sqlite3
import math
import numpy as np

def dist(P,C):
    return sum([abs(p - float(c)) ** 2 for p,c in zip(P, C)])

class KMeans(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def configure_args(self):
        #Define input file, output file and number of iteration
        super(KMeans, self).configure_args()
        self.add_file_arg('--database')
        #self.add_passthrough_option('--iterations', dest='iterations', default=10, type='int')
    def mapper_loader(self, _, line):
        yield None,line
    def reducer_loader(self,key,values):
        input=""
        for value in values:
            input+=value
            input+="¤"
        yield None,input
    def mapper_init(self):
        self.sqlite_conn = sqlite3.connect(self.options.database)
        self.c = self.sqlite_conn.cursor()
    def mapper(self, _, line):
        centroids=line.split("¤")
        centroids=centroids[:-1]
        points=self.c.execute("SELECT (score1-(SELECT min(score1) from Wiki))/((SELECT max(score1) from Wiki)-(SELECT min(score1) from Wiki)),(score2-(SELECT min(score2) from Wiki))/((SELECT max(score2) from Wiki)-(SELECT min(score2) from Wiki)),(score3-(SELECT min(score3) from Wiki))/((SELECT max(score3) from Wiki)-(SELECT min(score3) from Wiki)),(score4-(SELECT min(score4) from Wiki))/((SELECT max(score4) from Wiki)-(SELECT min(score4) from Wiki)) from Wiki")
        for point in points:
            min=-1
            mindist=1000000000
            d=0
            for i in range(len(centroids)):
                d=dist(point,centroids[i].split(","))
                if d<mindist:
                    mindist=d
                    min=i
            yield min,point

    def reducer(self, Cluster_ID, points):
        points = list(points)
        n = len(points)
        if n>50:
            centroid = [0,0,0,0]
            for point in points:
                for i in range(len(point)):
                    centroid[i]+=point[i]
            centroid = [c/n for c in centroid]
            s=""
            for c in centroid:
                s+=str(c)
                s+=","
            s=s[:-1]
            yield None,s
    def steps(self):
        return [MRStep(mapper=self.mapper_loader,reducer=self.reducer_loader),
                       MRStep(mapper_init=self.mapper_init,mapper=self.mapper,reducer=self.reducer)
                ] * 100#self.options.iterations

if __name__ == '__main__':
    KMeans.run()
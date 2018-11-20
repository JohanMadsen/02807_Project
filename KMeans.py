from mrjob.job import MRJob
from mrjob.job import MRStep
import mrjob
import sqlite3
import math
import numpy as np
#need a lot of work
#def dist(x, y):
#    return math.fabs(x, y)

class KMeans(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def configure_args(self):
        #Define input file, output file and number of iteration
        super(KMeans, self).configure_args()
        self.add_file_arg('--database')
        self.add_passthrough_option('--iterations', dest='iterations', default=10, type='int')
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
        centorids=line.split("¤")
        centorids=centorids[:-1]
        #NEED TO READ WHOLE DATABASE HERE
        #LOOP OVER ALL POINTS HERE
            #for centroid in centorids:
                #YIELD (Cluster_ID,point) here

    def reducer(self, Cluster_ID, points):
        print("hey")
        #CALCULATE NEW CENTROID GIVEN points
        #YIED NEW CENTROID
    def steps(self):
        return [MRStep(mapper=self.mapper_loader,reducer=self.reducer_loader),
                       MRStep(mapper_init=self.mapper_init,mapper=self.mapper,reducer=self.reducer)
                ] * self.options.iterations

if __name__ == '__main__':
    KMeans.run()
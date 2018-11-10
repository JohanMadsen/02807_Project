from mrjob.job import MRJob
from mrjob.job import MRStep
import mrjob
import math
import numpy as np

#def dist(x, y):
#    return math.fabs(x, y)

class KMeans(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def configure_options(self):
        #Define input file, output file and number of iteration
        super(KMeans, self).configure_options()
        self.add_file_option('--infile')
        self.add_file_option('--outfile')
        self.add_passthrough_option('--iterations', dest='iterations', default=1, type='int')

    def get_centroids(self):
        Centroid = np.loadtxt(self.options.infile, delimiter = ',')
        return Centroid

    def write_centroids(self, Centroid):
        np.savetxt(self.options.outfile, Centroid[None], fmt = '%.5f',delimiter = ',')


    def mapper(self, _, line):
        data_ID, Cluster_ID, Coord= line.split('|')
        Coord = Coord.strip('\r\n')
        Coord_arr = np.array(Coord.split(','), dtype = float)
        global Centroid
        Centroid = self.get_centroids()
        Centroid_arr = np.reshape(Centroid, (-1, len(Coord_arr)))
        global nclass
        nclass = Centroid_arr.shape[0]
        global ndim
        ndim = Centroid_arr.shape[1]
        Distance = ((Centroid_arr - Coord_arr)**2).sum(axis = 1)
        Cluster_ID = str(Distance.argmin() + 1)
        Coord_arr = Coord_arr.tolist()
        yield Cluster_ID, (data_ID, Coord_arr)
    def combiner(self, Cluster_ID, values):
        member = []
        Coord_set = []
        Coord_sum = np.zeros(ndim)
        for data_ID, Coord_arr in values:
            Coord_set.append(','.join(str(e) for e in Coord_arr))
            Coord_arr = np.array(Coord_arr, dtype = float)
            member.append(data_ID)
            Coord_sum += Coord_arr
            Coord_sum = Coord_sum.tolist()
        yield Cluster_ID, (member, Coord_sum, Coord_set)
    def reducer(self, Cluster_ID, values):
        final_member = []
        final_Coord_set = []
        final_Coord_sum = np.zeros(ndim)
        for member, Coord_sum, Coord_set in values:
            final_Coord_set += Coord_set
            Coord_sum = np.array(Coord_sum, dtype=float)
            final_member += member
            final_Coord_sum += Coord_sum

        n = len(final_member)
        new_Centroid = final_Coord_sum / n
        Centroid[ndim * (int(Cluster_ID) - 1): ndim * int(Cluster_ID)] = new_Centroid
        if int(Cluster_ID) == nclass:
            self.write_centroids(Centroid)
        for ID in final_member:
            ind = final_member.index(ID)
            yield None, (ID + '|' + Cluster_ID + '|' + final_Coord_set[ind])
    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer)] * self.options.iterations

if __name__ == '__main__':
    KMeans.run()
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostPopularMovie(MRJob):
    def steps(self):
        return [MRStep(mapper = self.rateCountMapper, reducer = self.rateCountReducer),
               MRStep(reducer = self.maxRateCountReducer)]
    def rateCountMapper(self, key, line):
        (userID, movieID, rating, timeStamp) = line.split("\t")
        yield(movieID, 1)
        
    def rateCountReducer(self, key, values):
        yield None, (sum(values), key)
        
    def maxRateCountReducer(self, key, values):
        #'max' only look at the first element of each tuple of your values if your 
        #values are tuples.
        yield max(values)
        
if __name__ == "__main__":
    MRMostPopularMovie.run()
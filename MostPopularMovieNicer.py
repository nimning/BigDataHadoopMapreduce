from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostPopularMovieNicer(MRJob):
    #configure_options: we tell the MRjob that we have additional options we want to accept on command line 
    #when we run script
    def configure_options(self):
        super(MRMostPopularMovieNicer, self).configure_options()
        #send the file 
        
        #add_file_option: Indicates there is another file we want to send along with everything else to the map reduce job
        #'--items': when we pass a parameter (file) after "--items", that file will be distributed along with the code for the script to every node that this job might runs on
        #movieID: moviedName passed along to everynode of the job where it is needed
        self.add_file_option('--items',help = 'Path to u.item')
    def steps(self):
        return [MRStep(mapper = self.rateCountMapper, 
                       reducer_init = self.reducer_init,
                       reducer = self.rateCountReducer),
               MRStep(reducer = self.maxRateCountReducer)]
    def reducer_init(self):
        self.movieNames = {}
        
        with open("u.ITEM") as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1]
            
    def rateCountMapper(self, key, line):
        (userID, movieID, rating, timeStamp) = line.split("\t")
        yield(movieID, 1)
        
    def rateCountReducer(self, key, values):
        yield None, (sum(values), self.movieNames[key])
        
    def maxRateCountReducer(self, key, values):
        #'max' only look at the first element of each tuple of your values if your 
        #values are tuples.
        yield max(values)
        
if __name__ == "__main__":
    MRMostPopularMovieNicer.run()
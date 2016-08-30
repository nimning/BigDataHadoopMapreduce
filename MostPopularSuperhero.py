from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularSuperhero(MRJob):
    def configure_options(self):
        super(MostPopularSuperhero, self).configure_options()
        self.add_file_option('--items', help = 'path to data');
        
    def steps(self):
        return [
            MRStep(mapper = self.countFriendsMapper,
                   reducer_init = self.reducerInit,
                   reducer = self.combineFriendsReducer),
            MRStep(reducer = self.characterWithMaxFriendsReducer)
        ]
    
         
    def countFriendsMapper(self, key, line):
        words = line.split();
        yield (int(words[0]), len(words) - 1);
        
        
    def reducerInit(self):
        self.IdToNameMap = {};
        with open("Marvel-Names.txt") as f:
            for line in f:
                words = line.split('"')
                self.IdToNameMap[int(words[0])] = unicode(words[1], errors = 'ignore')
        
    def combineFriendsReducer(self, key, values):
        yield None, (sum(values), self.IdToNameMap[key])
    
    
    def characterWithMaxFriendsReducer(self, key, values):
        yield max(values)
        
if __name__ == "__main__":
    MostPopularSuperhero.run()
        
    
        
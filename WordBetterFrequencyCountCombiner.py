
from mrjob.job import MRJob
import re

wordRegExp = re.compile(r"[\w']+")
class MRBetterWordFrequencyCountCombiner(MRJob):
    def mapper(self, _, line):
        words = wordRegExp.findall(line)
        for word in words:
            yield (word.lower(), 1)
    
    #do the reducer's work in mapper's node. It may increase the
    #performance
    def combiner(self, key, values):
        yield (key, sum(values))
        
    def reducer(self, key, values):
        yield (key, sum(values))
        
    #combiner and reducer only differ in output, the input needs to
    #be the same
    
    #combiner may not be called
        
if __name__ == "__main__":
    MRBetterWordFrequencyCountCombiner.run()
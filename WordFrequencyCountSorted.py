from mrjob.job import MRJob
from mrjob.step import MRStep
import re

wordRegExp = re.compile(r"[\w']+")

class MRWordFrequencyCountSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.mapperGetWords,
                   reducer = self.reducerCountWords),
            MRStep(mapper = self.mapperMakeCountsKey,
                   reducer = self.reduceOutputWordsSorted)
            ]
    
    def mapperGetWords(self, _, line):
        words = wordRegExp.findall(line);
        for word in words:
            yield (word.lower(), 1)
            
    def reducerCountWords(self, key, values):
        yield (key, sum(values))
        
    def mapperMakeCountsKey(self, key, value):
        yield '%04d' %int(value), key
    
    def reduceOutputWordsSorted(self, key, values):
        for word in values:
            yield (key, word)
            
if __name__ == "__main__":
    MRWordFrequencyCountSorted.run()
        
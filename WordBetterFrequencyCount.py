from mrjob.job import MRJob
import re

wordRegExp = re.compile(r"[\w']+")
class MRBetterWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        words = wordRegExp.findall(line)
        for word in words:
            yield (word.lower(), 1)
            
    def reducer(self, key, values):
        yield (key, sum(values))
        
if __name__ == "__main__":
    MRBetterWordFrequencyCount.run()
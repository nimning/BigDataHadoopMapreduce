from mrjob.job import MRJob
class MRFriendsByAge(MRJob):
    def mapper(self, key, line):
        (ID, name, age, numFriends) = line.split(",")
        yield (age, float(numFriends))
    
    def reducer(self, key, values):
        sum = 0;
        numElement = 0;
        for val in values:
            sum += val;
            numElement += 1;
        
        yield (key, sum / numElement)

if __name__ == '__main__':
    MRFriendsByAge.run()
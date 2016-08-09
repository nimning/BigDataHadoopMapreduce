from mrjob.job import MRJob

class MRTotalAmountByCustomer(MRJob):
    def mapper(self, _, line):
        customer, item, amount = line.split(",")
        yield customer, float(amount)
        
    def reducer(self, key, values):
        yield key, sum(values)
        
if __name__ == "__main__":
    MRTotalAmountByCustomer.run()
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTotalAmountByCustomerSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper  = self.mapperCustomerAmount,
                  reducer = self.reducerCustomerAmount),
            MRStep(mapper  = self.mapperAmountSorted,
                  reducer = self.reducerOutputSorted)
        ]
    
    def mapperCustomerAmount(self, _, line):
        customer, item, amount = line.split(",");
        yield(customer, float(amount))
        
    def reducerCustomerAmount(self, key, values):
        yield (key, sum(values))
        
    def mapperAmountSorted(self, key, value):
        yield ('%04.02f' %float(value), key)
        
    def reducerOutputSorted(self, key, values):
        for value in values:
            yield(value, key)
            
if __name__ == '__main__':
    MRTotalAmountByCustomerSorted.run()
        
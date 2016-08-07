from mrjob.job import MRJob
class MRMaxTemperature(MRJob):
    def MakeFahrenheit(sel, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius);
        fahrenheit = celsius * 1.8 + 32
        return fahrenheit
    
    def mapper(self, _, line):
        (location, date, type, data, x, y, z, w) = line.split(",")
        if (type == "TMAX"):
            temperature = self.MakeFahrenheit(data)
            yield (location, temperature)
            
    def reducer(self, key, temperatures):
        yield (key, max(temperatures))
        
if __name__ == "__main__":
    MRMaxTemperature.run()
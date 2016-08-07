from mrjob.job import MRJob
class MRMinTemperature(MRJob):
    def MakeFahrenheit(sel, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius);
        fahrenheit = celsius * 1.8 + 32
        return fahrenheit
    
    def mapper(self, _, line):
        (location, date, type, data, x, y, z, w) = line.split(",")
        if (type == "TMIN"):
            temperature = self.MakeFahrenheit(data)
            yield location, temperature
            
    def reducer(self, key, temperatures):
        yield key, min(temperatures)
        
if __name__ == "__main__":
    MRMinTemperature.run()
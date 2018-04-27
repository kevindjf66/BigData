from mrjob.job import MRJob
from mrjob.step import MRStep

class SortedCarSales(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.secondmapper,
                   reducer = self.secondreducer)
        ]

    def mapper(self, _, line):
        (year, month, brand, mode, number, per) = line.split(',')
        yield mode, int(number)

    def reducer(self, mode, number):
        yield mode, sum(number)

    def secondmapper(self, mode, total):
        yield '%05d'%int(total), mode

    def secondreducer(self, total, mode):
        for m in mode:
            yield total, m

    
if __name__ == '__main__':
    SortedCarSales.run()

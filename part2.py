from mrjob.job import MRJob
from mrjob.step import MRStep

class TotalSpentByCustomer(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.mapper_spend,
                   reducer = self.reducer_spend),
            MRStep(mapper = self.mapper_total_spend,
                   reducer = self.reducer_ordered_spend)
        ]

    def mapper_spend(self, _, line):
        (customerID, itemID, amountSpent) = line.split(',')
        yield customerID, float(amountSpent)

    def reducer_spend(self, customerID, amountSpent):
        yield customerID, sum(amountSpent)

    def mapper_total_spend(self, customerID, total):
        yield '%04.02f'%float(total), customerID

    def reducer_ordered_spend(self, total, customerID):
        for ID in customerID:
            yield total, ID

if __name__=='__main__':
    TotalSpentByCustomer.run()

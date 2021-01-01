from mrjob.job import MRJob
from mrjob.step import MRStep

class MRFlights(MRJob):

    def step(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]
    def mapper(self,_,line):
        year,items=line.split('\t')
        year=year[1:-1]
        items=items[1:-1]
        month,day,airline,distance=items.split(',')
        distance=int(distance)
        yield None,distance

    def reducer(self,key,values):
        total=0
        num=0
        for value in values:
            total+=value
            num+=1
        yield None,total/num

if __name__=='__main__':
    MRFlights.run()
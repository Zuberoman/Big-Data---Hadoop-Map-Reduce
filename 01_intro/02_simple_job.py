from mrjob.job import MRJob
from mrjob.step import MRStep

class SimpleJob(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]


    def mapper(self, _, line):
        yield 'line',1
        yield 'words',len(line.split())
        yield 'chars',len(line)

    def reducer(self,key,values):
        yield key,sum(values)

if __name__=='__main__':
    SimpleJob.run()
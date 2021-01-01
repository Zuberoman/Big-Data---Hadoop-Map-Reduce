from mrjob.job import MRJob
from mrjob.step import MRStep

class MRFlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_get_keys,
                   reducer=self.reducer_get_sorted)
        ]
    def mapper(self,_,line):
        (YEAR,MONTH,DAY,DAY_OF_WEEK,AIRLINE,FLIGHT_NUMBER,TAIL_NUMBER,
         ORIGIN_AIRPORT,DESTINATION_AIRPORT,SCHEDULED_DEPARTURE,DEPARTURE_TIME,DEPARTURE_DELAY,
         TAXI_OUT,WHEELS_OFF,SCHEDULED_TIME,ELAPSED_TIME,AIR_TIME,DISTANCE,WHEELS_ON,TAXI_IN,
         SCHEDULED_ARRIVAL,ARRIVAL_TIME,ARRIVAL_DELAY,DIVERTED,CANCELLED,CANCELLATION_REASON,
         AIR_SYSTEM_DELAY,SECURITY_DELAY,AIRLINE_DELAY,LATE_AIRCRAFT_DELAY,WEATHER_DELAY)=line.split(',')
        yield (ORIGIN_AIRPORT,DESTINATION_AIRPORT),1

    def reducer(self,key,values):
        yield key,sum(values)

    def mapper_get_keys(self,key,value):
        yield None,(value,key)

    def reducer_get_sorted(self,key,values):
        self.results=[]
        for value in values:
            self.results.append((key,value))

        yield None, sorted(self.results,reverse=True)
if __name__=='__main__':
    MRFlights.run()
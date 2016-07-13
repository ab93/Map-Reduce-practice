from mrjob.job import MRJob

class MRCustomerExpenditure(MRJob):

    def mapper(self, __, line):
    	cust_id, item_id, amt = line.split(',')
    	yield cust_id, float(amt)

    def reducer(self,cust_id,amounts):
    	yield cust_id, sum(amounts)


if __name__ == '__main__':
	MRCustomerExpenditure.run()
#'%04.02f'%float(orderTotal)
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCustomerExpenditure(MRJob):

	def steps(self):
		return[
			MRStep(mapper=self.mapper_get_amount, 
				reducer=self.reducer_set_amount),
			MRStep(mapper=self.mapper_make_amt_key,
				reducer=self.reducer_output_cust)
			]

	def mapper_get_amount(self, __, line):
		cust_id, item_id, amt = line.split(',')
		yield cust_id, float(amt)

	def reducer_set_amount(self, cust_id, amt):
		yield cust_id, sum(amt)

	def mapper_make_amt_key(self,cust_id,amt):
		yield '%04.02f'%float(amt), cust_id

	def reducer_output_cust(self,amt,cust_id):
		for cust in cust_id:
			yield amt, cust

if __name__ == '__main__':
	MRCustomerExpenditure.run()
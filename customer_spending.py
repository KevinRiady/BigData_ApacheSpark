from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RevenueByCustomer")

def parseLine(line):
  fields = line.split(',')
  customerID = fields[0]
  amountSpent = fields[2]
  return (int(customerID), float(amountSpent))

lines = sc.textFile("/FileStore/tables/customer_orders.csv")
parsedLines = lines.map(parseLine)
custSpending = parsedLines.reduceByKey(lambda x, y: x + y)
custSpendingSorted = custSpending.map(lambda x: (x[1], x[0])).sortByKey()
results = custSpendingSorted.collect()

for result in results:
  print(result)
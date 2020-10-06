from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")

def parseLine(line):
  fields = line.split(',')
  stationID = fields[0]
  entryType = fields[2]
  temperature = float(fields[3]) * 0.1 
  return (stationID, entryType, temperature)

lines = sc.textFile("/FileStore/tables/1800.csv")
parsedLines = lines.map(parseLine)
maxTemp = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemp = maxTemp.map(lambda x: (x[0], x[2]))
maxTemp = stationTemp.reduceByKey(lambda x, y: max(x,y))
results = maxTemp.collect();

for result in results:
  print(result[0] + "\t{:.2f}mm".format(result[1]))
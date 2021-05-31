from pyspark import SparkContext
import json
import sys
import time


class Task3:
    def __init__(self):
        self.sc = SparkContext.getOrCreate()
        self.sc.setLogLevel("ERROR")

        self.review_file = sys.argv[1]
        self.output_file = sys.argv[2]
        self.partition_type = sys.argv[3]
        self.n_partitions = int(sys.argv[4])
        self.top_n = int(sys.argv[5])

        self.output = {}
    
    def read_data(self):
        self.review_data = self.sc.textFile(self.review_file)

    @staticmethod
    def custom_hash(x):
        return x

    def get_data(self):
        top_n = self.top_n
        rdd = self.review_data.map(lambda row: (json.loads(row)['business_id'], 1))

        if self.partition_type == "customized":
            rdd = rdd.partitionBy(self.n_partitions, Task3.custom_hash)

        self.output["n_partitions"] = rdd.getNumPartitions()
        self.output["n_items"] = rdd.mapPartitions(lambda x: iter(x), True).collect()
        self.output["result"] = rdd.reduceByKey(lambda x, y: x + y)\
                                    .filter(lambda x: x > top_n).collect()
    
    def write_output(self):
        with open(self.output_file, "w") as f:
            json.dump(self.output, f)

if __name__ == "__main__":
    start_time = time.time()

    runner = Task3()    
    runner.read_data()
    runner.get_data()
    runner.write_output()

    end_time = time.time()

    print("Total time taken: {}".format(end_time - start_time))

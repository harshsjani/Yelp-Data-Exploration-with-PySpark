from pyspark import SparkContext
import json
import sys
import time
from collections import defaultdict

class Task2:
    def __init__(self):
        self.review_file = sys.argv[1]
        self.business_file = sys.argv[2]
        self.output_file = sys.argv[3]
        self.use_spark = True if sys.argv[4] == "spark" else False

        if self.use_spark:
            self.sc = SparkContext.getOrCreate()
            self.sc.setLogLevel("ERROR")

        self.top_n = int(sys.argv[5])

        self.output = {}
    
    def read_data(self):
        if self.use_spark:
            self.review_data = self.sc.textFile(self.review_file)
            self.business_data = self.sc.textFile(self.business_file)
        else:
            with open(self.review_file) as f:
                self.review_data = f.readlines()
            with open(self.business_file) as f:
                self.business_data = f.readlines()

    def get_top_rated_categories(self):
        if self.use_spark:
            self.spark_get_top(self.top_n)
        else:
            self.py_get_top()

    @staticmethod
    def format_review_data(row):
        data = json.loads(row)
        return (data["business_id"], float(data["stars"]))
    
    @staticmethod
    def format_business_data(row):
        data = json.loads(row)
        return (data["business_id"], data["categories"])
    
    @staticmethod
    def split_categories(row):
        return [(row[0], x.strip()) for x in row[1].strip().split(",")]

    def py_get_top(self):
        flat_map = lambda x, fn: [item for lst in x for item in fn(lst)]
        review_data = list(map(Task2.format_review_data, self.review_data))
        business_data = flat_map(filter(lambda row: row[1] is not None, map(Task2.format_business_data, self.business_data)), Task2.split_categories)
        
        # Perform the inner join
        review_dict = defaultdict(list)
        for x in review_data:
            review_dict[x[0]].append(x[1])

        cat_sum = defaultdict(int)
        cat_count = defaultdict(int)

        for row in business_data:
            b_id = row[0]
            if b_id in review_dict:
                for val in review_dict[b_id]:
                    cat_sum[row[1]] += val
                    cat_count[row[1]] += 1

        avg_data = []
        for k in cat_sum:
            avg_data.append((k, cat_sum[k] / cat_count[k]))
        
        avg_data.sort(key=lambda x: (-x[1], x[0]))

        self.output["result"] = list(map(lambda x: (x[0], round(x[1], 1)), avg_data[:self.top_n]))

    
    def spark_get_top(self, top_n):
        r_data = self.review_data.map(lambda row: Task2.format_review_data(row))
        b_data = self.business_data.map(lambda row: Task2.format_business_data(row))\
            .filter(lambda row: row[1] is not None)\
            .flatMap(lambda row: Task2.split_categories(row))

        zero_value = (0, 0)
        seq_fn = (lambda x, y: (x[0] + y, x[1] + 1))
        comb_fn = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
        
        self.output["result"] = r_data.join(b_data)\
                                .map(lambda x: (x[1][1], x[1][0]))\
                                    .aggregateByKey(zero_value, seq_fn, comb_fn)\
                                        .mapValues(lambda x: x[0]/x[1])\
                                            .sortBy(lambda x: (-x[1], x[0]))\
                                                .map(lambda x: (x[0], round(x[1], 1)))\
                                                    .take(top_n)
    
    def write_output(self):
        with open(self.output_file, "w") as f:
            json.dump(self.output, f)

if __name__ == "__main__":
    start_time = time.time()

    runner = Task2()    
    runner.read_data()
    runner.get_top_rated_categories()
    runner.write_output()

    end_time = time.time()

    print("Total time taken: {}".format(end_time - start_time))

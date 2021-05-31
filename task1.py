from pyspark import SparkContext
import json
import sys
import time
import re


class Task1:
    def __init__(self):
        self.sc = SparkContext.getOrCreate()
        self.sc.setLogLevel("ERROR")
        self.input_file = sys.argv[1]
        self.output_file = sys.argv[2]
        self.stopwords_file = sys.argv[3]
        self.year = sys.argv[4]
        self.top_m = int(sys.argv[5])
        self.top_n = int(sys.argv[6])
        print("====== LOGS =====")
        print("Input file: {}".format(self.input_file))
        print("Output file: {}".format(self.output_file))
        print("Stopwords file: {}".format(self.stopwords_file))
        print("Year: {}".format(self.year))
        print("top_m: {}".format(self.top_m))
        print("top_n: {}".format(self.top_n))
        self.output = {}

    @staticmethod
    def wrangled(review):
        punctuations = set(['(', '[', ',', '.', '!', '?', ':', ';', ']', ')'])
        ret = []
        for c in review:
            if (c.isalpha() or c == " ") and c not in punctuations:
                ret.append(c)
        return "".join(ret)

    def read_data(self):
        with open(self.stopwords_file) as f:
            self.stopwords = set(map(lambda x: x.strip(), f.readlines()))

        self.data = self.sc.textFile(self.input_file)\
                        .map(lambda line: json.loads(line))
        self.data.cache()
        self.reviewers = self.data.map(lambda review: (review["user_id"], 1))
        self.reviewers.cache()

        self.reviews = self.data.map(lambda review: review["text"])\
                                    .map(lambda review: Task1.wrangled(review))\
                                    .map(lambda line: line.lower())\
                        
    def count_reviews(self):
        self.output["A"] = self.data.count()
    
    def count_reviews_in_year(self, year):
        self.output["B"] = self.data.filter(lambda review: review["date"][:4] == year).count()
    
    def count_distinct_reviewers(self):
        self.output["C"] = self.reviewers.distinct().count()

    def top_reviewers(self, top_m):
        self.output["D"] = self.reviewers.reduceByKey(lambda x, y: x + y).sortBy(lambda x: (-x[1], x[0])).take(top_m)

    def frequent_words(self, stopwords, top_n):
        self.output["E"] = self.reviews\
            .flatMap(lambda review: review.split())\
            .filter(lambda word: word not in stopwords)\
                .map(lambda word: (word, 1))\
                    .reduceByKey(lambda x, y: x + y)\
                        .sortBy(lambda x: -x[1])\
                            .take(top_n)

    def write_output(self):
        print(self.output)
        with open(self.output_file, "w") as f:
            json.dump(self.output, f)


if __name__ == "__main__":
    start_time = time.time()
    runner = Task1()    
    runner.read_data()

    # Task A
    runner.count_reviews()

    # Task B
    runner.count_reviews_in_year(runner.year)

    # Task C
    runner.count_distinct_reviewers()

    # Task D
    runner.top_reviewers(runner.top_m)

    # Task E
    runner.frequent_words(runner.stopwords, runner.top_n)

    runner.write_output()
    end_time = time.time()
    
    print("Total time taken: {}".format(end_time - start_time))

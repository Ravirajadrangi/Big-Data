from pyspark import SparkConf, SparkContext
from pyspark.mllib.util import MLUtils
from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import Vectors
import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import SparseVector
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import  Rating
import random
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
import matplotlib.pyplot as plt

def parseRating(line):
    parts = line.strip().split("::")
    # userID, movieID, rating
    return (int(parts[0]),int(parts[1]),float(parts[2]))

# Step 1 - create spark context
conf = SparkConf().setAppName("Kmeans-Content").set("spark.executor.memory","4g").set("spark.storage.memoryFraction","0")
sc = SparkContext()

# Step 2 - Load in input file
data = MLUtils.loadLibSVMFile(sc, "/Users/David/spark-1.6.0-bin-hadoop2.6/tutorial/data/movielens/medium/movie_features_dataset.dat")

#movieIDs == labels
labels = data.map(lambda x: x.label)
features = data.map(lambda x: x.features)

#for every feature in features:
#   f' = (f-mean)/std

# Step 3 - standardize the data with unit values and 0 mean
#scaler = StandardScaler(withMean = True, withStd = True).fit(features)
scaler = StandardScaler(withMean=False, withStd=True).fit(features)
data2 = labels.zip(scaler.transform(features))
#data2 = labels.zip(scaler.transform(features.map(lambda x: Vectors.dense(x.toArray()))))

#(movieID vector)
numFeatures = len(data2.values().take(1)[0])
#print("Type of data2:", type(data2)) #RDD
#print("Type of data2.values():",  type(data2.values())) #pipelinedrdd
#print("Sample:", data2.values().take(1)[0])

#splitting up the data
xvalidate,test = data2.randomSplit([.90,.10])
partition = xvalidate.count()/10
xdata = xvalidate.collect()
j = 0
bestModel = None
bestK = 0
minError = sys.maxint
k=10
maxIterations = 10
runs = 5
epsilon=0.00001
errorDict = {z: [] for z in range(10,20)}
for x in range(1,10):
    valList = xdata[j:j+partition]
    trainList = list(set(xdata) - set(valList))
    j+=partition
    validation = sc.parallelize(valList)
    training = sc.parallelize(trainList)
    for k in range(10,20):#+range(50,100)+range(100,200):
        model = KMeans.train(training.values(), k, maxIterations, runs)
        error = model.computeCost(training.values())
        errorDict[k] += [error]

list1 = []
list2 = []
for key in errorDict:
    error = sum(errorDict[key])/len(errorDict[key])
    list1.append(key)
    list2.append(error)
    if error < minError:
        bestK = key
        minError = error

print(list1)
print(list2)
plt.plot(list1, list2)
plt.xlabel("Key")
plt.ylabel("Error")
plt.show()
print("Best K: ", bestK)

k = bestK
model = KMeans.train(training.values(), k, maxIterations, runs)

#data2.map(lambda x: (random.randInt(1,10),x))
#train = data2.filter(lambda x: if x[0] in range(1,9))
#print("Training Dataset Size:", training.count())
#print("Validation Dataset Size:", validation.count())
#print("Test Dataset Size:", test.count())

clusterCenters = model.clusterCenters
trainingClusterLabels = training.map(lambda x: (model.predict(x[1]),x[0]))
print(trainingClusterLabels.collect())
# RDD where each item is (clusterID, movieID)

# given a movie find the appropriate cluster

# recommendaiton for user'
# moviesLiked <- user' liked
# for m' in moviesLiked:
#   clusterLabel = use KMeans to predict the cluster for m'
# most frequent clusterLabel

#open ratings file and select a user rating
ratings = sc.textFile("/Users/David/spark-1.6.0-bin-hadoop2.6/tutorial/data/movielens/medium/ratings.dat").map(parseRating)

ratingsByUser = ratings.map(lambda x: (x[0],(x[1],x[2])))
ratingsByUser = ratingsByUser.groupByKey().map(lambda x: (x[0],list(x[1]))).collect()
#ratingsByUser -> RDD of (key=>userId, values=>Sparse/DenseVector)

# lets do a test for 1 user
user = ratingsByUser[0] # look at 1 user
# get the ratings of movies the user liked, i.e. movies with rating = 5
userHighRatings = [movieRating for movieRating in user[1] if movieRating[1] == 5]
singleRating = userHighRatings[0]
clusterId = model.predict(data2.lookup(singleRating[0])[0])
samplesInRelevantCluster = trainingClusterLabels.lookup(clusterId)
print(samplesInRelevantCluster)

sc.stop()

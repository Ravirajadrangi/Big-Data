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

def vectorize(ratings, numMovies):
    return ratings.map(lambda x : (x[0]-1, (x[1]-1, x[2])))\
           .groupByKey()\
           .mapValues(lambda x: SparseVector(numMovies,x))

def parseRating (line):
    # userID:: movieID:: rating::timestamp
    parts = line.strip().split("::")
    return long(parts[3])%10 , (int (parts[0]),int(parts[1]),float(parts[2]) )

def loadRatings (sc, MLDir):
    return sc.textFile(join (MLDir,"ratings.dat")).map(parseRating)

if __name__=="__main__":
    if(len(sys.argv) !=2):
        print("Usage: /path to spark/bin/spark-submit name.py movieDir")
        
    # setting the conf
    conf = SparkConf().setAppName("KMeans Collaborative")
    sc = SparkContext(conf=conf)

    movieLenHomeDir = sys.argv[1]

    # loading the data from file
    ratings = loadRatings(sc, movieLenHomeDir)
    print("Type of ratings obj", type(ratings)) # RDD - PipelinedRDD
    print("Count of ratings:", ratings.count())
    print("Sample ratings:", ratings.take(1))

    # ratings RDD (timestamp, (userid, movieid, ratings))

    # number of users
    numUsers = ratings.values().map(lambda x:x[0]).max()+1
    
    # number of movies
    numMovies = ratings.values().map(lambda x:x[1]).max()+1

    # transform the data into matrix
    # hard to represent as matrix, so represent it as sparse vectors
    
    ratingsSV = vectorize(ratings.values() , numMovies)
    data = ratingsSV.collect()
    # model = KMeans.train(ratingsSV.values(), 10, maxIterations=20,runs=10)
    bestModel = None
    bestK = 0
    minError = sys.maxint
    xvalidate = data[:int(len(data)*.9)]
    test = data[int(len(data)*.9)+1:]
    partition = len(xvalidate)/10
    xdata = xvalidate
    j = 0
    bestModel = None
    bestK = 0
    minError = sys.maxint
    k=10
    maxIterations = 10
    runs = 5
    epsilon=0.00001
    errorDict = {z: [] for z in range(10,20)}
    
##    for x in range(1,10):
##    
##        for k in range(10,20):#+range(50,100)+range(100,200):
##            model = KMeans.train(ratingsSV.values(), 10, maxIterations=20,runs=10)
##            error = model.computeCost(ratingsSV.values())
##            if error < minError:
##                bestModel = model
##                bestK = k

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
        
    # model.save(sc, "KMeansModelCollaborative")
    # model = KMeansModel.load(sc, "KMeansModelCollaborative")

    # how to do predict
    user = ratingsSV.values().take(1)[0]# sample of 1 from the dataset
    label = model.predict(user) # which cluster this user belong
    print("Predicted Label:", label)
    #==> id between 1 and 10
    clusterCenters = model.clusterCenters
    print("clusterCenters", clusterCenters[label])
    movieId = 4
    print("Predicted Value:", clusterCenters[label][movieId])
    # len(clusterCenters) => 10
    # clusterCenters[0] => a list of ratings, len(clusterCenters[0]) => numMovies
    # clusterCenters[0][5]

    sc.stop()

>>> sc
<pyspark.context.SparkContext at 0x109929d50>

>>> rdd1 = sc.textFile("ratings.csv")
>>> rdd1.take(10)
[u'userId,movieId,rating,timestamp',
 u'1,122,2.0,945544824',
 u'1,172,1.0,945544871',
 u'1,1221,5.0,945544788',
 u'1,1441,4.0,945544871',
 u'1,1609,3.0,945544824',
 u'1,1961,3.0,945544871',
 u'1,1972,1.0,945544871',
 u'2,441,2.0,1008942733',
 u'2,494,2.0,1008942733']

>>> rdd1.count() - 1
24404096
#This is the total number of records.

>>> rdd2 = rdd1.map(lambda x: x.split(',')[0]).distinct()

>>> rdd2.count() - 1
259137
#This is the number of unique users.
>>> rdd3 = rdd1.map(lambda x: x.split(',’)[1]).distinct()
>>> rdd3.count() - 1
39443
#This is the total number of unique movies.
>>> rdd4 = rdd1.map(lambda x: x.split(',’)[2]).distinct()
>>> mapper = “mapper.py"
>>> import mapper
>>> mapper
<module ‘mapper’ from ‘mapper.py'>
>>> Input = rdd4.collect()
>>> kv = mapper(Input)
>>> res = sc.parallelize(kv).sortByKey(False)
>>> res.take(5)
[(5.0, 132900), (5.0, 138172), (5.0, 138504), (5.0, 138568), (5.0, 138620)]
#The average ratings from highest

>>> rdd6 = rdd1.map(lambda x: (x.split(',')[0], 1)).reduceByKey(lambda a, b: a + b)
>>> rdd7 = rdd6.filter(lambda x: x[1] <= 10)
>>> rdd8 = rdd6.filter(lambda x: x[1] > 10 and x[1] <= 20)
>>> rdd9 = rdd6.filter(lambda x: x[1] > 20 and x[1] <= 30)
>>> rdd10 = rdd6.filter(lambda x: x[1] > 30 and x[1] <= 40)
>>> rdd11 = rdd6.filter(lambda x: x[1] > 40 and x[1] <= 50)
>>> rdd12 = rdd6.filter(lambda x: x[1] > 50 and x[1] <= 200)
>>> rdd7.count() - 1
41270
#41270 users rated up to 10 movies.
>>> rdd8.count()
64252
#64252 users rated up 10 to 20 movies.
>>> rdd9.count()
26447
#26447 users rated up 20 to 30 movies.

>>> rdd10.count()
17307
#17307 users rated up 30 to 40 movies.
>>> rdd11.count()
13014
#13014 users rated up 40 to 50 movies.
>>> rdd12.count()
66092
#66092 users rated up 50 to 200 movies.
>>> rdd14 = rdd1.map(lambda x: (x.split(',')[1], x.split(',')[0])).groupByKey()
>>> rdd15 = rdd1.map(lambda x: (x.split(',')[0], x.split(',')[1])).groupByKey()
>>> input1 = rdd14.collect()
>>> input2 = rdd15.collect()
>>> helper = "helper.py"
>>> import helper
>>> helper
<module ‘helper’ from ‘helper.py'>
>>> reducer = “reducer.py"
>>> import reducer
>>> reducer
<module ‘helper’ from ‘helper.py'>
>>> result = reducer(input1, input2)

------------
def mapper(input):
    D = dict()
    for i in input:
        if i[0] != 'movieId':
            k = int(i[0])
            if k in D:
                D[k][0] += float(i[1])
                D[k][1] += 1.0
            else:
                D[k] = list()
                D[k].append(float(i[1]))
                D[k].append(1.0)
    d = dict()
    for k in D:
        d[k] = round(D[k][0]/D[k][1],1)
    kv = [(d[k], k) for k in d]
    return kv

------------
def helper(Input1, Input2):
    D = dict()
    for i in Input1:
        D[i] = 0
    for j in Input2:
        if j in D:
            D[j] += 1
    L = list()
    for k in D:
        if D[k] > 0:
            L.append(k)
    return L

------------
def reducer(Input1, Input2):
    D = dict()
    S = set()
    for movies in Input1:
        i = 0
        while i < len(Input1[movies]):
            viewer = Input1[movies][i]
            if len(Input2[viewer])>= 5:
                j = 0
                while i != j and j < len(Input1[movies]) and (i,j) not in S and (j,i) not in S:
                    viewer2 = Input1[movies][j]
                    if len(Input2[viewer2]) >= 5:
                        S.add((i,j))
                        #print(viewer, viewer2)
                        #print("test", Input2[viewer], Input2[viewer2])
                        L = helper3(Input2[viewer], Input2[viewer2])
                        if len(L) == 5:
                            D[(i+1,j+1)] = L
                    j += 1
            i += 1
    return D

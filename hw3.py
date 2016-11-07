>>> sc

<pyspark.context.SparkContext at 0x1099d7d50>

>>> rdd1 = sc.textFile("higgs-social_network.edgelist.from.LT.4000.txt")
>>> rdd1.count()

354951

>>> rdd1.take(10)
[u'1 2’, u'1 3’, u'1 4’, u'1 5’, u'1 6’, u'1 7’, u'1 8’, u'1 9’, u'1 10’, u'1 11']
>>> rdd2 = rdd1.map(lambda x: x.split(" ")).map(lambda x: (int(x[0]), int(x[1])))

>>> mapper = “mapper.py"
>>> import mapper
>>> mapper
<module 'mapper' from 'mapper.py'>

>>> Input = rdd2.collect()
>>> kv = mapper(Input)
>>> rdd3 = sc.parellelize(kv)
>>> rdd3.count()
3999
>>> output = “output.py"
>>> import output
>>> output
<module ‘output’ from ‘output.py'>
>>> result = output(kv)
>>> res = sc.parallelize(result).saveAsTextFile("output.txt")

#define a function to group users with their recommendation list
def mapper(Input):
     Map = dict()
     #initialize a dictionary to group people the same user is following
     for pair in input:
          if pair[0] in Map:
          #if there is this user as Map’s one key, append its following user to Map’s value list
               Map[pair[0]].append(pair[1])
          else:
          #if Map does not have this user as a key, put it in and include its following user to its value list
               Map[pair[0]] = [pair[1]]
     #group completed, look for recommendations
     #use a new dictionary to save the recommendations for each user
     newMap = dict()
     for elem in Map:
          #all the people one user is following saved at fos, one temporary list variable
          #use a set to collect recommendations users to avoid duplicates, this is also a temporary variable
          fos = Map[elem]
          Set = set()
          for fo in fos:
               #if someone that the user is following doesn’t have an entry in the Map, we cannot find people it’s following
               #skip it to avoid dictionary’s key error
               if fo not in Map:
                    continue
               else:
               #among the people the user is following, look for those people’s following users, and add them to the set
                    for rec in Map[fo]:
                         #if the user being followed is the original user, or it’s someone that the original user is following, skip it
                         if rec == elem or rec in fos:
                              continue
                         #if not, add the user to the set
                         Set.add(rec)
     #transform the recommendations set to list, add it with the original user ID as value and key to the newMap
     newMap[elem] = list(Set)
     #return a new list that contains all users with their recommendation list
     kv = [(x, newMap[x]) for x in newMap]
     return kv


#define a function to generate the final output
def output(Input):
     List = [1,27,31,137,3113]
     res = dict()
     #search for users in list within the Input array and save the result in the res dictionary
     for input in Input:
          for elem in List:
               if input[0] == elem:
                      res[elem] = input[1]
     #sort the keys in res to print out users with recommendations in ascending order
     keys = res.keys()
     keys.sort()
     result = list()
     for key in keys:
          result.append((key, "-should-follow->", res[key]));
     return result

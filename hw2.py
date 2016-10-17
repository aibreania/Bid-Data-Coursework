bible = sc.textFile("ascii_bible.txt")


filter = bible.flatMap(lambda line: line.split(" ")).collect()


import re


def filter1(list):
    i = 0
    while(i < len(list)):
        if bool(re.search(r'[,.?;:!]', list[i])):
            if bool(re.search(r'[A-Za-z]', list[i])):
                list[i] = re.sub('[,.?;:!]', '', list[i])
            else:
                list[i] = re.sub('[,.?;!]', '', list[i])
        list[i] = list[i].replace('\t', ' ').lower()
        i += 1
    return list


filter = filter1(filter)


filter = sc.parallelize(filter)


filter = filter.flatMap(lambda line: line.split(' ')).collect()


def filter2(list):
    list = [str for str in list if len(str) >= 1]
    return list


filter = filter2(filter)


filter = sc.parallelize(filter)


mapper = filter.map(lambda a: (a, 1))


groups = mapper.groupByKey()


reducer = groups.mapValues(len).collect()


def search(list, rng):
    i = 0
    result = [0 for x in range(1, rng+1)]
    while i < len(list):
        if len(list[i][0]) <= rng:
            result[len(list[i][0])-1] += list[i][1]
        i += 1
    print "output 1"
    for j in range(len(result)):
        print "number-of-words-with-"+str(j+1)+"-character:",result[j]


search(reducer, 10)


def uniqueword(list, rng):
    i = 0
    result = {}
    while i < len(list):
        if len(result) == rng[1]+1 - rng[0]:
            break
        t = list[i]
        tlen = len(t[0])
        if result.get(tlen) == None and tlen >= rng[0] and tlen <= rng[1] and t[1] < 4:
            result[tlen] = t
        i += 1
    print "output 2                                 word  frequency"
    for i in range(rng[0], rng[1]+1):
        if result.get(i):
            t = result.get(i)
            print "one unique word with " + str(i) + " characters: " + t[0] + "   " + str(t[1])


uniqueword(reducer,[10,30])

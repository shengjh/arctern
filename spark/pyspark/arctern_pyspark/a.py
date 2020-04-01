with open("/home/shengjh/pyspark-profile/cluster2/1000000/timmer.txt", "r") as f:
    while True:
        text = f.readline()
        if not text:
           break
        text = text.strip('\n')
        r = text.split(" ")
        print(r[1], end=",")

import time
import pandas as pd
words_list = []

start_time= time.time()
with open('really_big_text.txt') as f:
    for i in f.readlines():
        words = i.split()
        words_list = words_list + map(lambda x:(x,1),words)

df = pd.DataFrame(words_list,columns=['word','count'])\
                    .groupby('word').sum()\
                    .sort('count',ascending=False)


print "TIME: %s"%(time.time()-start_time)
print df

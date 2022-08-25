from functools import reduce
from multiprocessing import Pool
from collections import Counter
from timeit import default_timer as timer
import pandas as pd
from util import *

# Create dataset
cols = ['sentiment','id','date','query_string','user','text']
df = pd.read_csv("testdata.manual.2009.06.14.csv", encoding='latin1', names=cols)
data = df['text'].tolist()

# Word counter function
def word_counter(text):
    
    cnt = Counter()
    for text in data:
        words = text.split()
        cleaned = map(clean_word, words)
        filtered = filter(word_not_in_stopwords, cleaned)
        cnt.update(filtered)
    return cnt


# MapReduce
def mapper(text):
    words = text.split()
    cleaned = map(clean_word, words)
    filtered = filter(word_not_in_stopwords, cleaned)
    return Counter(filtered)


def reducer(cnt1, cnt2):
    cnt1.update(cnt2)
    return cnt1


def chunk_mapper(chunk):
    
    mapped = map(mapper, chunk)
    return reduce(reducer, mapped)


if __name__ == "__main__":

    # # Basic implementation
    # start = timer()
    # print(word_counter(data).most_common(10))
    # end = timer()
    # print(end - start)

    # # MapReduce implementation
    # start = timer()
    # mapped = map(mapper, data)
    # reduced = reduce(reducer, mapped)
    # print(reduced.most_common(10))
    # end = timer()
    # print(end - start)

    # Parallelised MapReduce
    no_chunks = 2
    chunks = list(chunkify(data, len(data) // no_chunks))

    start = timer()
    pool = Pool(no_chunks)
    mapped = pool.map(chunk_mapper, chunks)
    reduced = reduce(reducer, mapped)
    print(reduced.most_common(10))

    end = timer()
    print(end - start)
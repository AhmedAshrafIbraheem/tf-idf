from math import log10
from pyspark import SparkContext
from re import findall


def split_filter(doc: str):
    terms = findall("dis_[^ ]*_dis|gene_[^ ]*_gene", doc)
    doc_num = doc.split(' ', 1)[0]
    return int(doc_num), terms


def flatmap_terms(doc):
    doc_num = doc[0]
    size = len(doc[1])
    terms = []
    for term in doc[1]:
        terms.append(((doc_num, size, term), 1))
    return terms


def tf_idf(file_name):
    sc = SparkContext("local", "Spark_Project")
    docs_rdd = sc.textFile(file_name)

    docs_num = docs_rdd.count()
    # Phase1
    # Input: [Doc1, Doc2, ]
    doc_terms = docs_rdd.map(split_filter)
    # Output: [(Doc#, [terms]) .... ]
    # <<<<< End Phase1

    # >>>>> Phase2
    # Input: [(Doc#, [terms]) .... ]
    doc_term_1 = doc_terms.flatMap(flatmap_terms)
    # Output: [((Doci, doci_size, termj), 1) .... ]

    # Input: [((Doci, doci_size, termj), 1) .... ]
    doc_term_freq = doc_term_1.reduceByKey(lambda x, y: x + y)
    # Output: [((Doci, doci_size, termj), total_freq) .... ]
    # <<<<< End Phase2

    # >>>>> Phase3
    # Input: [((Doci, doc_size, termj), total_freq) .... ]
    tf = doc_term_freq.map(lambda x: (x[0][2], (x[0][0], x[1] / x[0][1])))
    # Output: [(termj, (Doci, total_freq / sizei)) ...]
    # <<<<< End Phase3

    # >>>>> Phase4
    # Input: [((Doci, doc_size, termj), total_freq) .... ]
    term_1 = doc_term_freq.map(lambda x: (x[0][2], 1))
    # Output: [(termj, 1) .. ]

    # Input:  [(termj, 1) .. ]
    term_freq = term_1.reduceByKey(lambda x, y: x + y)
    # Output: [(termj, doc_freq) .. ]
    # <<<<< End Phase4

    # >>>>> Phase5
    # Input: [(termj, doc_freq) .. ]
    idf = term_freq.mapValues(lambda x: log10(docs_num / x))
    # Output: [(termj, log(D/doc_freq)) ....]
    # <<<<< End Phase5

    # Input: tf [(termj, (Doci, total_freq / sizei)) ...]
    # Input: idf [(termj, log(D/doc_freq)) ....]
    tf_join_idf = tf.join(idf)
    # Output: [(termj, ((doci, tf), idf)) ....]

    # >>>>> Phase6
    # Input: [(termj, ((doci, tf), idf)) ....]
    tf_idf_separated = tf_join_idf.mapValues(lambda x: [(x[0][0], x[0][1] * x[1])])
    # Output: [(termj, [(doci, tf-idf)]) ...]

    # Input: [(termj, [(doci, tf-idf)]) ...]
    tf_idf_grouped = tf_idf_separated.reduceByKey(lambda x, y: x + y)
    # Output: [(termj, [(doci, tf-idf), ....]) ...]
    # <<<<< End Phase6

    printers = tf_idf_grouped.collect()
    for printer in printers:
        print(printer)

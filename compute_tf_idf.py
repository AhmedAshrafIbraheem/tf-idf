from math import log10, sqrt
from pyspark import SparkContext
from re import findall
from pathlib import Path

convention_dir_name = "tf_idf"


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


def check_if_dir_exist():
    convention_dir_path = Path(convention_dir_name)
    return convention_dir_path.exists()


def delete_dir_if_exist():
    import shutil
    convention_dir_path = Path(convention_dir_name)
    if convention_dir_path.exists():
        shutil.rmtree(convention_dir_path)


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

    delete_dir_if_exist()
    tf_idf_grouped.saveAsTextFile(convention_dir_name)


def splitter(term: str):
    loc = term.index("'", 2)
    name = term[2: loc]

    ret = []
    docs = term[loc + 4: -2].split(', ')
    for i in range(0, len(docs), 2):
        ret.append((int(docs[i][1:]), float(docs[i+1][:-1])))

    return name, ret


def query(query_term: str):
    if not check_if_dir_exist():
        print("No Documents has been read Yet")
        return

    sc = SparkContext.getOrCreate()
    tf_idf_grouped = sc.textFile(convention_dir_name).map(splitter)

    term_tuple = tf_idf_grouped.filter(lambda x: query_term.__eq__(x[0])).take(1)
    if len(term_tuple) < 1:
        print("Term Not Found")
        return

    term_tuple = term_tuple[0]
    term = {}
    term_size = 0
    for doc in term_tuple[1]:
        term[doc[0]] = doc[1]
        term_size += doc[1] * doc[1]
    term_size = sqrt(term_size)

    def compute_similarity(current):
        dot_product = 0
        current_size = 0
        for element in current[1]:
            current_size += element[1] * element[1]
            if element[0] in term:
                dot_product += term[element[0]] * element[1]
        current_size = sqrt(current_size)
        return dot_product / current_size / term_size, current[0]

    similarities = tf_idf_grouped.map(compute_similarity).sortByKey(False)
    for term in similarities.collect():
        print(term)

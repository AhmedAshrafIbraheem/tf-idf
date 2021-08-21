# tf-idf & similarity queries (Big DATA Class 2nd Project)


## 1 MapReduce: Step-by-step design

Apache Spark is the main framework we are using in this project. First, we must connect with the framework using the SparkContext() function in the pyspark library. Our file is read using the textFile() function and then the total number of documents is counted using the count() function in Spark. After doing so, we begin our 6 phases of MapReduce.


**Phase 1**

In Phase 1, we are getting rid of non-term words. According to the sample in the project description, we must only search for terms in the form ‘dis_xyz_dis’ and
‘gene_abc_gene’. In this phase, our input is an RDD that contains all documents and our output is (the document_i and the terms in that document).

  * Map:
      
        Input: [Doc1, Doc2, ]
        
        Output: [(Doc_i, [terms]) .... ]


**Phase 2**

In Phase 2, we take in the output of Phase 1, which is document_i to its terms and we are using the flatMap() function in pyspark then reduce to return an RDD
(Resilient Distributed Dataset) with the document number, the size of the document, and the specific terms in the document matched to the total frequency (TF) of the term.

  * Map:
  
        Input: [(Doc_i, [terms]) .... ]
  
        Output: [((Doc_i, Doc_i size, term_j), 1) .... ]
  
  * Reduce:
    
        Input: [((Doc_i, Doc_i size, term_j), 1) .... ]
    
        Output: [((Doc_i, Doc_i size, term_j), total_freq) .... ]
 
 
**Phase 3**

In Phase 3, we take in the output of Phase 2, where we will then use another map() function that outputs each term matched to a document number and its total
frequency in the document divided by the size of the document. We are using the formula provided in the lecture.
  
  * Map:
    
        Input: [((Doc_i, Doc_i size, termj), total_freq) .... ]
    
        Output: [(termj, (Doc_i, total_frequency / Doc_i size)) ...] => ​ TF
    
    
**Phase 4**

In Phase 4, we are taking in the input from Phase 2, not Phase 3, in the format [((Doc_i, Doc_i size, term_j), total_freq) .... ] where we are going to use the map() function then reduce to output an array of the terms to its document frequency (How many documents this term appears in).

  * Map:
    
        Input: [((Doc_i, Doc_i size, term_j), total_freq) .... ]
    
        Output: [(term_j, 1) .. ]
  
  * Reduce:
    
        Input: [(term_j, 1) .. ]
    
        Output: [(term_j, Doc_freq_j) .. ]
    

**Phase 5**

In Phase 5, we will use the output of Phase 4 as our input which is in the format [(term_j, Doc_freq).. ], which is basically how many documents each term appears
in. In this phase, we will be mapping the values using the mapValue() function. This function passes each value in the key-value pair RDD through a map function
without changing the keys. We will also perform the operating ​ log10 (Documents_number / Doc_freq_j).
  
  * Map:

        Input: [(term_j, Doc_freq_j) .. ]
    
        Output: [(term_j, log10(D/Doc_freq_j)) ....] => ​ IDF


*After this, we will be joining (TF and IDF) the terms based on their documents and we get the output of [(term_j, ((Doc_i, TF_ji), IDF_j)) ....]. => Combiner*


**Phase 6**

In Phase 6, this is the last phase that takes in the output of Phase 5. We will again use the mapValues() function which passes each value in the key-value pair RDD through a map function without changing the keys then reduce resulting in the output [(term_j, [(Doc_i1, TF-IDF_ji1), (Doc_i2, TF-IDF_ji2) ...]) . . . . ]. This is the phase where we get our TF-IDF.

  * Map:
    
        Input: [(term_j, ((Doc_i, TF_ji), IDF_j)) ....]
    
        Output: [(term_j, (Doc_i, TF-IDF_ji)) ...]
  
  * Reduce:
    
        Input: [(term_j, (Doc_i, TF-IDF_ji)) .. ]
    
        Output: [(term_j, [(Doc_i1, TF-IDF_ji1), (Doc_i2, TF-IDF_ji2) ...]) . . . . ]


*Phase 6 output (RDD) is saved onto disk*



## 2 Query

Once input is given (query_term). First saved data is loaded into RDD. Then, loaded RDD is mapped into the state it was in after Phase 6. Then filtering is done, as to search for query_term. If found, we save all its TF_IDF values into a dictionary. Then mapping is done to compute similarities using the saved dictionary.

  * Map:
    
        Input: [(term_j, [(Doc_i1, TF-IDF_ji1), (Doc_i2, TF-IDF_ji2) ...]) . . . . ]
    
        Output: [(similarity_term_j, term_j) ...]

Then sorting is done in descending using sortByKey(False) Or Top(10) function is used to get the highest 10 similarities.




## 3 How to Run

1 Make Sure python3, pyspark (Need Java 11) is installed.

2 Once the program is running, 3 kinds of input is expected

    *  tf_idf
    *  query [term_name]
    *  exit
    Anything else is refused

* case *tf_idf* => is basically the first part of the project, it is used to compute the values of tf-idf for each term and then save that into file

* case *query [term_name]* => is the second part of the project, it is used to read the file saved by tf_idf, then compute similarities between the given term and all other terms, the sort DESC and print

* case *exit* => is to exit the application

3 Enjoy Our App






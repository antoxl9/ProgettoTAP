from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.dataframe import DataFrame
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf
import numpy as np
import time
import json
import os
import re



###new 
## for data
import pandas as pd  #1.1.5
import numpy as np  #1.21.0

## for plotting
import matplotlib.pyplot as plt  #3.3.2

def elaborate(batch_df: DataFrame, batch_id: int):
  #print(batch_df)
    batch_df.show(truncate=False)

    
    # nlp = spacy.load("en_core_web_sm")
    # doc = nlp(batch_df)

    # # from text to a list of sentences
    # lst_docs = [sent for sent in doc.sents]
    # print("tot sentences:", len(lst_docs))


    # # take a sentence
    # i = 1
    # lst_docs[i]


    # for token in lst_docs[i]:
    #     print(token.text, "-->", "pos: "+token.pos_, "|", "dep: "+token.dep_, "")

    # from spacy import displacy

    # displacy.render(lst_docs[i], style="dep", options={"distance":100})

    # for tag in lst_docs[i].ents:
    #     print(tag.text, f"({tag.label_})") 

    # displacy.render(lst_docs[i], style="ent")


    # ## extract entities and relations
    # dic = {"id":[], "text":[], "entity":[], "relation":[], "object":[]}

    # for n,sentence in enumerate(lst_docs):
    #     lst_generators = list(textacy.extract.subject_verb_object_triples(sentence))  
    #     for sent in lst_generators:
    #         subj = "_".join(map(str, sent.subject))
    #         obj  = "_".join(map(str, sent.object))
    #         relation = "_".join(map(str, sent.verb))
    #         dic["id"].append(n)
    #         dic["text"].append(sentence.text)
    #         dic["entity"].append(subj)
    #         dic["object"].append(obj)
    #         dic["relation"].append(relation)


    # ## create dataframe
    # dtf = pd.DataFrame(dic)
    # print("ma")
    # print(dic)
    # print("ma")

    # ## example
    # dtf[dtf["id"]==i]

    # ## extract attributes
    # attribute = "DATE"
    # dic = {"id":[], "text":[], attribute:[]}

    # for n,sentence in enumerate(lst_docs):
    #     lst = list(textacy.extract.entities(sentence, include_types={attribute}))
    #     if len(lst) > 0:
    #         for attr in lst:
    #             dic["id"].append(n)
    #             dic["text"].append(sentence.text)
    #             dic[attribute].append(str(attr))
    #     else:
    #         dic["id"].append(n)
    #         dic["text"].append(sentence.text)
    #         dic[attribute].append(np.nan)

    # dtf_att = pd.DataFrame(dic)
    # dtf_att = dtf_att[~dtf_att[attribute].isna()]

    # ## example
    # dtf_att[dtf_att["id"]==i]
    # print("le")
    # doc = {"entity": dtf["entity"], "target": dtf["object"], "relation": dtf["relation"] }
    # print(doc)
    # ## create full graph
    # G = nx.from_pandas_edgelist(dtf, source="entity", target="object", 
    #                             edge_attr="relation", 
    #                             create_using=nx.DiGraph())

    # '''
    # print(G)

    # ## plot
    # plt.figure(figsize=(15,10))

    # pos = nx.spring_layout(G, k=1)
    # node_color = "skyblue"
    # edge_color = "black"

    # nx.draw(G, pos=pos, with_labels=True, node_color=node_color, 
    #         edge_color=edge_color, cmap=plt.cm.Dark2, 
    #         node_size=2000, connectionstyle='arc3,rad=0.1')

    # nx.draw_networkx_edge_labels(G, pos=pos, label_pos=0.5, 
    #                         edge_labels=nx.get_edge_attributes(G,'relation'),
    #                         font_size=12, font_color='black', alpha=0.6)
    # plt.show()
    # '''

    # from neo4j import GraphDatabase
    # import networkx as nx
    # import pandas as pd

    # # Assuming you have a NetworkX graph called G

    # # Convert the graph to a pandas DataFrame
    # df = pd.DataFrame(G.edges(data=True), columns=['entity', 'object', 'relation'])

    # # Connect to the Neo4j database
    # uri = "bolt://localhost:7687"
    # username = "neo4j"
    # password = "password"


    # driver = GraphDatabase.driver(uri, auth=(username, password))

    # # Create a session
    # with driver.session() as session:
    #     # Create nodes
    #     nodes = set(df['entity']).union(set(df['object']))
    #     for node in nodes:
    #         session.run(f"CREATE (:Node {{name: '{node}'}})")

    #     # Create relationships
    #     for _, row in df.iterrows():
    #         entity = row['entity']
    #         object = row['object']
    #     # relation = row['relation']
    #         relation = row['relation']['relation']  # Extract the value of the 'relation' key
    #         relation = relation.replace("'", "\\'")  # Escape single quotation marks
    #         session.run(f"MATCH (n1:Node {{name: '{entity}'}}), (n2:Node {{name: '{object}'}}) CREATE (n1)-[:RELATION {{relation: '{relation}'}}]->(n2)")

    #     # session.run(f"MATCH (n1:Node {{name: '{entity}'}}), (n2:Node {{name: '{object}'}}) CREATE (n1)-[:{relation}]->(n2)")
    #     # session.run(f"MATCH (n1:Node {{name: '{entity}'}}), (n2:Node {{name: '{object}'}}) CREATE (n1)-[:RELATION {{relation: '{relation}'}}]->(n2)")



'''
def get_spark_session():
    spark_conf = SparkConf() \
        .set('spark.streaming.stopGracefullyOnShutdown', 'true') \
        .set('spark.streaming.kafka.consumer.cache.enabled', 'false') \
        .set('spark.streaming.backpressure.enabled', 'true') \
        .set('spark.streaming.kafka.maxRatePerPartition', '100') \
        .set('spark.streaming.kafka.consumer.poll.ms', '512') \
        .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1') \
        .set('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint')
    return spark_conf
'''

sc = SparkContext(appName="PythonStructuredStreamsKafka")
spark = SparkSession(sc)
sc.setLogLevel("WARN")

'''
spark_session = SparkSession.builder \
    .appName('astrokg') \
    .config(conf=spark_conf) \
    .getOrCreate()
'''


#spark = get_spark_session()
topic = "datitranscript"
kafkaServer = "kafkaserver:9092"

# Read messages from Kafka
df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafkaServer) \
    .option('subscribe', topic) \
    .load()
    
   

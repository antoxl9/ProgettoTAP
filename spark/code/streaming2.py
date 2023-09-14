from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StringType, BinaryType
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType


import spacy  #3.5.0
from spacy import displacy
import textacy  #0.12.0
## for graph
import networkx as nx  #3.0 (also pygraphviz==1.10)
import numpy as np


# Define a function to process the collected data
def process_data(df, batch_id):
    print("batch id:" + str(batch_id))
   # print("dataframe" + str(df))
    # Collect the DataFrame and extract the string value
    if df.count() > 0:
       
        string_value = df.select("value").collect()[0][0]
        print("string value" + str(string_value))

        if string_value != None:
            extract_graph(string_value)
        else:
            print("None data, passed analysis")
    else:
        print("no data fetched")


import spacy
import textacy.extract
import pandas as pd
import networkx as nx
from neo4j import GraphDatabase

def extract_graph(txt):
    # Load the spaCy model outside the function to avoid reloading it for each call
    nlp = spacy.load("en_core_web_sm")

    # Tokenize the text once
    doc = nlp(txt)
    lst_docs = list(doc.sents)  # Convert generator to list

    # Create a single session for Neo4j to improve efficiency
    uri = "bolt://neo4j:7687"
    username = "neo4j"
    password = "password"
    driver = GraphDatabase.driver(uri, auth=(username, password))

    with driver.session() as session:
        # Create nodes and relationships in Neo4j
        create_nodes_and_relationships(session, lst_docs)

    driver.close()

def create_nodes_and_relationships(session, lst_docs):
    cypher_queries = []
    entity_set = set()
    relations = []

    for n, sentence in enumerate(lst_docs):
        # Extract entities and relations
        entities = list(textacy.extract.entities(sentence, include_types={"DATE"}))
        triples = list(textacy.extract.subject_verb_object_triples(sentence))

        # Create nodes for entities
        for entity in entities:
            entity_text = entity.text
            entity_set.add(entity_text)
            cypher_queries.append(f"CREATE (:Node {{name: '{entity_text}'}})")

        # Create relationships
        for triple in triples:
            subj = "_".join(map(str, triple.subject))
            obj = "_".join(map(str, triple.object))
            relation = "_".join(map(str, triple.verb))
            relations.append((subj, obj, relation))

    # Join cypher queries and execute them in batches
    cypher_query = "\n".join(cypher_queries)
    session.run(cypher_query)

    # Create a DataFrame for relationships
    df = pd.DataFrame(relations, columns=['entity', 'object', 'relation'])

    # Create relationships in Neo4j
    for _, row in df.iterrows():
        entity = row['entity']
        obj = row['object']
        relation = row['relation']
        relation = str(relation).upper()

        print("TESTING:" + "entity:" + str(entity) + "object: " + str(obj) + "relation: " + str(relation) )

        custom_rel_type = relation
        query = (
                "MATCH (n1:Node {name: $entity}), (n2:Node {name: $object})"
                f"CREATE (n1)-[:{custom_rel_type} {{relation_property: $relation}}]->(n2)"
            )
        session.run(query, entity=entity, object=obj, relation=relation, custom_rel_type=custom_rel_type)







# Kafka configuration
kafka_bootstrap_servers = "kafkaserver:9092"
kafka_topic = "datitranscript"

sc = SparkContext(appName="AstroKG")
spark = SparkSession(sc)
sc.setLogLevel("WARN")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# query = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
    # Add more fields as needed
])

# Cast the message received from kafka with the provided schema
# df = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json("value", schema).alias("data")) \
#     .select("data.*")


# Parse the Kafka message key and value
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# # Apply the schema to the value
# df = df.select(from_json("value", schema).alias("data")).select("data.*")


query = df.selectExpr("CAST(value AS STRING) AS value") \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(lambda batch_df, batch_id: process_data(batch_df, batch_id)) \
    .start()
    

query.awaitTermination()

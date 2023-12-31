## for data
import pandas as pd  #1.1.5
import numpy as np  #1.21.0

## for plotting
import matplotlib.pyplot as plt  #3.3.2

## for text
import wikipediaapi  #0.5.8
import nltk  #3.8.1
import re   

## for nlp
import spacy  #3.5.0
from spacy import displacy
import textacy  #0.12.0

## for graph
import networkx as nx  #3.0 (also pygraphviz==1.10)

## for timeline
import dateparser #1.1.7

nlp = spacy.load("en_core_web_sm")
doc = nlp(txt)

# from text to a list of sentences
lst_docs = [sent for sent in doc.sents]
print("tot sentences:", len(lst_docs))


# take a sentence
i = 1
lst_docs[i]


for token in lst_docs[i]:
    print(token.text, "-->", "pos: "+token.pos_, "|", "dep: "+token.dep_, "")

from spacy import displacy

displacy.render(lst_docs[i], style="dep", options={"distance":100})

for tag in lst_docs[i].ents:
    print(tag.text, f"({tag.label_})") 

displacy.render(lst_docs[i], style="ent")


## extract entities and relations
dic = {"id":[], "text":[], "entity":[], "relation":[], "object":[]}

for n,sentence in enumerate(lst_docs):
    lst_generators = list(textacy.extract.subject_verb_object_triples(sentence))  
    for sent in lst_generators:
        subj = "_".join(map(str, sent.subject))
        obj  = "_".join(map(str, sent.object))
        relation = "_".join(map(str, sent.verb))
        dic["id"].append(n)
        dic["text"].append(sentence.text)
        dic["entity"].append(subj)
        dic["object"].append(obj)
        dic["relation"].append(relation)


## create dataframe
dtf = pd.DataFrame(dic)
print("ma")
print(dic)
print("ma")

## example
dtf[dtf["id"]==i]

## extract attributes
attribute = "DATE"
dic = {"id":[], "text":[], attribute:[]}

for n,sentence in enumerate(lst_docs):
    lst = list(textacy.extract.entities(sentence, include_types={attribute}))
    if len(lst) > 0:
        for attr in lst:
            dic["id"].append(n)
            dic["text"].append(sentence.text)
            dic[attribute].append(str(attr))
    else:
        dic["id"].append(n)
        dic["text"].append(sentence.text)
        dic[attribute].append(np.nan)

dtf_att = pd.DataFrame(dic)
dtf_att = dtf_att[~dtf_att[attribute].isna()]

## example
dtf_att[dtf_att["id"]==i]
print("le")
doc = {"entity": dtf["entity"], "target": dtf["object"], "relation": dtf["relation"] }
print(doc)
## create full graph
G = nx.from_pandas_edgelist(dtf, source="entity", target="object", 
                            edge_attr="relation", 
                            create_using=nx.DiGraph())

from neo4j import GraphDatabase
import networkx as nx
import pandas as pd

# Convert the graph to a pandas DataFrame
df = pd.DataFrame(G.edges(data=True), columns=['entity', 'object', 'relation'])

# Connect to the Neo4j database
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"


driver = GraphDatabase.driver(uri, auth=(username, password))

# Create a session
with driver.session() as session:
    # Create nodes
    nodes = set(df['entity']).union(set(df['object']))
    for node in nodes:
        session.run(f"CREATE (:Node {{name: '{node}'}})")

    cypher_queries = []

    # Create nodes
    nodes = set(df['entity']).union(set(df['object']))
    for node in nodes:
        cypher_queries.append(f"CREATE (:Node {{name: '{node}'}})")

    cypher_query = "\n".join(cypher_queries)
    session.run(cypher_query)

    # Create relationships
    for _, row in df.iterrows():
        entity = row['entity']
        object = row['object']
        relation = row['relation']['relation']
        relation = str(relation).upper()

        custom_rel_type = relation
        query = (
            "MATCH (n1:Node {name: $entity}), (n2:Node {name: $object})"
            f"CREATE (n1)-[:{custom_rel_type} {{relation_property: $relation}}]->(n2)"
        )
        session.run(query, entity=entity, object=object, relation=relation, custom_rel_type=custom_rel_type)

driver.close()



from neo4j import GraphDatabase, Driver, Result
from streamlit import secrets
from json import load
import pandas as pd


def openGraph() -> Driver:
    """ """
    s = secrets
    driver = GraphDatabase.driver(s["NEO4J_URI"], auth=(s["NEO4J_USER"], s["NEO4J_PASSWORD"]))
    return driver


def neo4jTest():
    """ """
    graph = openGraph()
    res = graph.execute_query(
        "MATCH (n) RETURN n.Name AS text LIMIT 1"
    )
    graph.close()
    print(res)


def createNodeList(node_label:str)->list[dict]:
    """ """
    label_dicts = {'atu':{'file':'data/atu.json', 'keys':["atu","title","description"]}}
    label_dict = label_dicts[node_label]
    with open(label_dict['file'],"r",encoding='utf-8') as f:
        item_list = load(f)
    return [ { k : v for k, v in x.items() if k in label_dict['keys'] } for x in item_list]


def createNodeSet(node_label:str)->Result:
    """ """
    graph = openGraph()
    nodes = createNodeList(node_label)
    with graph.session(database="neo4j") as session:
        session.run("""
                    WITH $nodes AS batch
                    UNWIND batch AS node
                    MERGE (n:{})
                    SET n = node
                    """.format(node_label), nodes=nodes,
                        )

from neo4j import GraphDatabase, Driver, Result
from streamlit import secrets
from json import load


label_dicts = {
    'atu': {'file':'data/atu.json', 
            'props':["atu","title","description"],
            'motifs':{'target':'motif', 'props':{'relationGloss': "has motif", 'inverseGloss':"motif in"}}},
    'motif': {'file':'data/tmi.json', 
            'props':["motif","description","additional_description"]},
    'ref': {'file':'data/citations.json', 
            'props':["ref","citation"]},
                      }


def openGraph() -> Driver:
    """ """
    s = secrets
    driver = GraphDatabase.driver(s["NEO4J_URI"], auth=(s["NEO4J_USER"], s["NEO4J_PASSWORD"]))
    return driver


def createNodeList(node_label:str)->list[dict]:
    """ """
    label_dict = label_dicts[node_label]
    with open(label_dict['file'],"r",encoding='utf-8') as f:
        item_list = load(f)
    return [ { k : v for k, v in x.items() if k in label_dict['props'] and v != ""} for x in item_list]


def createNodeSet(node_label:str)->Result:
    """ """
    graph = openGraph()
    nodes = createNodeList(node_label)
    with graph.session(database="neo4j") as session:
        res:Result = session.run("""
                        WITH $nodes AS batch
                        UNWIND batch AS node
                        CREATE (n:{})
                        SET n = node
                        """.format(node_label), nodes=nodes,
                            )
    graph.close()
    return res


def creatRelSet(node_label:str, relation: str)->Result:
    """ """

createNodeSet('ref')
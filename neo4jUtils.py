from neo4j import GraphDatabase, Driver, Result
from streamlit import secrets
from json import load
from log import f_logger


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


def cleanTargets(targs:list[str], base_props:dict)->list[str]:
    """ """
    final = []
    for s in targs:
        if (any(char.isdigit() for char in s) and 
            any(char.isalpha() for char in s) and 
            "Type" not in s and "," not in s):
            s_dict = {'id': s, 'props': base_props}
            if "cf" in s.lower():
                s_dict['props'] = {'relationGloss': "has similar motifs to", 'inverseGloss':"similar to motifs in"}
            noise = ["Cf.", "cf.", "e.g.", "ff.", " "]
            for n in noise:
                s = s.replace(n,"")
            s = s.strip(".")
            s_dict['id'] = s
            if  ";" in s:
                sp = s.split(";")
                for spl in sp:
                    s_dict['id'] = spl
                    final.append(s_dict)
            if "–" in s:
                sp = s.split("–")
                pref:str = s[0]
                start_raw = sp[0]
                end_raw = sp[1]
                start = start_raw[1:] if start_raw[0].isalpha() else start_raw
                end = end_raw[1:] if end_raw[0].isalpha() else end_raw
                if "." in end:
                    e_split = end.split(".")
                    end = e_split[-1]
                    pref = pref + ".".join(e_split[:-1]) + "."
                    if "." in start:
                        s_split =  start.split(".")
                        start = s_split[-1]
                    else:
                        start = 0
                print(s,start,end)
                for i in range(int(start), int(end) + 1):
                    r_dict:dict = s_dict.copy()
                    r_dict['id'] = start_raw if i == 0 else pref + str(i)
                    final.append(r_dict)
            else:
                final.append(s_dict)
    return final


def creatRelSet(source_label:str, relation: str)->Result:
    """ """
    graph = openGraph()
    label_dict:dict = label_dicts[source_label]
    logger = f_logger()
    rel_count:int = 0
    update:int = 0
    missed_targets:dict = {}
    with open(label_dict['file'],"r",encoding='utf-8') as f:
        item_list:list[dict] = load(f)
    with graph.session(database="neo4j") as session:
        for i in item_list:
            source:str = i[source_label]
            targets:list[str] = i.get(relation)
            if targets is not None:
                rel_def = label_dict[relation]
                props = rel_def['props']
                targets = cleanTargets(targets, props)
                target_label = rel_def['target']
                results, summary, keys = session.run("""
                                WITH $targets AS targets
                                UNWIND targets AS target
                                MATCH (s:{} {{ {}:$source }})
                                MATCH (t:{} {{ {}:target.id }})
                                CREATE (s)-[r:{}]->(t)
                                SET r = target.props
                                RETURN t['{}']
                                """.format(source_label, source_label, 
                                        target_label, target_label, 
                                        relation, target_label), 
                                source=source, 
                                targets=targets
                                    ).to_eager_result()
                c = summary.counters.relationships_created
                rel_count += c
                update += c
                if update > 1000:
                    logger.success("{} relationships so far.".format(rel_count))
                    update = 0
                res = set([r[keys[0]] for r in results])
                targs = set([x['id'] for x in targets])
                missing = targs - res 
                if len(missing) > 0:
                    logger.warning("{} was expected but not created for {}.".format(missing, source))
                    for m in missing:
                        missing_sources:list = missed_targets.get(m)
                        if missing_sources is None:
                            missed_targets[m] = [source]
                        else:
                            missing_sources.append(source)
                            missed_targets[m] = missing_sources
    graph.close()
    logger.success("{} relationships so far.".format(rel_count))
    return missed_targets

print(creatRelSet("atu", "motifs"))
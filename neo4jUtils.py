from neo4j import GraphDatabase, Driver, Result
from streamlit import secrets
from json import load, dump
from log import f_logger
from uuid import uuid4 as getUUID


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


def createATUClass(atuClass, superclass):
    """ """
    graph = openGraph()
    with graph.session(database="neo4j") as session:
        _, summary, _ = session.run("""
                        CREATE (n:class)
                        SET n = $node
                        WITH n
                        MATCH (s:class { uuid:$super })
                        CREATE (n)-[:superclass {relationGloss: "subclass of", inverseGloss:"superclass of"}]->(s)
                        """, node=atuClass, super=superclass
                            ).to_eager_result()
    graph.close()
    return summary


def classifyATUs(atu_dict:dict):
    """ """
    classified:int = 0
    graph = openGraph()
    for k, v in atu_dict.items():
        with graph.session(database="neo4j") as session:
            _, summary, _ = session.run("""
                            WITH $atus AS atus
                            UNWIND atus AS atu
                            MATCH (a:atu { atu:atu }), (c:class { title:$cls })
                            CREATE (a)-[:class {relationGloss: "member of", inverseGloss:"includes"}]->(c)
                            """, atus=v, cls=k
                                ).to_eager_result()
            classified += summary.counters.relationships_created
    graph.close()
    return classified


def classifyRetiredATUs():
    """ """
    cuuid = str(getUUID())
    graph = openGraph()
    with graph.session(database="neo4j") as session:
        _, summary, _ = session.run("""
                        CREATE (u:class { title:"Discontinued ATU", uuid: $cuuid } )
                        WITH u
                        MATCH (s:class { title:"ATU" })
                        CREATE (u)-[:superclass {relationGloss: "subclass of", inverseGloss:"superclass of"}]->(s)
                        WITH u
                        MATCH (a:atu { description: "Combined with another type as per title." })
                        CREATE (a)-[:class {relationGloss: "member of", inverseGloss:"includes"}]->(u)
                        """, cuuid=cuuid).to_eager_result()
        classified:int = summary.counters.relationships_created
    graph.close()
    return classified


def getRetiredATUs(graph:Driver):
    """ """
    with graph.session(database="neo4j") as session:
        results, _, _ = session.run("""
                        MATCH (n:class { title:"Discontinued ATU" })<--(a:atu)
                        RETURN a.atu AS discontinued, a.title AS title
                        """).to_eager_result()
    return results


def cleanRetiredATUs(atu_results:list):
    troublemakers:dict = {}
    links:dict = {}
    for result in atu_results:
        atu:str = result.get('discontinued')
        title:str = result.get('title')
        spl:list[str] = title.split(".")
        if  spl[1] != "":
            troublemakers[atu] = title
        if spl[0].find("See Type") != -1:
            clean_spl = spl[0].replace("See Type", "")
            if clean_spl[0:2] == "s ":
                spl_spl = clean_spl.replace("s", "").split(",")
                for s in spl_spl:
                    if s != "":
                        links[atu] = s.strip(". ")
            elif atu.find("–") != -1:
                troublemakers[atu] = title
            else:
                links[atu] = clean_spl.strip(". ")
    file_name:str = "data/disc_atu_troublemakers.json"
    with open(file_name, 'w', encoding="utf-8") as f:
        dump(troublemakers, f, indent=1, ensure_ascii=False)
    return links


def linkRetiredATUs():
    """ """
    graph = openGraph()
    links = cleanRetiredATUs(getRetiredATUs(graph))
    with graph.session(database="neo4j") as session:
        _, summary, _ = session.run("""
                        WITH $links as links, keys($links) as ks 
                        UNWIND ks AS k
                        MATCH (d:atu { atu:k }), (a:atu { atu:links[k] })
                        CREATE (d)-[:discontinued {relationGloss: "merged into", inverseGloss:"absorbed"}]->(a)
                        """, links=links
                            ).to_eager_result()
        merged:int = summary.counters.relationships_created
    graph.close()
    return merged


def getMotifs(graph:Driver):
    """ """
    with graph.session(database="neo4j") as session:
        results, _, _ = session.run("""
                        MATCH (m:motif)
                        RETURN m.motif AS motif
                        """).to_eager_result()
    return results


def getMotifLinks(motif_results:list):
    links:dict = {}
    current_tl:str = ""
    for result in motif_results:
        motif:str = result.get('motif')
        spl:list[str] = motif.split(".")
        if motif[1] == "0" and len(spl) == 1:
            current_tl = motif[0]
            links[current_tl] = {}
        else:
            parent:str = ""
            if len(spl) == 1:
                parent = spl[0][0] + "0"
            else:
                end = -2 if spl[-2] == "0" else -1
                parent = ".".join(spl[:end])
            links[current_tl][motif] = parent
    return links


def linkMotifs():
    """ """
    graph = openGraph()
    all_links:dict = getMotifLinks(getMotifs(graph))
    all_linked:int = 0
    for links in all_links.values():
        with graph.session(database="neo4j") as session:
            _, summary, _ = session.run("""
                            WITH $links as links, keys($links) as ks 
                            UNWIND ks AS k
                            MATCH (v:motif { motif:k }), (g:motif { motif:links[k] })
                            CREATE (v)-[:parent {relationGloss: "variant of", inverseGloss:"has variant"}]->(g)
                            """, links=links
                                ).to_eager_result()
            linked:int = summary.counters.relationships_created
            all_linked += linked
    graph.close()
    return all_linked


def classifyTraditions(class_title, traditions)->int:
    """ """
    cuuid = str(getUUID())
    graph = openGraph()
    with graph.session(database="neo4j") as session:
        _, summary, _ = session.run("""
                        CREATE (c:class { title:$class_title, uuid: $cuuid } )
                        WITH c
                        MATCH (s:class {title:"Tradition", uuid:"58e1b5dc-4010-431e-bcf7-d84b62e69d0c"})
                        CREATE (c)-[:superclass {relationGloss: "subclass of", inverseGloss:"superclass of"}]->(s)
                        WITH c, $traditions as ts
                        UNWIND ts AS trad
                        CREATE (t:tradition {title:trad})
                        CREATE (t)-[:class {relationGloss: "member of", inverseGloss:"includes"}]->(c)
                        """, cuuid=cuuid, class_title=class_title, traditions=traditions
                        ).to_eager_result()
        classified:int = summary.counters.relationships_created
    graph.close()
    return classified


def createCitations(atu:str, atu_refs:dict):
    """ """
    graph = openGraph()
    with graph.session(database="neo4j") as session:
        hits, _, _ = session.run("""
                        MATCH (a:atu { atu:$atu } )
                        WITH a, $refs as refs
                        UNWIND keys(refs) AS trad
                        OPTIONAL MATCH (t:tradition { title: trad })
                        WITH a, t, refs[trad] AS refs
                        UNWIND refs as ref
                        MATCH (r:ref {ref: ref.citation} )
                        WITH a, t, r, ref
                        CREATE (a)-[:literature {relationGloss: "has relevant literature", inverseGloss:"concerns or features"}]->
                                    (c:citation:EXP {from: ref.raw})-[:reference {relationGloss: "full citation", inverseGloss:"cited as"}]->(r)
                        WITH c, r, t
                        FOREACH (i in CASE WHEN t IS NOT NULL THEN [1] ELSE [] END |
                                    CREATE (c)-[:tradition {relationGloss: "documents or analyzes", inverseGloss:"is featured in"}]->(t))
                        RETURN r.ref AS ref, t.title AS trad
                        """, atu=atu, refs=atu_refs
                        ).to_eager_result()
    graph.close()
    parsed:dict = {}
    for hit in hits:
        ref = hit.get('ref')
        trad = hit.get('trad')
        k = trad if trad is not None else 'cf'
        if parsed.get(k) is None:
            parsed[k] = [ref]
        else:
            parsed[k].append(ref)
    return parsed


def removeCitations():
    """ """
    graph = openGraph()
    with graph.session(database="neo4j") as session:
        _, summary, _ = session.run("""
                                    MATCH (ts:tradition)
                                    UNWIND ts as t
                                    MATCH (c:citation)-->(t) DETACH DELETE c
                                    MATCH (uc:citation) DETACH DELETE uc
                                    """).to_eager_result()
    graph.close()
    return summary.counters.nodes_deleted


def fixCitations(fixes):
    """ """
    graph = openGraph()
    with graph.session(database="neo4j") as session:
        _, summary, _ = session.run("""
                                    WITH $fixes as fixes
                                    UNWIND keys(fixes) AS fix
                                    MATCH (r:ref { ref:fix })
                                    SET r.ref = fixes[fix]
                                    """, fixes=fixes).to_eager_result()
    graph.close()
    return summary.counters.properties_set


def createAndLinkSubjects(main:str, subs:dict)->int:
    """ """
    muuid:str = str(getUUID())
    graph = openGraph()
    rels:int = 0
    with graph.session(database="neo4j") as session:
        session.run("CREATE (m:subject { title:$main, uuid: $muuid } )", 
                    main=main, muuid=muuid)
        for k, v in subs.items():
            suuid = str(getUUID())
            _, summary, _ = session.run("""
                            MATCH (m:subject { uuid: $muuid } )
                            CREATE (s:subject { title:$sub, uuid: $suuid })-[:parent {relationGloss: "variant of", inverseGloss:"has variant"}]->(m)
                            WITH s, $atus as atus
                            UNWIND atus AS atu
                            MATCH (a:atu {atu:atu})
                            CREATE (a)-[:subject {relationGloss: "involves subject", inverseGloss:"appears in type"}]->(s)
                            """, muuid=muuid, suuid=suuid, sub=k, atus=v
                            ).to_eager_result()
            classified:int = summary.counters.relationships_created
            rels += classified
    graph.close()
    return rels


def createRemarks(remarks:dict):
    """ """
    graph = openGraph()
    with graph.session(database="neo4j") as session:
        _, summary, _ = session.run("""
                        WITH $remarks as remarks
                        UNWIND keys(remarks) AS atu
                        MATCH (a:atu { atu: atu })
                        SET a.remarks = remarks[atu]
                        """, remarks=remarks
                        ).to_eager_result()
    graph.close()
    return {'expected': len(remarks), 'created': summary.counters.properties_set}


def linkCombos(combos:dict):
    """ """
    graph = openGraph()
    with graph.session(database="neo4j") as session:
        _, summary, _ = session.run("""
                        WITH $combos as combos
                        UNWIND keys(combos) AS atu0
                        WITH atu0, combos
                        UNWIND combos[atu0] AS atu1
                        MATCH (s:atu { atu: atu0 })
                        MATCH (t:atu { atu: atu1 })
                        MERGE (s)-[:combo {relationGloss: "sometimes combined with", inverseGloss:"sometimes combined with"}]-(t)
                        """, combos=combos
                        ).to_eager_result()
    graph.close()
    return summary.counters.relationships_created


def linkSubjects(source:str, targets:list[str]):
    """ """
    redundancies:dict = {}
    graph = openGraph()
    with graph.session(database="neo4j") as session:
        matches, summary, _ = session.run("""
                        WITH $source AS subj0, $targets AS targets
                        UNWIND targets AS subj1
                        MATCH (s:subject { title: subj0 })
                        MATCH (t:subject { title: subj1 })
                        MERGE (s)-[:see {relationGloss: "see also", inverseGloss:"see also"}]-(t)
                        RETURN s.title AS source, t.title AS target
                        """, source=source, targets=targets
                        ).to_eager_result()
    graph.close()
    if matches:
        for match in matches:
            redundancies[match.get('target')] = match.get('source')
    return summary.counters.relationships_created, redundancies
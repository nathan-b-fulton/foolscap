from pypdf import PdfReader, PageObject
from json import dump, load
from re import match, Match
from datetime import datetime as time
from alive_progress import alive_bar
from uuid import uuid4 as getUUID
from neo4jUtils import createATUClass, classifyTraditions, createCitations, fixCitations
from log import f_logger


rubrics:list[str] = ["Combinations", "Remarks", "Literature/Variants"]
last_known_atu_fl:float = 0.0


def clean_2_float(mess:str)->int:
    """Turn ATU letters and asterisks into a standardized decimal representation 
    suitable for ordinal comparison."""
    if mess != "":
        g:Match = match(r"([0-9]+)([A-Z\*\–]*)", mess)
        if g is not None and len (g.group(0)) ==  len(mess):
            n:float = float(g.group(1))
            for c in g.group(2):
                o:int = ord(c)
                if o > 42 and o < 91:
                    n = n + o / 100
                else:
                    n = n + o / 10000
            return n
    return -1.0


def atu_p(chunk:str, current_atu:dict):
    """Test if a text chunk appears to be the definition of the next ATU ('True'), 
    not the ATU but the start of a block of recognized structure ('False'), or some 
    other string that is probably an unintentionally separated tail ('None')."""
    atu_fl:float = last_known_atu_fl if current_atu is None else clean_2_float(current_atu.get("atu", last_known_atu_fl))
    lead:str = chunk if chunk == "" else chunk.split()[0]
    lead_fl = clean_2_float(lead)
    if lead_fl >= atu_fl and lead_fl - atu_fl < 200:
        print("{} is an atu ({} >= {}).\n".format(lead, lead_fl, atu_fl))
        return True
    elif lead.strip(":") in rubrics:
        print("{} is not an atu ({} < {}).\n".format(lead, lead_fl, atu_fl))
        return False
    else:
        print("Who knows what {} is ({}/{}) ?\n".format(lead, lead_fl, atu_fl))
        return None


def findNextHead(chunks:list[str], s:int, limiter, atu:dict)->int:
    """Find the index of the next chunk of text that is the start of a recognizable 
     block of structure."""
    for n in range(s+1, len(chunks)):
        if atu_p(chunks[n], atu) is not None:
            return n
    return limiter


def createAtu(chunk:str)->dict:
    """Create an ATU JSON object from a string."""
    atu:dict = {}
    if chunk.find("See Type") == -1:
        sentences:list[str] = chunk.split(".")
        names:list[str] = sentences[0].split()
        atu["atu"] = names[0]
        atu["title"] = " ".join(names[1:])
        atu["description"] = ".".join(sentences[1:]).strip()
        if " Cf" in sentences:
            cf_index = sentences.index(" Cf") + 1
            cross_refs:list[str] = sentences[cf_index].replace(
                "Types ", "").replace("and ", "").strip().split(", ")
            atu["cf"] = cross_refs
        motifs = []
        motif_marked:str = chunk.replace("[","@[").replace("]", "]@")
        mm_list=motif_marked.split("@")
        for m in mm_list:
            if m.startswith("[") and m.endswith("]"):
                motifs = motifs + m.strip("[]").split(", ")
        atu["motifs"] = motifs
    else:
        names:list[str] = chunk.split()
        atu["atu"] = names[0]
        atu["title"] = " ".join(names[1:])
        atu["description"] = "Combined with another type as per title."
    return atu


def cleanExpandCombo(list_str:str)->list[str]:
    """Take a string and parse it into a list of ATU strings, leaving ranges 
    for future processing."""
    clean_str:str = list_str.strip("., ")
    str_list:list[str] = clean_str.split(", ")
    return str_list


def processRubric(rubric:str, content:str, atu:dict)->dict:
    """For each of the defined rubrics of supplemental information, 
    clean a content string and parse it into a provided ATU dictionary."""
    val:str = content.replace(rubric, "").strip(" :")
    match rubric:
        case "Remarks":
            atu['remarks'] = val
        case "Literature/Variants":
            entries:dict = {}
            val_list = val.split("; ")
            cross_refs = []
            for v in val_list:
                entry = v.split(": ")
                if len(entry) == 2:
                    noisy = entry[0].split(". ")
                    if len(noisy) > 1:
                        entries[noisy[-1]] = entry[1]
                        cross_refs.append(". ".join(noisy[0:-2]))
                    else:
                        entries[entry[0]] = entry[1]
                else:
                    cross_refs.append(entry[0])
            if cross_refs != []:
                entries['cf'] = cross_refs
            atu['literature'] = entries
        case "Combinations":
            noise = ["This type is usually combined with ", 
                     "one or more other types, esp. ", "episodes of ", "and "]
            for n in noise:
                val = val.replace(n, "")
            split_val:list[str] = val.split("also ")
            basic_c:int = 0
            if len(split_val) == 2:
                atu['strongCombos'] = cleanExpandCombo(split_val[0])
                basic_c = 1
            atu['combos'] = cleanExpandCombo(split_val[basic_c])
    return atu


def amendAtu(atu:dict, chunk:str)->dict:
    """Parse a chunk of supplemental content (a string) into appropriate entries
    in a provided ATU dictionary."""
    remainder:str = chunk
    r_indices:dict = {}
    for r in rubrics:
        i = chunk.find(r)
        if i != -1:
            r_indices[r] = i
    for s in reversed(rubrics):
        start = r_indices.get(s)
        if start is not None:
            workpiece:str = ""
            if start == 0:
                workpiece = remainder.replace(s, "")
            else:
                split:list[str] = remainder.split(s)
                remainder = split[0]
                workpiece = split[1]
            atu = processRubric(s, workpiece, atu)
    return atu


def atuPDF2list(page: PageObject)->list[str]:
    """Strip text out of a page with some minimal cleaning and structuring."""
    raw:str = page.extract_text(extraction_mode="layout")
    text_list:list[str] = raw.split("\n")
    tl:list[str] = text_list[4:]
    chunks:list[str] = []
    psg:str = ""
    for l in tl:
        if l =='':
            chunks.append(psg.strip().replace("- ", ""))
            psg = ""
        else:
            l = " ".join(l.split())
            psg = psg + ' ' + l
    chunks.append(psg.strip().replace("- ", ""))
    return chunks


def atuList2json(chunks:list[str])->str:
    """Clean and structure text chunks, and save to a JSON file."""
    atu:dict = None
    c:int = 1
    t = len(chunks)
    atus:list = []
    with alive_bar(4809) as bar:
        while c < t:
            next_head = findNextHead(chunks, c, t, atu)
            chunk = " ".join(chunks[c:next_head]) if next_head > c + 1 else chunks[c]
            c = next_head
            new = atu_p(chunk, atu)
            if new:
                if atu is not None:
                    atus.append(atu)
                atu = createAtu(chunk)
            elif atu is not None:
                atu = amendAtu(atu, chunk)
            else:
                print(""" Warning: Supplementary content may have been found 
                      before first ATU was generated. ATU is:\n{}\nContent:\n{}""".format(
                          atu, chunk
                      ))
            bar()
    atus.append(atu)
    file_name:str = "atus_{}.json".format(time.now().date())
    with open(file_name, 'w', encoding="utf-8") as f:
        dump(atus, f, indent=1, ensure_ascii=False)
    return file_name


def atuParser()->str:
    """Parse all ATUs from PDFs and send to JSON file generation utility."""
    chunks:list[str] = []
    volumes:list[dict] = [{'file':"data/ATU1.pdf", 'start': 18, 'end': 622}, # 622
                          {'file':"data/ATU2.pdf", 'start': 9, 'end': 539}] # 539
    for volume in volumes:
        reader = PdfReader(volume['file'])
        pages = reader.pages
        bl = volume['end'] - volume['start']
        with alive_bar(bl) as bar:
            for p in range(volume['start'], volume['end']):
                chunks = chunks + atuPDF2list(pages[p])
                bar()
        reader.close()
    res:str = atuList2json(chunks)
    return res


def formatTrad(trad:str)->list[str]:
    """Perform final formatting for strings representing languages, peoples, 
    or regions in the ATU appendix of such terms."""
    tradList:list[str] =  []
    rawList:list[str] =  trad.split("–")
    for r in rawList:
        tradList.append(r.strip(". "))
    return tradList


def tradsParser()->str:
    """Extract strings representing languages, peoples, or regions in the ATU 
    appendix of such terms from a PDF and save to a JSON file."""
    continents:list[dict] = []
    reader = PdfReader("data/ATU3.pdf") #  'start': 31, 'end': 135
    trads_page:PageObject = reader.pages[9]
    raw:str = trads_page.extract_text(extraction_mode="layout")
    text_list:list[str] = raw.split("\n")
    tl:list[str] = text_list[2:]
    continent:dict = {}
    tradString:str = ""
    for l in tl:
        l = l.replace(": Schmidt 1989", "")
        if l ==''and continent != {}:
            continent['traditions'] = formatTrad(tradString)
            continents.append(continent)
            tradString = ""
        elif l.find(":") != -1:
            continent = {'continent': l.strip().replace(":", "")}
        elif l.find("–") != -1:
            tradString = tradString + l
    continent['traditions'] = formatTrad(tradString)
    continents.append(continent)
    reader.close()
    file_name:str = "data/traditions.json"
    with open(file_name, 'w', encoding="utf-8") as f:
        dump(continents, f, indent=1, ensure_ascii=False)
    return file_name


def sourcesParser()->str:
    """Extract strings representing citation/supplemental sources in the ATU 
    appendix of references from a PDF and save to a JSON file."""
    reader = PdfReader("data/ATU3.pdf")
    s:int = 31
    e:int = 136
    bl:int = e - s
    citations:list[dict] = []
    reference: str = ""
    cite_string:str = ""
    with alive_bar(bl) as bar:
        for p in range(s, e):
            cite_page:PageObject = reader.pages[p]
            raw:str = cite_page.extract_text(extraction_mode="layout")
            text_list:list[str] = raw.split("\n")
            tl:list[str] = text_list[2:]
            for l in tl:
                l_clean:str = l.replace("   ", "")
                if l == "":
                    continue
                elif l[0] != " ":
                    if cite_string != "":
                        citations.append({'ref': reference, 'citation': cite_string.strip()})
                    l_list = l_clean.split(":")
                    reference = l_list[0]
                    cite_string = ":".join(l_list[1:])
                else:
                    cite_string = cite_string + l_clean
            citations.append({'ref': reference, 'citation': cite_string})
            bar()
    reader.close()
    file_name:str = "data/citations.json"
    with open(file_name, 'w', encoding="utf-8") as f:
        dump(citations, f, indent=1, ensure_ascii=False)
    return file_name


def parseATUClass(raw:str, level:int)->dict:
    """ """
    atuClass:dict = {'uuid': str(getUUID()), 'subclasses': []}
    working:str = raw.strip("*")
    if level > 1:
        spl:list[str] = working.split()
        bounds:list = spl[-1].split("-")
        working = " ".join(spl[:-1])
        atuClass['lower'] = bounds[0]
        atuClass['upper'] = bounds[1]
    atuClass['title'] = working
    return atuClass


def buildATUTree()->dict:
    """ """
    atu:dict = {'title': "ATU", 'uuid': str(getUUID()), 'subclasses': [], 'nodeLabel': "atu"}
    currentL1:dict = {}
    currentL2:dict = {}
    with open("data/ATU_outline.txt", 'r', encoding="utf-8") as f:
        for c in f:
            level:int = c.count("*")
            currentClass:dict = parseATUClass(c, level)
            parent:dict = None
            match level:
                case 1:
                    parent = atu
                    currentL1 = currentClass
                case 2:
                    parent = currentL1
                    currentL2 = currentClass
                case 3:
                    parent = currentL2
            parent['subclasses'].append(currentClass)
    return atu


def recurseATUTree(tree:dict, superclass:str)->int:
    """ """
    atuClass:dict = { k : v for k, v in tree.items() if k != "subclasses"}
    summary = createATUClass(atuClass, superclass)
    message = "For {}, created {} and linked {}.".format(atuClass['title'], 
                                                         summary.counters.nodes_created,
                                                         summary.counters.relationships_created)
    print(message)
    for c in tree['subclasses']:
        recurseATUTree(c, atuClass['uuid'])
    return atuClass


def parseATUClassLight(raw:str)->dict:
    """ """
    atuClass:dict = {}
    spl:list[str] = raw.strip("*").split()
    bounds:list = spl[-1].split("-")
    atuClass['lower'] = bounds[0]
    atuClass['upper'] = bounds[1]
    atuClass['title'] = " ".join(spl[:-1])
    return atuClass


def getLeafClasses()->dict:
    """ """
    with open("data/ATU_outline.txt", 'r', encoding="utf-8") as f:
        leaf_classes:list[dict] = []
        prev_line:str = ""
        prev_level:int = 0
        for c in f:
            level:int = c.count("*")
            match level:
                case 3:
                    leaf_classes.append(parseATUClassLight(c))
                case _:
                    if prev_level == 2:
                        leaf_classes.append(parseATUClassLight(prev_line))
            prev_line = c
            prev_level = level
        leaf_classes.append(parseATUClassLight(prev_line))
    return leaf_classes


def attachATUs2Classes()->dict:
    """ """
    leaves = getLeafClasses()
    atu_int:int = 0
    atu = ""
    rels = {}
    with open('data/atu.json',"r",encoding='utf-8') as f:
        atus = iter(load(f))
        for leaf in leaves:
            u:int = int(leaf['upper'])
            cls:str = leaf['title']
            cls_atus:list = []
            while atu_int <= u:
                if atu != "":
                    cls_atus.append(atu)
                atu_dict = next(atus, None)
                if atu_dict is None:
                    atu_int = 2500
                elif atu_dict["description"] == "Combined with another type as per title.":
                    atu = "" 
                else:
                    atu = atu_dict['atu']
                    g:Match = match(r"([0-9]+)([A-Z\*\–]*)", atu)
                    atu_int = int(g.group(1))
            rels[cls] = cls_atus
    return rels


def createTraditions():
    """ """
    total = 0
    with open('data/traditions.json',"r",encoding='utf-8') as f:
        data = iter(load(f))
        for d in data:
            continent = d['continent']
            trads = d['traditions']
            suffix = "an" if continent == "Europe" else "n"
            title = continent + suffix + " Tradition"
            count = classifyTraditions(title, trads)
            total += count
    return total


specs:list[str] = ['AaTh', 'Afghanistan Journal', 'Am Urquell', 'Angeljček', 'Anthropophyteia', 
                   'Archiv für Litteraturgeschichte', 'Archiv für slavische Philologie',  
                   'Béaloideas',  'Bechstein/Uther 1997 I', 'Bechstein/Uther 1997 II', 'Børnenes Blad',
                   'Bll. f. Pomm. Vk.', 'Børnenes Blad', 'Celske slovenske novine', 'EM archive', 
                   'Eigen Volk', 'Fabula', 'Germania', 'Gesta Romanorum', 'Groningen', 'Jacques de Vitry', 
                   'Jacques de Vitry/Frenken', 'Johannes Gobi Junior', 'Kres', 'Kryptádia', 
                   'Laográphia', 'Ljubljanski zvon', 'Mensa philosophica', 'Mir', 'Mot.', 'Naš dom', 
                   'Neerlands Volksleven', 'Notes and Queries', 'Philippe le Picard', 'Philogelos', 
                   'Poggio', 'Roman de Renart', 'Senones, M.', 'Skattegraveren', 'Slovenski gospodar', 
                   'Soča', 'Thrakika', 'Trinkov koledar', 'Vedež', 'Verfasserlexikon', 'Volkskunde', 
                   'Volkskundig Bulletin', 'Vrtec', 'West Virginia Folklore', 'ZfVk.']


def cleanRefs(laundry:str, l)->list:
    """ """
    clean_refs:list[dict] = []
    refs:list[str] = laundry.split(",")
    for r in refs:
        r = r.strip().replace("cf. ", "").replace("Cf. ", "").replace("e.g. ", "")
        citation:str = ""
        for spec in specs:
            if spec in r:
                citation = spec
        if citation == "":
            m = match(r"[A-Z]{2,}|([\w\D/-]+ \d{0,2}[ (]*)([(]forthcoming[)]|\d{4}f*.{0,1})", r)
            if m:
                citation = m.group(0).strip()
            if "f." in citation:
                citation = citation.split("f.")[0] + "f."
            else:
                citation = citation.strip(".")
        if citation == "":
            l.info("Cleaning {} from reference".format(r))
        else:
            clean_refs.append({'raw': laundry, 'citation': citation})
    return clean_refs


def attachATUs2Citations()->dict:
    """ """
    logger = f_logger()
    with open('data/atu.json',"r",encoding='utf-8') as f:
        atus = iter(load(f))
        for i in range(1000): # atus:
            a:dict[str] = next(atus)
            ref_trads:dict = a.get('literature')
            if ref_trads is not None:
                atu_refs:dict[list] = {}
                for t, r in ref_trads.items():
                    trads:list[str] = t.split(",")
                    for tr in trads:
                        trad = tr.strip()
                        if trad == "cf":
                            cf_refs:list[dict] = []
                            for c in r:
                                cf_refs += cleanRefs(c, logger)
                            atu_refs['cf'] = cf_refs
                        else:
                            atu_refs[trad] = cleanRefs(r, logger)
                atu = a['atu']
                citations:dict[list] = createCitations(atu, atu_refs)
                for k, v in atu_refs.items():
                    base = set([i['citation'] for i in v])
                    trad = citations.get(k)
                    if trad is None:
                        logger.warning("For ATU {}, there appear to be no citations for {}. Expected: {}".format(atu, k, base))
                    else:
                        comp = set(trad)
                        if not base ^ comp:
                            logger.info("Exactly correct entries for {} in ATU {}, nice!".format(k, atu))
                        elif base - comp:
                            logger.warning("Expected citation(s) {} to be created for ATU {}, but they are not returned.".format(base - comp, atu))
                        elif comp - base:
                            logger.warning("Unexpected citation(s) {} were returned for {} in ATU {}.".format(comp - base, k, atu))
    return logger.success("Completed all ATUS.")

def auditCitations()->list:
    """ """
    bad:list[str] = []
    with open('data/citations.json',"r",encoding='utf-8') as f:
        refs = iter(load(f))
        for o in refs:
            r = o['ref']
            m = match(r"[A-Z]{2,}|([\w\D/-]+ \d{0,2}[ (]*)([(]forthcoming[)]|\d{4}f*.{0,1})", r)
            if not m:
                bad.append(r)
    return bad


def repairCitations():
    """ """
    broken:dict = {'Anderson1963': 'Anderson 1963', 'Bäckström1845': 'Bäckström 1845', 'Barag1995': 'Barag 1995', 'BinGorion1990': 'Bin Gorion 1990', 'Bolte1892': 'Bolte 1892', 'Brockpähler1980': 'Brockpähler 1980', 'Christiansen1949': 'Christiansen 1949', 'Fielhauer1968': 'Fielhauer 1968', 'Galley1977': 'Galley 1977', 'Gašparíková1981a': 'Gašparíková 1981a', 'Gašparíková1991f.': 'Gašparíková 1991f.', 'Ginsburg1971': 'Ginsburg 1971', 'Hertel1953': 'Hertel 1953', 'Köhler/Bolte1898ff.': 'Köhler/Bolte 1898ff.', 'Kretzenbacher1959': 'Kretzenbacher 1959', 'Legros1964': 'Legros 1964', 'Lescot1940f.': 'Lescot 1940f.', 'Levinsen/Bødker1958': 'Levinsen/Bødker 1958', 'Loewe1918': 'Loewe 1918', 'Lorentz1924': 'Lorentz 1924', 'Macdonald1910': 'Macdonald 1910', 'Maierbrugger1978': 'Maierbrugger 1978', 'Mamiya1999': 'Mamiya 1999', 'Meraklis1963f.': 'Meraklis 1963f.', 'Meyer/Sinninghe1973': 'Meyer/Sinninghe 1973', 'Meyer/Sinninghe1976': 'Meyer/Sinninghe 1976', 'Reinartz1970': 'Reinartz 1970', 'Roberts1966': 'Roberts 1966', 'Rubow1984': 'Rubow 1984', 'Sasaki/Morioka1984': 'Sasaki/Morioka 1984', 'Schenda/Tomkowiak1993': 'Schenda/Tomkowiak 1993', 'Schmid1955': 'Schmid 1955', 'Schwarzbaum1982': 'Schwarzbaum 1982', 'Schwickert1931': 'Schwickert 1931', 'Neumann1968c': 'Neumann 1968c', 'Ortmann/Ragotzky1988': 'Ortmann/Ragotzky 1988', 'Spies1967': 'Spies 1967', 'Stroescu1969': 'Stroescu 1969', 'Vėlius1990': 'Vėlius 1990', 'Weinreich1921': 'Weinreich 1921', 'Wienker-Piepho1992': 'Wienker-Piepho 1992', 'Wossidlo1897ff.': 'Wossidlo 1897ff.', 'Zipes1982': 'Zipes 1982'}
    fixed = fixCitations(broken)
    return fixed

print(attachATUs2Citations())
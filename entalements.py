from io import StringIO, BytesIO
from pandas import DataFrame, read_csv
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse
import streamlit as st
import requests
import zipfile


locc:str = None


@st.cache_data
def getCatalog()->DataFrame:
    """ """
    raw = requests.get("https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv.gz")
    catalog_text:str = raw.content.decode("utf-8")
    return DataFrame(read_csv(StringIO(catalog_text)))


@st.cache_data
def getText(id:int)->list[str]:
    """ """
    mirrors_html = requests.get("http://www.gutenberg.org/robot/harvest?filetypes[]=html")
    mirror_soup = BeautifulSoup(mirrors_html.text, features="html.parser")
    mirror:str = mirror_soup.body.a.get('href')
    host = urlparse(mirror).netloc
    path_base = "/".join(str(id)[:-1])
    path = "{}/{}/{}-h.zip".format(path_base, id, id)
    raw = requests.get(urlunparse(['http', host, path, '', '', '']))
    z = zipfile.ZipFile(BytesIO(raw.content))
    page = z.read(z.namelist()[0]).decode('utf-8')
    page_soup = BeautifulSoup(page, features="html.parser")
    return page_soup.get_text()


with st.sidebar:
    catalog_df:DataFrame = getCatalog()
    if not catalog_df.empty:
        locc = st.selectbox("Please select a Library of Congress Subject Heading", catalog_df['LoCC'].unique())


if locc:
    df = catalog_df.loc[catalog_df['LoCC'] == locc]
    df
    mirror = str(getText(16780))
    mirror

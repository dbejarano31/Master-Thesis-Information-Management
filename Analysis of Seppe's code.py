#!/usr/bin/env python
# coding: utf-8

# # Breaking down Seppe's code

# ## Libraries

# In[1]:


import requests
from bs4 import BeautifulSoup
from bs4 import NavigableString
from htmllaundry import sanitize
from htmllaundry.cleaners import LaundryCleaner
import htmllaundry.utils
import xmltodict
import re
import json
from pprint import pprint
import pandas as pd
from glob import glob
from cachecontrol import CacheControl
from IPython.display import HTML
import unicodedata


# In[2]:


sess = requests.session()
cach = CacheControl(sess)


# ## Cleaning SEC encoding

# In[3]:


CustomCleaner = LaundryCleaner(
            page_structure=False,
            remove_unknown_tags=False,
            allow_tags=['blockquote', 'a', 'i', 'em', 'p', 'b', 'strong',
                        'h1', 'h2', 'h3', 'h4', 'h5', 
                        'ul', 'ol', 'li', 
                        'sub', 'sup',
                        'abbr', 'acronym', 'dl', 'dt', 'dd', 'cite',
                        'dft', 'br', 
                        'table', 'tr', 'td', 'th', 'thead', 'tbody', 'tfoot'],
            safe_attrs_only=True,
            add_nofollow=True,
            scripts=True,
            javascript=True,
            comments=True,
            style=True,
            links=False,
            meta=True,
            processing_instructions=False,
            frames=True,
            annoying_tags=False)


# In[60]:


## The SEC is encoded in CP1252, and it is recommended to use UTF-8 always.
## see: https://www.w3.org/International/questions/qa-what-is-encoding
###### https://www.w3.org/International/articles/definitions-characters/#unicode
###### https://www.w3.org/International/questions/qa-choosing-encodings

def reformat_cp1252(match):
    codePoint = int(match.group(1))
    if 128 <= codePoint <= 159:
        return bytes([codePoint])
    else:
        return match.group()

def clean_sec_content(binary):
    return re.sub(b'&#(\d+);', reformat_cp1252, binary, flags=re.I).decode("windows-1252").encode('utf-8').decode('utf-8')


# In[59]:


## this is to normalize urls, making them more human friendly
def slugify(value):
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s\.\-]', '-', value).strip().lower()
    value = re.sub('[-\s]+', '-', value)
    return value


# ## Cleaning html

# maybe I don't have to use exactly this functions

# In[6]:


def read_html(file):
    with open(file, 'r') as f: return f.read()


# In[7]:


def clean_html(html):
    soup = BeautifulSoup(html)
    if not soup.find('p'):
        for div in soup.find_all('div'):
            div.name = 'p'
    for b in soup.find_all('b'):
        b.name = 'strong'
    for f in soup.find_all('font', style=re.compile('font-weight:\s*bold')):
        f.name = 'strong'
    for footer in soup.find_all(class_=['header', 'footer']): 
        try: footer.decompose()
        except: pass
    san = sanitize(str(soup), CustomCleaner)
    soup = BeautifulSoup(san)
    def decompose_parent(el, parent='p', not_grandparent='table'):
        try:
            parent = el.find_parent(parent)
        except: parent = None
        if not parent: return
        grandparent = parent.find_parent('table')
        if grandparent: return
        parent.decompose()
    for el in soup.find_all(text=lambda x: 'table of contents' == str(x).lower().strip()):
        decompose_parent(el, 'a')
    for el in soup.find_all(text=re.compile(r'^\s*S\-(\d+|[ivxlcdm]+)\s*$')): 
        decompose_parent(el, 'p')
    for el in soup.find_all(text=re.compile(r'^\s*\d+\s*$')): 
        decompose_parent(el, 'p')
    return soup


# ## Defining helper functions

# In[8]:


def pagination_provider_by_element_start_count(find_args, find_kwargs):
    def pagination_provider_by_element_start_count_wrapped(soup, params):
        if soup.find(*find_args, **find_kwargs) is None:
            return None
        params['start'] += params['count']
        return params
    return pagination_provider_by_element_start_count_wrapped


# In[9]:


def params_provider_by_dict(params):
    return lambda : params


# In[10]:


## look for a table, gets rid of HTML tags, removes line breaks
def table_provider_by_summary(summary, header=0, index_col=0):
    return lambda soup: pd.read_html(
        str(soup.find('table', summary=summary)).replace('<br>', '<br>\n'), header=header, index_col=index_col)[0]


# #### Breaking down table_provider_by_summary

# In[39]:


url = base_url.format('/Archives/edgar/data/1035443/0001047469-19-001263-index.html')
url


# In[44]:


soup = BeautifulSoup(cach.get(url).text)
soup


# In[51]:


var= soup.find('table')
var


# In[53]:


var = str(soup.find('table'))
var


# In[54]:


## <BR> tags denote line breaks, so here you get rid of them
var = str(soup.find('table')).replace('<br>', '<br>\n')
var


# In[55]:


var = pd.read_html(str(soup.find('table')).replace('<br>', '<br>\n'), header=0, index_col =0)
var


# In[56]:


var = pd.read_html(str(soup.find('table')).replace('<br>', '<br>\n'), header=0, index_col =0)[0]
var


# In[58]:


print('-'*80)


# In[11]:


def get_sec_table(url,
                  table_provider=None,
                  base_params={}, 
                  params_provider=None,
                  pagination_provider=None,
                  replace_links=True,
                  session=None):
    def return_data_frame(session, url, params, provider):
        request = session.get(url, params=params)
        soup = BeautifulSoup(request.text)
        if replace_links:
            for a in soup.find_all('a'):
                parent = a.find_parent('td')
                if parent: parent.string = a['href']
        df = provider(soup)
        return df, soup
    ####################################################################
    ###if no Session, then we use the base_url to do the pull request###
    ####################################################################
    if session is None:
        session = cach
    if not url.startswith('http://') and not url.startswith('https://'):
        url = base_url.format(url)
    ###############################################################################################    
    ###if the specified parameters are a dictionary, update params with the specified parameters###
    ###############################################################################################
    params = dict(base_params)
    if params_provider:
        if isinstance(params_provider, dict):
            params.update(params_provider)
        else:
            params.update(params_provider())
    ############################################################        
    ### what exactly is the purpose of a pagination provider?###
    ############################################################
    if not pagination_provider:
        df, soup = return_data_frame(session, url, params, table_provider)
        return df
    else:
        data_frames = []
        page_params = dict(params)
        while True:
            df, soup = return_data_frame(session, url, page_params, table_provider)
            data_frames.append(df)
            # Make sure columns retain their names
            data_frames[-1].columns = data_frames[0].columns
            new_params = pagination_provider(soup, page_params)
            if not new_params:
                break
            else:
                page_params.update(new_params)
        return pd.concat(data_frames, sort=False, ignore_index=True)


# ### Breaking down get_sec_table

# #### def_return_dataframe

# In[34]:


for a in soup.find_all('a'):
    print(a)


# In[35]:


for a in soup.find_all('a'):
    parent = a.find_parent('td')
    print(parent)


# #### params_provider

# In[38]:


base_params = {}
params = dict(base_params)
params_provider = {'company': '', 'owner': 'exclude', 'action': 'getcompany'}


# In[ ]:


if params_provider is instance(params_provider, dict):
    params.update(params_provider)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# ## Function to get the documents

# This is a function to get the documents in the filing details page for each filing. See below for an example page.
# 

# In[14]:


url = base_url.format('/Archives/edgar/data/1035443/0001047469-19-001263-index.html')
url


# In[15]:


get_filing_documents = lambda url, summary = 'Document Format Files' : get_sec_table(url,
                                                                                    table_provider = table_provider_by_summary(summary, index_col=None),
                                                                                    pagination_provider = pagination_provider_by_element_start_count(('input',), {'value': 'Next 100'}))


# ## Scraping most recent filings

# For the previous 5 days

# In[12]:


base_url = 'https://www.sec.gov{}'


# In[16]:


def get_current_events(days_before=0, form_type=''):
    soup = BeautifulSoup(cach.get(base_url.format('/cgi-bin/current'), 
                            params={'q1': days_before, 'q2': 0, 'q3': form_type}).text)
    pre = soup.find('pre')
    ls = []
    for line in str(pre).replace('<hr>', '\n').replace('<hr/>', '\n').split('\n'):
        bs_line = BeautifulSoup(line)
        clean_line = '  '.join(item.strip() for item in bs_line.find_all(text=True))
        split_line = [ x.strip() for x in clean_line.split('  ') if x.strip() ]
        split_line += [ a.get('href') for a in bs_line.find_all('a') ]
        if not all(x is None for x in split_line): ls.append(split_line)
    colnames = ls[0] + [ 'link_{}'.format(i) for i in range(max(len(l) for l in ls) - len(ls[0])) ]
    return pd.DataFrame(ls[1:], columns=colnames)


# In[17]:


get_current_events(form_type='8-K').head()


# #### Breaking down get_current_events

# Questions: 
# * What exactly is the goal of splitting every word in the list of rows? 
# 

# In[35]:


# this code goes to the current events page and scrapes the html of the list of documents
soup = BeautifulSoup(cach.get(base_url.format('/cgi-bin/current'), params = {'q1': 0, 'q2':0, 'q3': '8-K'}).text)
soup


# Since the code is super dirty and company names and dates are mixed in the same text, we have split them into new lines.
# 
# We are only interested in the table, so that's why we use soup.find('pre') --> look at the HTML code in the base_url. 

# In[36]:


pre = soup.find('pre')
lines = str(pre).replace('<hr>', '\n').replace('<hr/>', '\n'). split('\n')
lines


# In[38]:


## here instead of having each row of the table between single HTML tags, we make each row separate
for line in lines:
    bs_line = BeautifulSoup(line)
    print(bs_line)


# In[41]:


for line in lines:
    bs_line = BeautifulSoup(line)
    clean_line = '  '.join(item.strip() for item in bs_line.find_all(text=True))
    print(clean_line)


# In[43]:


## the split_line for some reason separates every letter
for line in lines:
    bs_line = BeautifulSoup(line)
    clean_line = '  '.join(item.strip() for item in bs_line.find_all(text=True))
    split_line = [x.strip() for x in clean_line.strip('  ') if x.strip()]
    print(split_line)


# In[44]:


## we then add the links for the forms into each list if they have a link in bs_line
for line in lines:
    bs_line = BeautifulSoup(line)
    clean_line = '  '.join(item.strip() for item in bs_line.find_all(text=True))
    split_line = [x.strip() for x in clean_line.strip('  ') if x.strip()]
    split_line += [a.get('href') for a in bs_line.find_all('a')]
    print(split_line)


# In[46]:


ls=[]
for line in lines:
    bs_line = BeautifulSoup(line)
    clean_line = '  '.join(item.strip() for item in bs_line.find_all(text=True))
    split_line = [x.strip() for x in clean_line.strip('  ') if x.strip()]
    split_line += [a.get('href') for a in bs_line.find_all('a')]
    if not all(x is None for x in split_line): ls.append(split_line)
ls


# ## Downloading SEC documents

# Questions: 
# * What is the purpose of defining a directory? It does not seem to work when I use it as a parameter for download_sec_documents
# 
# * What does the error "index 0 is out of bounds for axis 0 with size 0" mean? I still manage to download the files.

# In[28]:


def download_sec_documents(doc_link):
    contents = clean_sec_content(cach.get(base_url.format(doc_link)).content)
    name = slugify(doc_link)
    with open(name, 'w') as f: f.write(contents)


# #### Downloading 424B5s

# In[29]:


forms = get_current_events(0, '424B5')
for link in forms['link_0']:
    docs = get_filing_documents(base_url.format(link))
    doc_link = docs.loc[docs.Type == '424B5', 'Document'].values[0]
    download_sec_documents(doc_link)


# #### Downloading 8-Ks

# In[84]:


num_days = 1

for p in range(0, num_days):
    print('Scraping day-page:', p)
    forms = get_current_events(p, '8-K')
    for link in forms['link_0']:
        docs = get_filing_documents(base_url.format(link))
        doc_link = docs.loc[docs.Type == '8-K', 'Document'].values[0]
        download_sec_documents(doc_link)


# To download the 8-K I always get the index error above, however when I try to download 424B5 filings this is not a issue. This happens in the filing details page: 
# 
# 8-K example: https://www.sec.gov/Archives/edgar/data/926660/0001193125-20-298746-index.html
# 
# 424B5 example: https://www.sec.gov/Archives/edgar/data/1035443/0001047469-19-001263-index.html
# 

# In[30]:


content = read_html('-archives-edgar-data-1629210-000156459020054792-pzg-424b5.htm')
cleaned = clean_html(content)


# In[31]:


window(str(cleaned))


# ## Summary extraction

# There is a difference between 424B5 and 8-K forms. The 424B5 forms have summary tables that are what the extract_dual_tables function extracts. 8-K forms are entirely text. We need to find a way to select and extract relevant info from the 8-Ks

# In[21]:


def extract_dual_tables(soup):
    dualrows = []
    for tr in soup.select("table tr"):
        row = [td.text.strip() for td in tr.find_all('td')]
        if len(row) != 2:
            continue
        if row[1].strip() == '':
            continue
        if all([row[x] == '' for x in range(0, len(row)-1)]):
            if len(dualrows) > 0 and len(row) == len(dualrows[-1]):
                dualrows[-1][-1] += ' ' + row[-1]
        else:
            dualrows.append(row)
    return dualrows


# In[22]:


from IPython.display import HTML

def window(html):
    s = '<script type="text/javascript">'
    s += 'var win = window.open("", "", "toolbar=no, location=no, directories=no, status=no, menubar=no, scrollbars=yes, resizable=yes, width=780, height=200, top="+(screen.height-400)+", left="+(screen.width-840));'
    s += 'win.document.body.innerHTML = \'' + html.replace("\n",'\\n').replace("'", "\\'") + '\';'
    s += '</script>'
    return HTML(s)


# In[23]:


cleaned.find('p', text=re.compile(r'OFF'))


# In[24]:


def match_by_name_and_regex(name, regex, lowercase=True):
    return lambda el: el.name == name and re.search(regex, el.text.lower() if lowercase else el.text) is not None


# In[32]:


def get_offering_header_candidates(soup):
    return soup.find_all(match_by_name_and_regex('p', r'\s*offering\s*$'))

def get_after_offering_header_tables(header):
    tables = ''
    nextSibling = header.nextSibling
    table_seen = False
    while True:
        if nextSibling is None:
            break
        if type(nextSibling) == NavigableString:   
            if table_seen and str(nextSibling).strip() != '': break
            nextSibling = nextSibling.nextSibling
            continue
        if nextSibling.name != 'table':
            if table_seen and nextSibling.get_text(strip=True) != '': break
            nextSibling = nextSibling.nextSibling
            continue
        table_seen = True
        tables += str(nextSibling)
        if not nextSibling.nextSibling:
            print(nextSibling)
            print(nextSibling.nextSibling)
        nextSibling = nextSibling.nextSibling
    return tables

def extract_offering(soup):
    for header in get_offering_header_candidates(soup):
        tables = get_after_offering_header_tables(header)
        if tables:
            return extract_dual_tables(BeautifulSoup(tables))

        
extract_offering(cleaned)


# In[26]:


window(str(cleaned))


# In[ ]:





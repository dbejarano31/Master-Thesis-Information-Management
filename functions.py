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
from bs4 import Comment
from IPython.display import HTML
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
import datefinder



sess = requests.session()
cach = CacheControl(sess)



def window(html):
    s = '<script type="text/javascript">'
    s += 'var win = window.open("", "", "toolbar=no, location=no, directories=no, status=no, menubar=no, scrollbars=yes, resizable=yes, width=780, height=200, top="+(screen.height-400)+", left="+(screen.width-840));'
    s += 'win.document.body.innerHTML = \'' + html.replace("\n",'\\n').replace("'", "\\'") + '\';'
    s += '</script>'
    return HTML(s)


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



def reformat_cp1252(match):
    codePoint = int(match.group(1))
    if 128 <= codePoint <= 159:
        return bytes([codePoint])
    else:
        return match.group()

def clean_sec_content(binary):
    return re.sub(b'&#(\d+);', reformat_cp1252, binary, flags=re.I).decode("windows-1252").encode('utf-8').decode('utf-8')



def slugify(value):
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s\.\-]', '-', value).strip().lower()
    value = re.sub('[-\s]+', '-', value)
    return value


def read_html(file):
    with open(file, 'r') as f: return f.read()



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




def pagination_provider_by_element_start_count(find_args, find_kwargs):
    def pagination_provider_by_element_start_count_wrapped(soup, params):
        if soup.find(*find_args, **find_kwargs) is None: 
            return None
        params['start'] += params['count'] 
        return params
    return pagination_provider_by_element_start_count_wrapped





def params_provider_by_dict(params):
    return lambda : params



def table_provider_by_summary(summary, header=0, index_col=0):
    return lambda soup: pd.read_html(
        str(soup.find('table', summary=summary)).replace('<br>', '<br>\n'), header=header, index_col=index_col)[0]



def get_sec_table(url,
                  table_provider=None,
                  base_params={}, 
                  params_provider=None,
                  pagination_provider=None,
                  replace_links=True,
                  session=None):
    ### this function returns a tuple of a df with the respective soup element
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
    ### in case you only scrape one page, it will just return the df of the respective page###
    if not pagination_provider:
        df, soup = return_data_frame(session, url, params, table_provider)
        return df
    ### in case you want to scrape multiple pages, create an empty list of dfs and add each df from each 
    ### page to the empty list, at the end you just concatenate all of the dfs
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




get_filing_documents = lambda url, summary = 'Document Format Files' : get_sec_table(url,
                                                                                    table_provider = table_provider_by_summary(summary, index_col=None),
                                                                                    pagination_provider = pagination_provider_by_element_start_count(('input',), {'value': 'Next 100'}))




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



def download_sec_documents(doc_link):
    contents = clean_sec_content(cach.get(base_url.format(doc_link)).content)
    name = slugify(doc_link)
    with open(name, 'w') as f: f.write(contents)
        


def get_header(soup):
    for header in soup.find_all():
        match = re.match(r'RISK\s*FACTORS\s*', header.text.upper(), re.M) 
        if match:
            if header.name == 'p':
                parent = header.parent
                if parent.name == 'body': ## this is to make it easier to get the delimiting header, since it must be a sibling of header
                    return header



def get_delimiter_header(content):
    soup = BeautifulSoup(content, 'html.parser') #we use the html parser to list the headers with the sourceline
    positions = []
    for tag in soup.find_all('a'):
        positions.append(tag.string)
    limit = len(positions)
    listing = []
    for i in range(0, limit-1):
        if positions[i] is not None:
            match = re.match(r'RISK\s*FACTORS\s*', str(positions[i]).upper(), re.M) #we spot the target header and select the header that follows it
            if match:
                if positions[i+1] is not None:
                    listing.append(positions[i+1].upper())
                if positions[i+2] is not None:
                    listing.append(positions[i+2].upper())
    return listing



def get_risk_info(header, content):
    paragraphs = ''
    brother = header.next_sibling
    limit = False
    delimiters = get_delimiter_header(content)
    while brother and limit == False:
        if brother.name == 'p' or brother.name == 'a': # we have to select the tag to use the .get_text attr
            if str(brother.get_text(strip = True)).replace('\n', ' ') in delimiters:
                limit = True
        paragraphs += str(brother)
        brother = brother.next_sibling
    return paragraphs


def get_424b5(soup, content): 
    header = get_header(soup)
    paragraphs = get_risk_info(header, content)
    if paragraphs:
        return paragraphs


def cleanhtml(raw_html):
    cleanr = re.compile(r'<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});|\n')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext


def get_8k_headers(soup):
    headers = []
    for header in soup.find_all():
        match = re.match(r'ITEM.*', header.text.upper())
        if match:
            headers.append(header)
    return headers

def get_8k_delims(soup):
    delims = []
    for header in soup.find_all():
        match = re.match(r'.(D).*EXHIBIT', header.text.upper())
        if match:
            delims.append(str(header).strip().upper())
    return delims

def get_8k_info(headers, soup):
    paragraphs = ''
    delimiters = get_8k_delims(soup)
    for header in headers:
        if header.name == 'p':
            brother = header.next_sibling
            stop = False
            while brother and stop == False:
                if brother.name == 'table':
                    stop = True
                if str(brother).strip().upper() in delimiters:
                    stop = True
                paragraphs += str(brother)
                brother = brother.next_sibling
            return paragraphs
        
def get_8k(soup):
    headers = get_8k_headers(soup)
    delims = get_8k_delims(soup)
    paragraphs = get_8k_info2(headers, soup)
    if paragraphs:
        return paragraphs
    
def get_dates(soup):
    date = []
    for i in soup.find_all():
        string = i.text
        matches = datefinder.find_dates(string)
        try:
            for match in matches:
                date.append(match)
        except:
            pass
    return date[1]
            

def previous_close_and_next_open(tickers, dates):
    """This function obtains, for each pair of ticker and date, the closing price of the ticker during the 
    day before the given date and the opening price of the ticker for the day next to the reference date.
    
    For the inputs:
    tickers: List of tickers, each represented by a string
    dates: List of dates, each represented in the format %Y-%m-%d (2010-01-24)
    
    The output is a pandas dataframe, with as many rows as specified tickers, and columns Reference Date, Previous Close,
    and Next Open."""
    
    results = pd.DataFrame(columns=['Ticker', "Reference Date", "Previous Close", "Next Open"]).set_index('Ticker')
    for i, t in enumerate(tickers):
        #If date falls in weekends, take Friday and Monday
        extra_add = extra_sub = 0
        if datetime.strptime(dates[i], '%Y-%m-%d').isoweekday() == 6:
            extra_add = 1
        elif datetime.strptime(dates[i], '%Y-%m-%d').isoweekday() == 7:
            extra_sub = 1
                
        yesterday = datetime.strptime(dates[i], '%Y-%m-%d') - timedelta(days=1+ extra_sub)
        tomorrow = datetime.strptime(dates[i], '%Y-%m-%d') + timedelta(days=1 + extra_add)
        
        data = yf.download(t, start=yesterday + timedelta(days=1), end=tomorrow + timedelta(days=1))
        
        previous_close = data.iloc[0]['Close']
        next_open = data.iloc[-1]['Open']

        single = pd.DataFrame({"Reference Date":dates[i], "Previous Close":previous_close, "Next Open":next_open}, index=[t])
        results = results.append(single)
    return results
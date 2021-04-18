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

base_url = 'https://www.sec.gov{}'

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
    paragraphs = get_8k_info(headers, soup)
    if paragraphs:
        return paragraphs
    
def get_dates(soup):
    date = []
    for i in soup.find_all():
        string = i.text
        matches = datefinder.find_dates(string)
        try:
            for match in matches:
                if match.date().year in [2020, 2021] and match.date().strftime('%Y-%m-%d')< datetime.today().strftime('%Y-%m-%d'):
                    date.append(match)
        except:
            pass
    return date[1]

def parse10k(filing):
    raw_10k = filing.text
    ##define delimiter patterns to get the regex iterator
    doc_start_pattern = re.compile(r'<DOCUMENT>')
    doc_end_pattern = re.compile(r'</DOCUMENT')
    type_pattern = re.compile(r'<TYPE>[^\n]+')

    doc_start_is = [x.end() for x in doc_start_pattern.finditer(raw_10k)]
    doc_end_is = [x.start() for x in doc_end_pattern.finditer(raw_10k)]

    doc_types = [x[len('<TYPE>'):] for x in type_pattern.findall(raw_10k)]

    document = {}
    ##selecting only the 10-K section
    for doc_type, doc_start, doc_end in zip(doc_types, doc_start_is, doc_end_is):
        if doc_type == '10-K':
            document[doc_type] = raw_10k[doc_start:doc_end]

    #defining patterns for the delimiting sections
    regex = re.compile(r'(>Item(\s|&#160;|&nbsp;)(1A|1B|7A|7|8)\.{0,1})|(ITEM\s(1A|1B|7A|7|8))')
    
    try:
        matches = regex.finditer(document['10-K'])
    
        test_df = pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])

        test_df.columns = ['item', 'start', 'end']
        test_df['item'] = test_df.item.str.lower()

        test_df.replace('&#160;',' ',regex=True,inplace=True)
        test_df.replace('&nbsp;',' ',regex=True,inplace=True)
        test_df.replace(' ','',regex=True,inplace=True)
        test_df.replace('\.','',regex=True,inplace=True)
        test_df.replace('>','',regex=True,inplace=True)

        ##df contains duplicate sections because it also matches the index
        pos_dat = test_df.sort_values('start', ascending=True).drop_duplicates(subset=['item'], keep='last')
        pos_dat.set_index('item', inplace=True)

        item_1a_raw = document['10-K'][pos_dat['start'].loc[pos_dat.index[0]]:pos_dat['start'].loc[pos_dat.index[1]]]

        item_1a_content = BeautifulSoup(item_1a_raw, 'lxml')

        item_1a_cleaned = item_1a_content.get_text("\n\n")
        
    except:
        item_1a_cleaned = 'Nope'
    return cleanhtml(item_1a_cleaned)



def get_10k(link):

    try:
        docs = get_filing_documents(base_url.format(link))
        doc_link = docs.loc[docs.Description == 'Complete submission text file', 'Document'].values[0]
        r = requests.get(base_url.format(doc_link))
        result = parse10k(r)
    except:
        result = 'Nope'

    return result
            

def get_stats2(df, time_delta):
    ticker_list = list(df['Ticker'])
    date_list= list(df['Date Filed'])
    
    names = []
    prev_deltas = []
    next_deltas = []    
    
    for i,j in zip(ticker_list, date_list):
        # calculating the different day limits we are going to use to measure the price change
        past_limit = (datetime.strptime(j, '%Y-%m-%d') + timedelta(days = - time_delta)).strftime('%Y-%m-%d')
        future_limit = future_limit = (datetime.strptime(j, '%Y-%m-%d') + timedelta(days = + time_delta)).strftime('%Y-%m-%d')
        
        # querying data
        past_data = yf.download(tickers = i, start = past_limit, end = j)
        past_sp500 = yf.download(tickers = '^GSPC', start = past_limit, end = j)
        future_data = yf.download(tickers = i, start = j, end = future_limit)
        future_sp500 = yf.download(tickers = '^GSPC', start = j, end = future_limit)
        
        # selecting only the adjusted close prices
        past_data = np.array(past_data['Adj Close'])
        past_sp500 = np.array(past_sp500['Adj Close'])
        future_data = np.array(future_data['Adj Close'])
        future_sp500 = np.array(future_sp500['Adj Close'])
        
        #calculating respective average price changes per time block
        past_deltas = np.nanmean(np.diff(past_data))
        past_deltas_sp500 = np.nanmean(np.diff(past_sp500))
        future_deltas = np.nanmean(np.diff(future_data))
        future_deltas_sp500 = np.nanmean(np.diff(future_sp500))
        
        #normalizing the price changes per time block by the average price change in S&P500
        norm_past_deltas = past_deltas/past_deltas_sp500
        norm_future_deltas = future_deltas/future_deltas_sp500
        
        prev_deltas.append(norm_past_deltas)
        next_deltas.append(norm_future_deltas)
        names.append(i)
        
    dic = {
        'Ticker': names,
        'PrevAvgPriceChange': prev_deltas,
        'PostAvgPriceChange': next_deltas
    }
        
    return pd.DataFrame(dic)

def delta_days_and_current(tickers, dates, delta=7):
    """This function obtains, for each pair of ticker and date, the closing price of the ticker delta days
    after the given date and the closing price of the ticker for the day of the reference date.
    
    For the inputs:
    tickers: List of tickers, each represented by a string. Same length as dates!
    dates: List of dates, each represented in the format %Y-%m-%d (e.g. 2010-01-24)
    delta: Number of days after the reference date from which to obtain the previous price. It can also be a list,
        with as many deltas as desired.
    
    The output is a pandas dataframe, with as many rows as specified tickers, and columns Reference Date, 
    Previous Close, and Current Close."""
    

    if type(delta) == int:
        delta = [delta]
    
    results = {field: [] for field in 
               ['Ticker', "Reference Date", "Current Close"] + \
               [f"Close_Price_{abs(d)}_Days_Before" for d in delta if d < 0] + \
               [f"Close_Price_{d}_Days_Later" for d in delta if d > 0]}
    
    #This unelegant move is because im lazy
    delta = [-d for d in delta]
        
    for i, t in enumerate(tickers):
        #If date falls in weekends, take Friday and Monday
        extra_add = 0
        if datetime.strptime(dates[i], '%Y-%m-%d').isoweekday() == 6:
            extra_add = -1
        elif datetime.strptime(dates[i], '%Y-%m-%d').isoweekday() == 7:
            extra_add = 1
        
        current = datetime.strptime(dates[i], '%Y-%m-%d') + timedelta(days=extra_add)
        
        if max(delta) >= 0:
            max_previous = current + timedelta(days=-max(delta))
            if min(delta) > 0:
                max_next = current
            else:
                max_next = current + timedelta(days=-min(delta))    
        else:
            max_next = current + timedelta(days=-min(delta)) 
            max_previous = current
        
        # this is the try/except block I added during the call
        try:
            data = yf.download(t, start=max_previous + timedelta(days=-2), end=max_next + timedelta(days=2))
        except:
            pass
        
        ## here I turned current_close to an array to avoid the index problem
        current_close = data.loc[data.index == current, 'Close'].values
        try: # we are going to try to convert it from array to float
            current_close = current_close[0].astype(float)
        except:
            pass # sometimes the output is of size 0, so in that case we do nothing
        
        #print(data[['Close']])
        results['Ticker'].append(t)
        results["Reference Date"].append(current)
        results["Current Close"].append(current_close)
        
        for d in delta:
            if d != 0:
                previous = current + timedelta(days=-d)

                #If date falls in weekends, take Friday and Monday
                if previous.isoweekday() == 6:
                    previous += timedelta(days=-1)
                elif previous.isoweekday() == 7:
                    previous += timedelta(days=+1)
                
                previous_close = data.loc[data.index == previous, 'Close'].values
                try:
                    previous_close = previous_close[0].astype(float)
                except:
                    pass

                if d > 0:
                    results[f"Close_Price_{d}_Days_Before"].append(previous_close)
                elif d < 0:
                    results[f"Close_Price_{abs(d)}_Days_Later"].append(previous_close)

    results = pd.DataFrame(results).set_index('Ticker')
    return results
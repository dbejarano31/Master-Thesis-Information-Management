#!/usr/bin/env python
# coding: utf-8

# In[5]:


import sys
get_ipython().system('{sys.executable} -m pip install numpy')


# In[6]:


pip install --upgrade pip


# In[5]:


pip install requests


# In[3]:


import re
import json
import csv
from io import StringIO
from bs4 import BeautifulSoup
import requests


# In[4]:


url_stats = 'https://finance.yahoo.com/quote/{}/key-statistics?p={}'
url_profile = 'https://finance.yahoo.com/quote/{}/profile?p={}'
url_financials ='https://finance.yahoo.com/quote/{}/financials?p={}'


# In[69]:


stock = 'OGEN'


# In[9]:


response = requests.get(url_financials.format(stock, stock))


# In[10]:


soup = BeautifulSoup(response.text, 'html.parser')


# In[11]:


pattern = re.compile(r'\s--\sData\s--\s')
script_data = soup.find('script', text=pattern).contents[0]


# In[13]:


#beginning
script_data[:500]


# In[14]:


# the end
script_data[-500:]


# In[15]:


start = script_data.find("context")-2


# In[16]:


json_data = json.loads(script_data[start:-12])


# In[17]:


json_data["context"].keys()


# In[18]:


json_data['context']['dispatcher']['stores']['QuoteSummaryStore'].keys()


# In[19]:


annual_is = json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['incomeStatementHistory']['incomeStatementHistory']
quarterly_is = json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['incomeStatementHistoryQuarterly']['incomeStatementHistory']

annual_cf = json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['cashflowStatementHistory']['cashflowStatements']
quarterly_cf =json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['cashflowStatementHistoryQuarterly']['cashflowStatements']

annual_bs = json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['balanceSheetHistory']['balanceSheetStatements']
quarterly_bs = json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['balanceSheetHistoryQuarterly']['balanceSheetStatements']


# In[20]:


print(annual_is[0])


# In[22]:


annual_is[0]['operatingIncome']


# In[23]:


annual_is_stmts = []

#consolidate annual 
for s in annual_is:
    statement = {}
    for key, val in s.items():
        try:
            statement[key] = val['raw']
        except TypeError:
            continue
        except KeyError:
            continue
        annual_is_stmts.append(statement)


# In[24]:


annual_is_stmts[0]


# In[26]:


annual_cf_stmts = []

#consolidate annual 
for s in annual_cf:
    statement = {}
    for key, val in s.items():
        try:
            statement[key] = val['raw']
        except TypeError:
            continue
        except KeyError:
            continue
        annual_cf_stmts.append(statement)


# In[27]:


annual_cf_stmts[0]


# # Profile Data

# In[28]:


response = requests.get(url_profile.format(stock, stock))
soup = BeautifulSoup(response.text, 'html.parser')
pattern = re.compile(r'\s--\sData\s--\s')
script_data = soup.find('script', text=pattern).contents[0]
start = script_data.find("context")-2
json_data = json.loads(script_data[start:-12])


# In[29]:


json_data['context']['dispatcher']['stores']['QuoteSummaryStore'].keys()


# In[30]:


json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['assetProfile'].keys()


# In[32]:


json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['assetProfile']['companyOfficers']


# In[33]:


json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['assetProfile']['longBusinessSummary']


# In[34]:


json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['secFilings']


# In[35]:


json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['summaryDetail']


# # Statistics 

# In[36]:


response = requests.get(url_stats.format(stock, stock))
soup = BeautifulSoup(response.text, 'html.parser')
pattern = re.compile(r'\s--\sData\s--\s')
script_data = soup.find('script', text=pattern).contents[0]
start = script_data.find("context")-2
json_data = json.loads(script_data[start:-12])


# In[37]:


json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['defaultKeyStatistics']


# # Historical Stock Data

# In[164]:


stock_url = 'https://query1.finance.yahoo.com/v7/finance/download/{}?period1=1575904001&period2=1607526401&interval=1d&events=history&includeAdjustedClose=true'


# In[165]:


response = requests.get(stock_url.format(stock))


# In[170]:


response.text


# In[171]:


stock_url = 'https://query1.finance.yahoo.com/v7/finance/download/{}?''

params = {
    'period1':'1575904001',
    'period2':'1607526401',
    'interval':'1d',
    'events' :'history'
}


# In[172]:


params = {
    'range': '1m',
    'interval': '1d',
    'events': 'history'
}


# In[173]:


response = requests.get(stock_url.format(stock), params-params)


# In[174]:


response.text


# In[1]:


file = StringIO(response.text)
reader = csv.reader(file)
data = list(reader)
for row in data[:10]: poqPOSEQzaaiop^^p^pp
    print(row)


# In[176]:


params = {
    'range': '5y',
    'interval':'1d',
    'events':'history'
}


# In[177]:


response = requests.get(stock_url.format(stock), params=params)


# In[178]:


file = StringIO(response.text)
reader = csv.reader(file)
data = list(reader)
for row in data[:5]:
    print(row)


# In[154]:


stock


# In[4]:


jupyter nbconvert notebook.ipynb --to .py


# In[ ]:





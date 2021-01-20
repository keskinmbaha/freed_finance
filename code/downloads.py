import itertools, pandas as pd
import requests, pprint, time
import concurrent.futures
from bs4 import BeautifulSoup

# returns a url with all items in comp appended to url
def make_url(url , comp):
    for r in comp:
        url = '{}/{}'.format(url, r)
    return url

# creates a csv of all (plus a few trash) daily index urls
def daily_index_urls():
    # define the urls needed to make the request, let's start with all the daily filings
    base_url = r"https://www.sec.gov/Archives/edgar/daily-index"

    url = make_url(base_url, ['index.json'])
    content = requests.get(url).json()
    filings = content['directory']['item']

    for index, filing in enumerate(filings):
        if str(filing['name']).find('sitemap') != -1:
            latest = int(filings[index-1]['name'])
            earliest = int(filings[0]['name'])
            break

    # creating a dictionary with all years filings
    daily_urls = []

    for i in range(earliest, latest+1):
        for ii in range(1,5):
            daily_urls.append(make_url(base_url, [i, 'QTR{}/'.format(ii)]))

    df = pd.DataFrame({'URLs' : daily_urls})
    df.to_csv('urls.csv', index=False, header=True)

# grabs all the 10k filings from a master file
def master_10k(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()

        for index, line in enumerate(lines):
            if line.find("-----------") != -1:
                start_index = index+1
                break

        lines = lines[start_index:]
        dict_10k = {'CIK' : [], 'Name' : [], 'Form': [], 'Date' : [], 'URL' : []}

        for index, line in enumerate(lines):

            line = line.rstrip()
            line = line.split("|")
            
            if line[2].find("10-K") != -1:

                dict_10k['CIK'].append(line[0])
                dict_10k['Name'].append(line[1])
                dict_10k['Form'].append(line[2])
                dict_10k['Date'].append(line[3])
                dict_10k['URL'].append(line[4])

    # save all 10ks to a csv
    df = pd.DataFrame(dict_10k)
    df.to_csv("10K_Filings.csv", index=False, header=True)

# grabs all the 10q filings from a master file
def master_10q(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()

        for index, line in enumerate(lines):
            if line.find("-----------") != -1:
                start_index = index+1
                break

        lines = lines[start_index:]
        dict_10q = {'CIK' : [], 'Name' : [], 'Form': [], 'Date' : [], 'URL' : []}

        for index, line in enumerate(lines):

            line = line.rstrip()
            line = line.split("|")

            if line[2].find("10-Q") != -1:

                dict_10q['CIK'].append(line[0])
                dict_10q['Name'].append(line[1])
                dict_10q['Form'].append(line[2])
                dict_10q['Date'].append(line[3])
                dict_10q['URL'].append(line[4])

    # save all 10qs to a csv
    df = pd.DataFrame(dict_10q)
    df.to_csv("10Q_Filings.csv", index=False, header=True)

# grabs all the 8k filings from a master file
def master_8k(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()

        for index, line in enumerate(lines):
            if line.find("-----------") != -1:
                start_index = index+1
                break

        lines = lines[start_index:]
        dict_8k = {'CIK' : [], 'Name' : [], 'Form': [], 'Date' : [], 'URL' : []}

        for index, line in enumerate(lines):

            line = line.rstrip()
            line = line.split("|")

            if line[2].find("8-K") != -1:
                dict_8k['CIK'].append(line[0])
                dict_8k['Name'].append(line[1])
                dict_8k['Form'].append(line[2])
                dict_8k['Date'].append(line[3])
                dict_8k['URL'].append(line[4])
    
    # save all 8ks to a csv
    df = pd.DataFrame(dict_8k)
    df.to_csv("8K_Filings.csv", index=False, header=True)    

master_10k('master.idx')
master_10q('master.idx')
master_8k('master.idx')
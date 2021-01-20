# all imports
import itertools, pandas as pd
import requests, pprint, time
import concurrent.futures
from bs4 import BeautifulSoup
from config import *

# all global variables
NUM_REQUESTS = 0
START_TIME = time.time()

class EdgarData:

    # constructor method
    def __init__(self, name):
        self.name = name
        self.directory = OUTPUT_DIR + "/" + name
        os.mkdir(self.directory)

    # limits the number of requests to less than 10 per second
    def limit_request(self, url):
        global NUM_REQUESTS, START_TIME

        if NUM_REQUESTS == 10:
            if time.time() - START_TIME <= 2:
                print("Too many requests: {}".format(time.time() - START_TIME))
                time.sleep(1)
                print("Requesting resumed")
            NUM_REQUESTS = 0
            START_TIME = time.time()
        
        page = requests.get(url)
        NUM_REQUESTS += 1
        return page

    # returns a file directory path
    def make_directory(self, name):
        filename = self.directory + "/" + name
        return filename

    # returns a url with all items in comp appended to url
    def make_url(self, url , components):
        for component in components:
            url = '{}/{}'.format(url, component)
        return url

    # creates a csv of all (plus a few trash) daily index urls
    def csv_daily_urls(self):
        # define the urls needed to make the request, let's start with all the daily filings
        base_url = r"https://www.sec.gov/Archives/edgar/daily-index"

        url = self.make_url(base_url, ['index.json'])
        page = self.limit_request(url)
        content = page.json()
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
                daily_urls.append(self.make_url(base_url, [i, 'QTR{}/'.format(ii)]))

        filename = self.make_directory("urls.csv")
        df = pd.DataFrame({'URLs' : daily_urls})
        df.to_csv(filename, index=False, header=True)

    # grabs all the master file urls in the given daily index url
    def index_master_urls(self, url):
        url = url[:len(url)-1]
        json_url = self.make_url(url, ['index.json'])
        page = self.limit_request(json_url)
        masters = []

        try:
            content = page.json()

            print("Fetching {}".format(json_url))
            for master in content['directory']['item']:
                name = master['name']

                if name.find("master") != -1:
                    master_url = self.make_url(url, [master['name']])
                    masters.append(master_url)
        except:
            print("Page does not exist")
            return None
        
        return masters

    # grabs all the 10k filings from a master file
    def master_10k(self, filename):
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
        filename = self.make_directory("10K_Filings.csv")
        df = pd.DataFrame(dict_10k)
        df.to_csv(filename, index=False, header=True)

    # grabs all the 10q filings from a master file
    def master_10q(self, filename):
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
        filename = self.make_directory("10Q_Filings.csv")
        df = pd.DataFrame(dict_10q)
        df.to_csv(filename, index=False, header=True)

    # grabs all the 8k filings from a master file
    def master_8k(self, filename):
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
        filename = self.make_directory("8K_Filings.csv")
        df = pd.DataFrame(dict_8k)
        df.to_csv(filename, index=False, header=True)    

    # create a csv of all the master urls
    def csv_index_master_urls(self):
        filename = self.directory + "/urls.csv"
        df = pd.read_csv(filename)
        urls = df['URLs']

        for index, url in enumerate(urls):
            masters = self.index_master_urls(url)

            if index == 10: break

            if masters != None:
                url = url[url.find("index")+6:]
                url = url.replace("/", "")
                url = url[:url.find("Q")] + "_" + url[url.find("Q"):] + ".csv"

                filename = self.make_directory(url)
                df = pd.DataFrame({'URLs' : masters})
                df.to_csv(filename, index=False, header=True)

test = EdgarData("test")
test.csv_daily_urls()
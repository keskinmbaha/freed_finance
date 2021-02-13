# all imports
import itertools, pandas as pd
import requests, pprint, time
import gzip, shutil
import math, numpy as np
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
        try:
            os.mkdir(self.directory)
        except:
            print("{} already exists".format(self.directory))

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

        filename = "{}/urls.csv".format(self.directory)
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
        print("{}\n".format(filename))

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
                
                try:
                    if line[2].find("10-K") != -1:

                        dict_10k['CIK'].append(line[0])
                        dict_10k['Name'].append(line[1])
                        dict_10k['Form'].append(line[2])
                        dict_10k['Date'].append(line[3])
                        dict_10k['URL'].append(line[4])
                except:
                    print("LINE: {}".format(line))

        # save all 10ks to a csv
        df = pd.DataFrame(dict_10k)
        return df

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
        filename = "{}/10Q_Filings.csv".format(self.directory)
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
        filename = "{}/8K_Filings.csv".format(self.directory)
        df = pd.DataFrame(dict_8k)
        df.to_csv(filename, index=False, header=True)    

    # create a csv of all the master urls
    def csv_index_master_urls(self):
        filename = self.directory + "/urls.csv"
        df = pd.read_csv(filename)
        urls = df['URLs']
        all_masters = {}
        max_len = -1

        for url in enumerate(urls):
            masters = self.index_master_urls(url)

            if masters != None:
                filename = url[url.find("index")+6:]
                filename = filename.replace("/", "")
                filename = filename[:filename.find("Q")] + "_" + filename[filename.find("Q"):]

                all_masters[filename] = masters

                if max_len < len(masters):
                    max_len = len(masters)
        
        for key in all_masters.keys():
            if len(all_masters[key]) != max_len:
                filler = [None]*(max_len - len(all_masters[key]))
                all_masters[key].extend(filler)

        filename = "{}/masters.csv".format(self.directory)
        df = pd.DataFrame(all_masters)
        df.to_csv(filename, index=False, header=True)
    
    # downloads all the master files
    def master_download(self):
        filename = self.directory + "/masters.csv"
        df = pd.read_csv(filename)
        
        try:
            masters_dir = "{}/masters".format(self.directory)
            os.mkdir(masters_dir)
        except:
            print("{} already exists".format(masters_dir))

        for col in df:
            temp = df[col]
            temp.dropna(inplace=True)
            
            try:
                yq_dir = "{}/{}".format(masters_dir, col)
                os.mkdir(yq_dir)
                print(yq_dir)
            except:
                print("{} already exists".format(yq_dir))

            for url in temp:
                page = self.limit_request(url)
                content = page.content

                filename = url[url.find("master"):]
                filename = filename.replace(".idx", "")
                filename = filename + ".txt"
                print(filename)

                filename = "{}/{}".format(yq_dir, filename)
                with open(filename, 'wb') as f:
                    f.write(content)
                
    # loops through all the master files and parses out the 10ks
    def master_to_10k(self):
        
        # creates a folder for all 10ks
        try:
            filename = "{}/10k".format(self.directory)
            os.mkdir(filename)
        except:
            print("{} already exists".format(filename))


        filename = "{}/masters".format(self.directory)
        directories = list(os.walk(filename))
        directories = directories[1:]

        for index, directory in enumerate(directories):
            dir_name = str(directory[0])

            try:
                temp = dir_name[:dir_name.find("masters")] + "10k" + dir_name[dir_name.find("masters")+7:]
                os.mkdir(temp)
            except:
                print("{} already exists".format(temp))
            
            for filename in os.listdir(dir_name):
                file_path = "{}/{}".format(dir_name, filename)

                df = self.master_10k(file_path)
                # print(df)


dir_name = OUTPUT_DIR
url = r"https://www.sec.gov/Archives/edgar/daily-index/2013/QTR1/master.20130104.idx.gz"
filename = "{}/master.20130104.idx.gz".format(dir_name)
content = requests.get(url).content

with open(filename, 'wb') as f:
    f.write(content)

# print(content)

with gzip.open(filename, 'rb') as f_in:
    with open('file.txt', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
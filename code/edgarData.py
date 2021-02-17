# all imports
import itertools, pandas as pd
import requests, pprint, time
import gzip, shutil
import math, numpy as np
import concurrent.futures
from bs4 import BeautifulSoup
from config import *

# all global variables
NUM_REQUESTS = 0    # holds the number of requests done so far
START_TIME = time.time()    # keeps track of the most recent start time

# class for grabbing edgar data
class EdgarData:

    # constructor
    def __init__(self, name):

        # grabs the name and initializes a folder of that name in the output dir
        self.name = name
        self.directory = OUTPUT_DIR + "/" + name
        try:
            os.mkdir(self.directory)
        except:
            print("Already Exists: {}".format(self.directory))


    # limits the number of requests to less than 10 per second
    def limit_request(self, url):
        global NUM_REQUESTS, START_TIME

        # if the number of requests is above 10, see how long it took to make them
        if NUM_REQUESTS == 10:

            # if less than 1.2 seconds have passed between requests, pause
            # SEC allows for at most 10 request per second
            if time.time() - START_TIME <= 1.2:
                print("Too many requests: {}".format(time.time() - START_TIME))
                time.sleep(1)
                print("Requesting resumed")

            # reset the variables
            NUM_REQUESTS = 0
            START_TIME = time.time()
        
        # make the request and increment the counter
        page = requests.get(url)
        NUM_REQUESTS += 1

        return page


    # returns a url with all items in comp appended to url
    def make_url(self, base , components):

        # loops through the components and adds them to the base url
        for component in components:
            base = '{}/{}'.format(base, component)

        return base


    # creates a csv of all (plus a few trash) daily index urls
    def csv_daily_urls(self):

        # creates a url in the format of a json file
        base_url = r"https://www.sec.gov/Archives/edgar/daily-index"
        url = self.make_url(base_url, ['index.json'])

        # makes a request and manipulates the response to get all the filiings
        page = self.limit_request(url)
        content = page.json()
        filings = content['directory']['item']

        # loops through all the filings
        for index, filing in enumerate(filings):

            # makes sure the name is a number for the year
            if str(filing['name']).find('sitemap') != -1:

                # grabs the earliest and latest year of the filings
                latest = int(filings[index-1]['name'])
                earliest = int(filings[0]['name'])
                break

        # creating a dictionary for all years filings
        daily_urls = []

        # goes from the earliest year to the latest and creates urls for each year/quarter
        for i in range(earliest, latest+1):
            for ii in range(1,5):
                daily_urls.append(self.make_url(base_url, [i, 'QTR{}/'.format(ii)]))

        # creates the urls.csv file in the output dir
        filename = "{}/urls.csv".format(OUTPUT_DIR)
        df = pd.DataFrame({'URLs' : daily_urls})
        df.to_csv(filename, index=False, header=True)


    # grabs all the master file urls in a given daily index urls
    def index_master_urls(self, url):

        # creates a json url for the given daily index url
        url = url[:len(url)-1]
        json_url = self.make_url(url, ['index.json'])
        page = self.limit_request(json_url)

        # create a list to hold the master urls
        masters = []

        # make sure the page actually exists
        try:
            content = page.json()

            # go through all the master file names
            print("Fetching: {}".format(json_url))
            for master in content['directory']['item']:
                name = master['name']

                # if a master filename exists, make a url out of it and store it
                if name.find("master") != -1:
                    master_url = self.make_url(url, [master['name']])
                    masters.append(master_url)

        # if it doesn't, print out an error message, and return nothing
        except:
            print("Page does not exist")
            return None
        
        return masters


    # create a csv of all the master urls for all daily index urls
    def csv_index_master_urls(self):

        # opens the urls.csv
        filename = "{}/urls.csv".format(OUTPUT_DIR)
        df = pd.read_csv(filename)

        # cretes a dictionary to hold all masters, and a max length variable
        # so when saving the masters into a csv, they all have the same length
        all_masters = {}
        max_len = -1

        # loops through all the urls in urls.csv
        for index, row in df.iterrows():

            # grabs the url and grabs all the master files from that url
            url = row['URLs']
            masters = self.index_master_urls(url)

            # makes sure the page exists
            if masters != None:

                # creates column a name
                filename = url[url.find("index")+6:]
                filename = filename.replace("/", "")
                filename = filename[:filename.find("Q")] + "_" + filename[filename.find("Q"):]

                all_masters[filename] = masters

                # keeps track of the greatest number of master files
                if max_len < len(masters):
                    max_len = len(masters)
        
        # fills in all the indecies that did not have the greatest number of master files
        for key in all_masters.keys():
            if len(all_masters[key]) != max_len:
                filler = [None]*(max_len - len(all_masters[key]))
                all_masters[key].extend(filler)
        
        # saves the csv to the output dir
        filename = "{}/masters.csv".format(OUTPUT_DIR)
        df = pd.DataFrame(all_masters)
        df.to_csv(filename, index=False, header=True)   
    

    # downloads all the master files
    def master_download(self):

        # opens up the masters.csv file
        filename = "{}/masters.csv".format(OUTPUT_DIR)
        df = pd.read_csv(filename)
        
        # tries to create a master file dir
        try:
            os.mkdir(OUTPUT_MASTER_DIR)
        except:
            print("Already Exists: {}".format(OUTPUT_MASTER_DIR))

        # loops through each year/quarter file in the masters.csv
        for col in df:

            # drops all the rows that have a None value
            temp = df[col]
            temp.dropna(inplace=True)
            
            # tries to create a dir for the year/quarter in the master file dir
            try:
                yq_dir = "{}/{}".format(OUTPUT_MASTER_DIR, col)
                os.mkdir(yq_dir)
                print("Creating: {}".format(yq_dir))
            except:
                print("Already Exists: {}".format(yq_dir))

            # loops through each url in the year/quarter
            for url in temp:

                # makes a request using the master url
                page = self.limit_request(url)
                content = page.content

                # creates a filename with the .txt extension
                # TODO FIX FOR ZIPPED FILES
                filename = url[url.find("master"):]
                filename = filename.replace(".idx", "")
                filename = filename + ".txt"
                print(filename)

                # downloads the master file to its respective folder
                filename = "{}/{}".format(yq_dir, filename)
                with open(filename, 'wb') as f:
                    f.write(content)


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

         
    # loops through all the master files and parses out the 10ks
    def master_to_10k(self):
        
        # creates a folder for all 10ks
        try:
            filename = "{}/10k".format(self.directory)
            os.mkdir(filename)
        except:
            print("Already Exists: {}".format(filename))

        directories = list(os.walk(OUTPUT_MASTER_DIR))
        directories = directories[1:]

        for index, directory in enumerate(directories):
            dir_name = str(directory[0])
            print(dir_name)

            try:
                temp = dir_name[:dir_name.find("masters")] + "10k" + dir_name[dir_name.find("masters")+7:]
                os.mkdir(temp)
            except:
                print("Already Exists: {}".format(temp))
            
            for filename in os.listdir(dir_name):
                file_path = "{}/{}".format(dir_name, filename)

                df = self.master_10k(file_path)
                # print(df)


# main function for testing out the code
def main():
    test = EdgarData('test')
    test.csv_daily_urls()

    # dir_name = OUTPUT_DIR
    # url = r"https://www.sec.gov/Archives/edgar/daily-index/2013/QTR1/master.20130104.idx.gz"
    # filename = "{}/master.20130104.idx.gz".format(dir_name)
    # content = requests.get(url).content

    # with open(filename, 'wb') as f:
    #     f.write(content)

    # # print(content)

    # with gzip.open(filename, 'rb') as f_in:
    #     with open('file.txt', 'wb') as f_out:
    #         shutil.copyfileobj(f_in, f_out)


# runs the main function
if __name__ == "__main__":
    main()
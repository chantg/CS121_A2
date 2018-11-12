import logging
from datamodel.search.ChantgTylerrb_datamodel import ChantgTylerrbLink, OneChantgTylerrbUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
import lxml.html
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4
from StringIO import StringIO
from urlparse import urlparse, parse_qs
from uuid import uuid4
import signal
import sys
import inspect
import shutil 

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

greatestURL = "" #global variables for analysiss
urls = {}
maxOut =  0
@Producer(ChantgTylerrbLink)
@GetterSetter(OneChantgTylerrbUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "ChantgTylerrb"
    persistantMax =0 #if the crawler is stopped maxOut is reset to 0
    def __init__(self, frame):
        self.app_id = "ChantgTylerrb"
        self.frame = frame
        self.counter = 0 #stop after getting 3000 links


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneChantgTylerrbUnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = ChantgTylerrbLink("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneChantgTylerrbUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        if(self.counter > 3000): #stops after 3000 downloaded
                analysis()
                
                self.shutdown()
        for link in unprocessed_links:
            self.counter += 1
            print "Got a link to download:", link.full_url
            downloaded = link.download()
			
            links = extract_next_links(downloaded)       
            for l in links:
                if is_valid(l):
                    self.frame.add(ChantgTylerrbLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
	
def analysis():
    global maxOut
    global greatestURL
    global urls
	
    try:
        from_file = open(os.path.dirname(os.path.abspath(inspect.stack()[0][1])) + "/analysis.txt") 
        line = from_file.readline() # get first line

        line.split(":")

        to_file = open(os.path.dirname(os.path.abspath(inspect.stack()[0][1])) + "/analysis.txt",mode="w")
        to_file.write(line)
        shutil.copyfileobj(from_file, to_file) #rewrites the first line of the file
        if(maxOut > int(line[1])):
            to_file = greatestURL + " : " + str(maxOut) + "\n"
        line = ":".join(line)

        to_file.write(line + "\n")
        shutil.copyfileobj(from_file, to_file) #does the rewrite 
        from_file.close()
        to_file.close()
    except:
        file = open(os.path.dirname(os.path.abspath(inspect.stack()[0][1])) + "/analysis.txt", "w+") #create and write at the begining
        file.write(greatestURL + " : " + str(maxOut))
        file.close()
    
    file = open(os.path.dirname(os.path.abspath(inspect.stack()[0][1])) + "/analysis.txt", "a") #appends to the end
    for index in urls:
        file.write("\n"+ index + " - " + str(urls[index]))
    file.close()

def signal_handler(sig, frame):
    analysis() #handles a pause
    sys.exit(0)
    
def extract_next_links(rawDataObj):
    outputLinks = []
    global maxOut
    global greatestURL
    global urls
    signal.signal(signal.SIGINT, signal_handler)
    
    if(rawDataObj.content):
        
        document = html.fromstring(rawDataObj.content) #create a document from the raw data object
        
        document.make_links_absolute("http://www.ics.uci.edu/") #resolve all incomplete links in the document with the ics domain
        for item in document.iterlinks(): #grab every link in the page and add it to the output
            outputLinks.append(item[2])
    
    if(len(outputLinks) > maxOut):  #after adding out links check if it's the most out link in the list
        maxOut = len(outputLinks)
        greatestURL = rawDataObj.url
		
    urls[rawDataObj.url] = len(outputLinks)
    
    return outputLinks

def is_valid(url):
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    folders = re.split("/", parsed.path)
    for folder in folders:
        if (len(folder) > 65): #if any part of the url is a long string or if we have repeating directories it is likely dynamically generated
            return False
        
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False


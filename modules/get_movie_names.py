import pandas as pd
import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import re
from tqdm import tqdm


BASE_URL = "https://www.imdb.com/list/ls056549735/?sort=list_order,asc&st_dt=&mode=detail&page="
SOURCE_DATA = '../data/raw/'
DEST_DIR = '../data/processed/'
pbar = tqdm(total=10)

def fetch_movie_names():
    print("Fetching 1000 movie names from 10 pages...")
    pages_count = 0
    uncleaned_movie_names = []
    while pages_count !=10:
        html = urlopen(BASE_URL+str(pages_count))
        soup = BeautifulSoup(html,'html.parser')
        movie_header_tags = soup.findAll('h3',{'class':'lister-item-header'})
        for tag in movie_header_tags:
            uncleaned_movie_names.append(tag.text)
        pages_count +=1
        pbar.update(1)
    movie_names = pd.DataFrame({'name':uncleaned_movie_names})
    movie_names.to_csv(SOURCE_DATA+'unclean_movie_names.csv',index=False)
    print("1000 movie names fetched and stored as csv under ../data/raw/unclean_movie_names.csv")
    return 



def cleanMovieName():
    print("Cleaning movie names...")
    names = []
    movie_names = pd.read_csv(SOURCE_DATA+'unclean_movie_names.csv')
    movie_names.name = movie_names.name.str.replace("\n","")
    movie_names.name = movie_names.name.str.replace("(","")
    movie_names.name = movie_names.name.str.replace(")","")
    for movie in movie_names.name:
        cleaned_name = movie[movie.find(".")+1:-4]
        names.append(cleaned_name)
    clean_movie_names = pd.DataFrame({'text':names})
    clean_movie_names.to_csv(DEST_DIR+'clean_movie_names.csv',index=False)
    print("Movie names cleaned and stored under ../data/processed/clean_movie_names.csv")
    return
    
if __name__ == "__main__":
    fetch_movie_names()
    clean_movie_names()
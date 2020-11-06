# -*- coding: utf-8 -*-
import pandas as pd
import requests
from bs4 import BeautifulSoup
import tqdm.notebook as tq

BASE_DIR='../data/processed/'
movies_list= BASE_DIR + 'clean_movie_names.csv'
BASE_URL='https://en.wikipedia.org/wiki/'
DEST_DIR = '../data/unseen/'

def fetch_attributes_from_wikipedia():
    cast=[]
    release_year=[]
    plot=[]
    title=[]
    movies_name=pd.read_csv(movies_list)
    print("Fetching movie attributes..")
    for movie in tq.tqdm(movies_name['text']):
        full_plot="NA"
        movie_uri=movie.replace(" ","_")
        start_url = requests.get(BASE_URL + movie_uri).content
        soup = BeautifulSoup(start_url, 'html.parser')
        table = soup.find("table",{"class":"infobox vevent"})
        if table is None:
            start_url = requests.get(BASE_URL + movie + '_(film)').content
            soup = BeautifulSoup(start_url, 'html.parser')
            table = soup.find("table",{"class":"infobox vevent"})
        values = []
        keys=[]
        if table is not None:
            try:
                tag  = soup.select_one('#Plot').find_parent('h2').find_next_sibling()
                while tag.name == 'p':
                    full_plot=full_plot + tag.text
                    tag = tag.find_next_sibling()
            except:
                full_plot=full_plot
            for tr in table:
                th=tr.find_all('th')
                td = tr.find_all('td')
                value = [tr.text for tr in td]
                key= [tr.text for tr in th]
                values.append(value)
                keys.append(key)
            values=values[0]
            keys=keys[0]
            dictionary = dict(zip(keys, values))
            try:
                cast.append(dictionary['Starring'])
                release_year.append(dictionary['Release date'])
                title.append(movie)
                plot.append(full_plot)
                #print("All attributes fetched for movie - "+movie)
            except:
                cast.append('NA')
                release_year.append('NA')
                title.append(movie)
                plot.append(full_plot)
    unseen_data = pd.DataFrame({'title':title,
                                'cast':cast,'release_year':release_year,'plot':plot})
    unseen_data.to_csv(DEST_DIR+'unseen_data.csv',index=False)
    print("Data fetching from Wikipedia completed! Results stored under ../data/unseen/unseen_data.csv")
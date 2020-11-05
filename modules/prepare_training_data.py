                                    
import os
import pandas as pd
import csv
from tqdm import tqdm
import json

BASE_DIR = '../data/train/raw/'
DESTINATION_DIR = '../data/train/processed/'
wiki_data = BASE_DIR+'wiki_movie_data.csv'
CMU_data = BASE_DIR+'CMU_movie_metadata.tsv'
kaggle_data = BASE_DIR+'kaggle_movie_data.csv'
CMU_plot_summaries = BASE_DIR+'CMU_plot_summaries.txt'



class ReadData():
    def load_file(self,file_path,file_format):
        if file_format not in ['tsv','csv']:
            raise TypeError("File format not supported in class ReadData. Please use manual loading.")
            
        else:
            if file_format == 'csv':
                return pd.read_csv(file_path)
            else:
                return pd.read_csv(file_path,sep='\t')

        

def convert_txt_to_csv(txt_filename):
    data = []
    with open(txt_filename, 'r') as f:
        reader = csv.reader(f, dialect='excel-tab') 
        for row in tqdm(reader):
            data.append(row)
    movie_id = []
    plot = []

    # extract movie Ids and plot summaries
    for i in tqdm(data):
        movie_id.append(i[0])
        plot.append(i[1])

    # create dataframe
    movies = pd.DataFrame({'ID': movie_id, 'Plot': plot})
    movies.to_csv(BASE_DIR+'CMU_plot.csv',index=False)


data_reader = ReadData()

wikipedia_data = data_reader.load_file(file_path=wiki_data,file_format='csv')

wikipedia_data.head()

map_wiki_categories={'drama, horror':'horror',
                     'horror':'horror',
                      'horror, comedy':'horror',
                     'romantic drama':'romance',
                     'romance drama':'romance',
                     'romance/drama':'romance',
                     'drama, romance':'romance',
                     'romantic comedy/drama':'romance',
                     'romance/drama':'romance',
                     'romance/comedy':'romance',
                     'romance':'romance',
                     'romantic comedy':'romance',

                     'sci-fi, horror':'science fiction',
                     'action, sci-fi':'science fiction',
                     'drama, science fiction':'science fiction',
                     'comedy, science fiction':'science fiction',
                     'sci-fi comedy':'science fiction',
                     'sci-fi, comedy':'science fiction',
                     'science fiction':'science fiction',
                     'science-fiction':'science fiction',
                     'horror, science fiction':'science fiction',
                     'sci-fi':'science fiction',
                     'horror, sci-fi':'science fiction',
                     'science fiction, horror':'science fiction',
                     'action, science fiction ':'science fiction',
                     'science fiction, thriller':'science fiction',
                     'tokusatsu, action, sci-fi':'science fiction',
                     'science fiction comedy':'science fiction',
                     'drama, science fiction':'science fiction',

                     'action comedy':'action',
                     'action drama':'action',
                     'action, drama':'action',
                     'short action/crime western':'action',
                     'action adventure':'action',
                     'action thriller':'action',
                     'action masala':'action',
                     'action':'action',

                     'suspense':'suspense',
                     'mystery, thriller':'suspense',
                     'mystery, thriller':'suspense',
                     'mystery, horror':'suspense',
                     'action, thriller':'suspense',
                     'mystery':'suspense',
                     'thriller':'suspense',
                     'drama, mystery':'suspense',
                     'mystery, thriller':'suspense',
                     'drama, mystery':'suspense',
                     'drama, thriller':'suspense',
                     'psychological thriller':'suspense',
                     'horror thriller':'suspense',
                     'crime/thriller':'suspense',

                     'crime':'others',
                     'adventure':'others',
                     'drama, adventure':'others',
                    }

wikipedia_data['Genre']=wikipedia_data['Genre'].map(map_wiki_categories)

wiki_selected_data=wikipedia_data[wikipedia_data['Genre'].isin(['romance','action','suspense','horror','science fiction','others'])]

wiki_selected_data['Genre'].unique()

wiki_selected_data=wiki_selected_data[['Release Year','Title','Cast','Plot','Genre']].reset_index(drop=True)

wiki_selected_data.head()

convert_txt_to_csv(CMU_plot_summaries)

cmu_plot_data=data_reader.load_file(BASE_DIR + 'CMU_plot.csv','csv')

cmu_movie_data=data_reader.load_file(CMU_data,'tsv')[['ID', 'Title', 'Genre','Release Year']]

cmu_data = cmu_movie_data.merge(cmu_plot_data,on = 'ID',how='left')

cmu_data=cmu_data[cmu_data['Plot'].notnull()]

cmu_selected_data=cmu_data[(~cmu_data.Title.isin(wikipedia_data.Title))]

cmu_selected_data.head()

genres = [] 
# extract genres
for i in cmu_selected_data['Genre']: 
    genres.append(list(json.loads(i).values())) 

# add to 'movies' dataframe  
cmu_selected_data['Genre'] = genres
cmu_selected_data['Genre'] = cmu_selected_data['Genre'].astype(str)

cmu_mapping={"['Action']":"action",
             "['Thriller']":"suspense",
            "['Science Fiction']":"science fiction",
            "['Science Fiction', 'Horror']":"science fiction",
            "['Science Fiction', 'Action']":"science fiction",
            "['Thriller', 'Science Fiction', 'Horror']":"science fiction",
            "['Science Fiction', 'Drama']":"science fiction",
            "['Science Fiction', 'Comedy']":"science fiction",
            "['Thriller', 'Science Fiction', 'Action']":"science fiction",
            "['Science Fiction', 'Adventure']":"science fiction"}

cmu_selected_data['Genre']=cmu_selected_data['Genre'].map(cmu_mapping)
cmu_selected_data=cmu_selected_data[cmu_selected_data['Genre'].isin(['romance','action','suspense','horror','science fiction','others'])].reset_index(drop=True)
cmu_selected_data=cmu_selected_data.drop('ID',axis=1)
cmu_selected_data['Cast']=None

cmu_selected_data.head()

final_data=wiki_selected_data.append(cmu_selected_data)

final_data.to_csv(DESTINATION_DIR+'train_data.csv',index=False)


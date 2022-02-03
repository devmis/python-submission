import pandas as pd
import sqlite3

class DataManagement():
    def __init__(self, csv_filepath, db_filepath):
        self.dataf = pd.read_csv(csv_filepath)
        self.vaccin_covid_db_filepath = db_filepath

    def create_database(self):
        self.con = sqlite3.connect(self.vaccin_covid_db_filepath)

        # self.vaccin_covid_db = self.dataf.to_sql(self.name, self.vaccin_covid_db) 

    def seed_database(self):

        #daily_covid_data = pd.read_csv(r"C:/Users/deves/Documents/python/vaccine_covid.csv")

        vaccines = list()
        for item in self.dataf['vaccines'].str.split(",").apply(pd.Series).stack().reset_index(drop=True).unique():
            vaccines.append(item.lstrip())
        vaccines = pd.Series(vaccines).reset_index()

        # Vaccine id being the primary key
        vaccines.columns = ['vaccine_id',"vaccine_name"]


        # There were 92 sources corresponding to 219 countries hence duplicacy hence normalisation
        # dropping duplicates and resetting the index to find the ids
        sources=self.dataf[['source_name','source_website']].drop_duplicates().reset_index(drop = True).reset_index().rename(columns = {"index":"source_id"})


        # Since it was a day level data for each country so country names were also repeated hence normalisation as required
        # normalisation was done in a similar fashion like Sources
        countries = self.dataf[['country','iso_code']].drop_duplicates().reset_index(drop = True).reset_index().rename(columns ={"index":"country_id"})
       
        # Since iso_code can act as unique identifier hence primary key so dropped country_id
        del countries['country_id']


        #As a result of normalisation mapping of country and vaccine suppliers needs to be maintained in a separate table
        country_vaccine=self.dataf[['iso_code','vaccines']].drop_duplicates().reset_index(drop=True)
        country_vaccine['vaccines']=country_vaccine['vaccines'].str.split(",")
        country_vaccine=country_vaccine.explode("vaccines")
        country_vaccine.rename(columns={"vaccines":"vaccine_name"},inplace=True)
        
        # Getting IDs of vaccines instead of names
        country_vaccine=vaccines.merge(country_vaccine,how='right',on='vaccine_name')
        country_vaccine=country_vaccine.drop_duplicates()
        del country_vaccine['vaccine_name']

        # Country Source of information mapping
        country_source=self.dataf[['iso_code','source_name']].drop_duplicates().merge(sources[['source_id','source_name']],how='left',on='source_name')
        del country_source['source_name']

        # Removing redundant fields from original dataframe
        del self.dataf['source_name']
        del self.dataf['source_website']
        del self.dataf['vaccines']
        del self.dataf['country']

         
        self.dataf.to_sql("country_level_daily_vaccination", self.con, if_exists="replace", index=False)
        vaccines.to_sql("vaccine_supplier", self.con, if_exists="replace", index=False)
        country_vaccine.to_sql("country_vaccine_mapping", self.con, if_exists="replace", index=False)
        countries.to_sql("country", self.con, if_exists="replace", index=False)
        sources.to_sql("data_source", self.con, if_exists="replace", index=False)
        country_source.to_sql("country_data_source_mapping", self.con, if_exists="replace", index=False)
        
        self.clean_nan('daily_vaccinations')
        print(self.dataf)
        print("\n")

        print("# Table Vaccines")
        print(vaccines)
        print("\n")

        print("# Table Sources")
        print(sources)
        print("\n")

    def clean_nan(self, col_name1):
        """Clean NaN occurences in table."""
        self.dataf.dropna(subset=[col_name1], inplace = True)

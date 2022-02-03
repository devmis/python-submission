from dbms_final import DataManagement

# Creating a database if it doesn't exist/Connecting to it if it exists already
# Please provide absolute path
# Basically defining a connection to db here

if (__name__ == '__main__'):
    csv_filepath = "C:/Users/deves/Documents/python/vaccine_covid.csv"
    db_filepath = "C:/Users/deves/Documents/python/vaccine_covid.db"

    data_management = DataManagement(csv_filepath, db_filepath)
    data_management.create_database()
    data_management.seed_database()

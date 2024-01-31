from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlite3

# 1. Write a data extraction function to retrieve the relevant information from the required URL.
def extract_scrap_data(url):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all("tbody")
    rows = tables[2].find_all("tr")

    df = pd.DataFrame(columns=["Country/Territory","UN_Region","IMF_Estimate", "IMF_Year"])

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {"Country/Territory": col[0].text,
                        "UN_Region": col[1].contents[0],
                        "IMF_Estimate": col[2].contents[0],
                        "IMF_Year": col[3].text}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    
    return df


# 2. Transform the available GDP information into 'Billion USD' from 'Million USD'.
def transform_data(df):
    df["IMF_Estimate"] = df["IMF_Estimate"].transform(lambda x: x.replace(",", ""))
    df["IMF_Estimate"] = df["IMF_Estimate"].transform(lambda x: x.replace("—", 0))
    df["IMF_Estimate"] = df["IMF_Estimate"].astype(int)

    def update_year(row):
        if len(row) == 4:
            row = int(row)
            return row
        elif row.find("]") != -1:
            row = row.split("]")[1]
            row = int(row)
            return row
        else:
            row = 0
            return row
    
    df["IMF_Year"] = df["IMF_Year"].apply(update_year)

    df = df.rename({"IMF_Estimate": "IMF_Estimate_USD_Mio"}, axis=1)

    df["IMF_Estimate_USD_Bio"] = df["IMF_Estimate_USD_Mio"].transform(lambda x: x/1000)
    df["IMF_Estimate_USD_Bio"] = df["IMF_Estimate_USD_Bio"].transform(lambda x: round(x, 2))

    return df


# 3. Load the transformed information to the required CSV file and as a database file.
def load_data(df, filename, table_name, db_name, attributes):
    final_df = df.copy()
    final_df = final_df.rename({"Country/Territory": "Country",
                                "IMF_Estimate_USD_Bio": "GDP_USD_billion"}, axis=1)
    final_df = final_df[attributes]
    final_df.to_csv(filename)

    conn = sqlite3.connect(db_name)
    final_df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
    conn.close()


# 4. Run the required query on the database.
def run_query(query, db_name):
    conn = sqlite3.connect(db_name)
    query_df = pd.read_sql(query, conn)
    conn.close()
    return query_df


# 5. Log the progress of the code with appropriate timestamps.
def execution_log(message, logfile):
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile, "a") as f:
        f.write(timestamp + ", " + message + "\n")
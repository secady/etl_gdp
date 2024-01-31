
import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
import myfunc as fun

# ### Scrape data


url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

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


# ### Convert data in "IMF Estimate" and "IMF Year" columns into integer

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



# ### The data in the "IMF Estimate" column is in USD Million. Let's change it into USD Billion and round it up to 2 decimal places

df = df.rename({"IMF_Estimate": "IMF_Estimate_USD_Mio"}, axis=1)

df["IMF_Estimate_USD_Bio"] = df["IMF_Estimate_USD_Mio"].transform(lambda x: x/1000)
df["IMF_Estimate_USD_Bio"] = df["IMF_Estimate_USD_Bio"].transform(lambda x: round(x, 2))
df


# ### Save data in JSON and DB

filename = 'Countries_by_GDP.json'
table_name = 'Countries_by_GDP'
db_name = 'World_Economies.db'
attributes = ['Country', 'GDP_USD_billion']
logfile = 'etl_project_log.txt'


final_df = df.copy()
final_df = final_df.rename({"Country/Territory": "Country",
                            "IMF_Estimate_USD_Bio": "GDP_USD_billion"}, axis=1)
final_df = final_df[attributes]

final_df.to_json("filename")

conn = sqlite3.connect(db_name)
final_df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
conn.close()


# ### Let's check the DB and log the execution process

conn = sqlite3.connect(db_name)
query = pd.read_sql(f"SELECT * FROM {table_name} WHERE GDP_USD_billion > 100", conn)
query


conn.close()

message = "Query has been run succesfully"

fun.execution_log(message=message, logfile=logfile)

import myfunc as fun


# 1. Write a data extraction function to retrieve the relevant information from the required URL.
url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
df = fun.extract_scrap_data(url=url)


# 2. Transform the available GDP information into 'Billion USD' from 'Million USD'.
df = fun.transform_data(df=df)


#3. Load the transformed information to the required CSV file and as a database file.
filename = 'Countries_by_GDP.csv'
table_name = 'Countries_by_GDP'
db_name = 'World_Economies.db'
attributes = ['Country', 'GDP_USD_billion']
fun.load_data(df=df, filename=filename, table_name=table_name, db_name=db_name, attributes=attributes)


# 4. Run the required query on the database.
fun.run_query(query=f"SELECT * FROM {table_name} WHERE GDP_USD_billion > 100", db_name=db_name)


# 5. Log the progress of the code with appropriate timestamps.
message = "Query has been run succesfully"
logfile = 'etl_project_log.txt'
fun.execution_log(message=message, logfile=logfile)

print("All is done!")

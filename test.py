import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv(r'G:\My Drive\HavenWise\Database\myVaillant.csv')

# Display the DataFrame
print(df)

# Iterate through each row
for index, row in df.iterrows():
    # Access row values using row['column_name'] or row[index]
    print(row)
    row.id
    row.login
    row.password
import os
import sys
import pandas as pd

# set the path to the directory containing the csv file
directory = os.path.join(os.path.dirname(__file__), '..')

# Load the Excel file into a pandas DataFrame
excel_file_path = 'output_key.xlsx'
key_df = pd.read_excel(directory + '\\' + excel_file_path)

# Create an empty dictionary to store the results
result_dict = {}
key_df['unified'] = key_df.apply(lambda row: next((entry for entry in row if pd.notna(entry)), None), axis=1)
print(key_df)

# Path to the 'Outputs' folder
folder_path = os.path.join(os.path.dirname(__file__), '..', 'Outputs')

# Create an empty DataFrame to store the data
output_df = pd.DataFrame()
df_list = []

# Iterate through the Excel files in the folder
for filename in os.listdir(folder_path):
    current_df = pd.DataFrame()
    if filename.endswith('.xlsx'):
        print(filename)
        file_path = os.path.join(folder_path, filename)
        
        # Extract lowercase filename without extension
        column_name = os.path.splitext(filename)[0].lower()
        
        # Read the first sheet from the Excel file
        df_sheet = pd.read_excel(file_path, sheet_name=0)
        
        columns = key_df[column_name].to_list()
        columns_dict = dict(zip(key_df['unified'], columns))
        columns_dict = {key: value for key, value in columns_dict.items() if pd.notna(value)}
        
        # Populate output_df with data from entries
        for attr, key in columns_dict.items():
            entries = df_sheet[key].tolist()
            current_df[attr] = entries
        # Append the current_df to the list
        df_list.append(current_df)

# Concatenate the DataFrames in the list to create output_df
output_df = pd.concat(df_list, ignore_index=True)
output_df['record_id'] = output_df['record_id'].str.replace(r'^S(\d{1,2})$', r'staff_\1')
columns = output_df.columns.tolist()

# Sort the columns alphabetically while keeping 'record_id' at the beginning
columns.remove('record_id')
columns.sort()

# Reinsert 'record_id' at the beginning of the list
columns.insert(0, 'record_id')

# Reorder the columns in output_df
output_df = output_df[columns]

output_df.to_csv('raw_output.csv', index=False)
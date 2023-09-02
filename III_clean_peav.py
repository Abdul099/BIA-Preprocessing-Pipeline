import pandas as pd
import os
import re
import csv
from collections import OrderedDict


## Part 1: Visualize the raw peav file and assess quality
def assess_quality(df):
    # Group the dataframe by the csv_file column
    grouped_data = df.groupby('csv_file')

    # Create a new dataframe to hold the results
    result_df = pd.DataFrame(columns=['CSV File', 'Unique IDs', 'Empty Date', 'Empty ID'])

    # Loop through each group and populate the new dataframe
    for csv_file, group_df in grouped_data:
        empty_date_rows = group_df[group_df['date'].isnull()]
        empty_id_rows = len(group_df[group_df['id'].isnull()])
        num_empty_ids = empty_date_rows['id'].nunique() 
        num_unique_ids = group_df['id'].nunique()
        num_unique_attr = group_df['attribute'].nunique()
        
        # Add the results to the new dataframe
        result_df = result_df.append({
            'CSV File': csv_file,
            'Unique IDs': num_unique_ids,
            'Empty Date': num_empty_ids,
            'Empty ID': int(empty_id_rows/num_unique_attr)
        }, ignore_index=True)

    # Print the new dataframe
    pd.set_option('max_rows', None)
    print(result_df)
    #print("\n")
    pd.reset_option('max_rows')

# set the path to the directory containing the csv file
directory = os.path.join(os.path.dirname(__file__), '..')

# read the csv file into a pandas dataframe
df = pd.read_csv(os.path.join(directory, 'raw_peav.csv'))
print("Initial Quality Report")
assess_quality(df)


### STEP 2: Generate a new cleaned version of PEAV Database 

# A) remove empty date and id rows
df.dropna(subset=['date', 'id'], inplace=True)

# B) Remove all id's that do not contain any numeric characters
df['id'] = df['id'].astype(str)
df = df[~df['id'].str.contains(r'^\D*$')]

def check_staff(input_str):
    # Check if the input is a number between 1 and 55 inclusive
    if input_str.isdigit():
        num = int(input_str)
        if 1 <= num <= 55:
            return True

    # Check if the input starts with 's' followed by a numerical character then anything
    s_pattern = r'^s\d.*'
    if re.match(s_pattern, input_str):
        return True

    # Check if the input starts with 'staff_' followed by anything
    staff_pattern = r'^staff.*'
    if re.match(staff_pattern, input_str):
        return True

    # If none of the conditions are satisfied, return False
    return False


def check_string_format(string): #TODO: -2 is accepted if at the end of the patient but count it as a duplicated patient!!!!!!!!!
    numerical_pattern = r'^\d+$'
    alphanumeric_pattern = r'^[A-Z]{3,4}-\d{2,}.*'
    if re.match(numerical_pattern, string) or re.match(alphanumeric_pattern, string) or check_staff(string):
        return True
    else:
        return False

# filter DataFrame
mask = df['id'].apply(check_string_format)
df = df[mask]

# C) Remove all intra-file duplicate entries
print(f'Total number of PEAV entries before removing intra-file duplicates: {df.shape[0]} Unique ids: {len(set(df["id"]))}')
df.drop_duplicates(keep='first', inplace=True)
print(f'Total number of PEAV entries after removing intra-file duplicates: {df.shape[0]} Unique ids: {len(set(df["id"]))}')

# D) Drop all inter-file duplicates
# create a boolean mask that identifies duplicate rows based on all columns except csv_file
mask = df.duplicated(subset=df.columns.difference(['csv_file']), keep='first')
# drop the duplicate rows based on the boolean mask
df = df[~mask]
print(f'Total number of PEAV entries after removing inter-file duplicates: {df.shape[0]} Unique ids: {len(set(df["id"]))}')

# E) Remove leading zeros in the id field
df['id'] = df['id'].str.lstrip('0')

### STEP 3: Refine the ID values by mapping to hospital ID when possible 
# Read all sheets from the Excel file into a dictionary of DataFrames
key_path = os.path.join(os.path.dirname(__file__), '..', 'key.xlsx')
key_data = pd.read_excel(key_path, sheet_name=None)

# Concatenate all DataFrames from different sheets into one DataFrame
key_df = pd.concat([df for df in key_data.values() if not df.columns[0] in ('', 'Unnamed')], ignore_index=True)
key_df = key_df.loc[:, ~key_df.columns.str.startswith('Unnamed')]

# Read the "old_key_jun23.xlsx" file
old_key_df = pd.read_excel(os.path.join(os.path.dirname(__file__), '..', 'old_key_jun23.xlsx'))
old_key_df = old_key_df.drop_duplicates(subset=["hospital_id", "record_id"])

# Concatenate key_df and old_key_df vertically
key_df = pd.concat([key_df, old_key_df], ignore_index=False)
key_df = key_df.dropna(subset=['hospital_id', 'record_id'])
key_df = key_df.astype(str)

# Drop duplicates based on hospital_id
key_df = key_df.drop_duplicates(subset=['record_id'])

# Set the path to the directory containing the csv file
directory = os.path.join(os.path.dirname(__file__), '..')
key_df.to_csv(os.path.join(directory, 'key.csv'), index=False)

prev_id = None
last_correction = None
# Iterate through rows of df
for index, row in df.iterrows():
    id_value = str(row['id'])  # Convert id_value to string
    # if id_value == prev_id:
    #     df.loc[index, 'id'] = last_correction
    #     continue
    prev_id = id_value
    # Check if id exists in hospital_id column of key_df
    mask = key_df['hospital_id'].astype(str) == id_value
    if mask.any():
        # Get the corresponding record_id
        record_id = key_df.loc[mask, 'record_id'].iloc[0]
        last_correction = record_id
        # Replace id with record_id in df
        df.loc[index, 'id'] = record_id
    else:
        # Check if id exists in accession_id column of key_df
        mask = key_df['accession_id'].astype(str) == id_value
        if mask.any():
            # Get the corresponding record_id
            record_id = key_df.loc[mask, 'record_id'].iloc[0]
            last_correction = record_id
            # Replace id with record_id in df
            df.loc[index, 'id'] = record_id

list1 = list(key_df["record_id"].astype(str))
list2 = list(df['id'].astype(str))
list3 = list(key_df["record_id"].astype(str))

hospital_id_found = set(list1).intersection(list2)
print("Length of list after matching hospitalids to recordids: " + str(len(list2)))
list2 = list(OrderedDict.fromkeys(list2))
print("Length of list after matching hospitalids to recordids and converting to ordered dict: " + str(len(list2)))

with open('sandbox.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Column0', 'Column2'])  # Writing column headers
    writer.writerows(list2)  # Writing the data rows

print(f"num existing hospital ids = {len(set(list3))}")
print(f"num unique ids = {len(set(list2))}")
print(f"num unique ids in common = {len(set(hospital_id_found))}")


def check_string_format_alphanumeric(string):
    alphanumeric_pattern = r'^[A-Z]{3,4}-\d{2,}.*'
    if re.match(alphanumeric_pattern, string) or check_staff(string):
        return True
    else:
        return False
    
# Boolean mask for rows with 'id' in hospital_id_found
mask = df['id'].isin(hospital_id_found) | df['id'].apply(check_staff)
# Filter the DataFrame using the mask
df = df[mask]

# filter DataFrame
mask = df['id'].apply(check_string_format_alphanumeric)
df = df[mask]

for id in df['id'].unique():
    print(id)

df.to_csv('c:/Users/aboud/OneDrive/Documents/Research/BIA DATA 2/peav.csv', index=False)
import pandas as pd
import os
import re

def unify_id_format(input_str):
    if input_str.isdigit():
        return f'staff_{input_str}'
    elif input_str.startswith('staff_'):
        num_part = input_str[len('staff_'):]
        if num_part.isdigit():
            return f'staff_{num_part}'
    elif input_str.startswith('staff'):
        num_part = re.findall(r'\d+', input_str)[0]
        return f'staff_{num_part}'
    elif input_str.startswith('s'):
        num_part = input_str[1:]
        if num_part.isdigit():
            return f'staff_{num_part}'
    return input_str

# set the path to the directory containing the csv file
directory = os.path.join(os.path.dirname(__file__), '..')

# read the csv file into a pandas dataframe
df = pd.read_csv(os.path.join(directory, 'unified_attributes_peav.csv'))

# Create a new DataFrame to hold the additional rows with 'source_file' attribute
additional_rows = []
for (id_val, date_val), group_df in df.groupby(['id', 'date']):
    csv_file_val = group_df['csv_file'].iloc[0]
    additional_row = {'id': id_val, 'date': date_val, 'attribute': 'source_file', 'value': csv_file_val}
    additional_rows.append(additional_row)

# Append the additional rows to the original DataFrame
df = df.append(additional_rows, ignore_index=True)

# Display the modified DataFrame
print(df)

# Save the modified DataFrame to a new CSV file
df.to_csv('modified_cleaned_unified_attributes_peav.csv', index=False)


unique_ids = set(df['id'].astype(str))
print(f'Started off with {len(unique_ids)} unique ids')

# attempt to convert the date column to datetime format
date_col = pd.to_datetime(df['date'], errors='coerce')

# count the number of successful conversions
num_valid_dates = date_col.count()

print(f'There are {num_valid_dates} valid dates out of {df.shape[0]}')

# convert the date column to datetime format
df['date'] = pd.to_datetime(df['date'])

# sort the dataframe by id and date
df = df.sort_values(['id', 'date'])

attr_rank = 115

# count the occurrences of each unique attribute and select the top x number of attributes
top_attributes = df['attribute'].value_counts().head(attr_rank).index

# create a boolean mask for the rows containing the top x attributes
mask = df['attribute'].isin(top_attributes)

row_counts = df.notnull().sum(axis=1)

# filter the original dataframe using the mask
df_filtered = df[mask]

# count the occurrences of each unique id
id_counts = df['id'].value_counts()

# filter the dataframe to keep only the rows that have all the attributes
#df= df[df['id'].isin(id_counts[id_counts>=attr_rank].index)]

# print(df.shape)
unique_ids = set(df['id'].astype(str))
print(f'Ended up with {len(unique_ids)} unique ids')

df.to_csv(os.path.join(directory, 'cleaned_unified_attributes_peav.csv'), index=False)

# Group the rows by 'id' and 'date' and pivot the table
pivot_table = df.pivot_table(index=['id', 'date'], columns='attribute', values='value', aggfunc='first')

# Reset the index to make 'id' and 'date' as regular columns
pivot_table = pivot_table.reset_index()

pivot_table['id'] = pivot_table['id'].astype(str)

# Create an empty list to store the indices of rows to be removed
rows_to_remove = []
numerical_components = pivot_table[pivot_table['id'].str.match(r'^(staff|s)')]['id'].str.extract(r'(\d+)').astype(str)

# Loop through each row in the DataFrame
for index, row in pivot_table.iterrows():
    # Check if the 'id' starts with numericals and also has a corresponding 'staff' or 's' entry
    if row['id'].isdigit() and row['id'] in numerical_components.values:
        rows_to_remove.append(index)
    # Check if the 'id' starts with numericals and the 'Age' is greater than 60
    elif row['id'].isdigit() and float(row['AGE']) > 60:
        rows_to_remove.append(index)
# Remove the rows with indices in 'rows_to_remove'

pivot_table = pivot_table.drop(rows_to_remove)
# Apply the function to the 'id' column

pivot_table['id'] = pivot_table['id'].apply(unify_id_format)

staffs = [str(i) for i in range(1,53)]
for index, row in pivot_table.iterrows():
    # Check if the 'id' starts with numericals and also has a corresponding 'staff' or 's' entry
    if row['id'].startswith('staff_'):
        rows_to_remove.append(index)
        num_part = row['id'][len('staff_'):]
        if num_part in staffs:
            staffs.remove(num_part)
    elif row['id'].startswith('staff'):
        rows_to_remove.append(index)
        num_part = row['id'][len('staff'):]
        if num_part in staffs:
            staffs.remove(num_part)

pivot_table = pivot_table.sort_values(by='id')
earliest_dates = pivot_table.groupby('id')['date'].transform('min')
pivot_table= pivot_table[pivot_table['date'] == earliest_dates]
# Display the resulting pivoted table
print(pivot_table)

# Display the resulting pivoted table with the 'csv_file' column
print(pivot_table)

# Convert the updated pivot table to a CSV file
pivot_table.to_csv('wide_format.csv', index=False)


# Read the CSV file into a DataFrame
csv_data = pd.read_csv('Research_Conference_Data_May11.csv')
# Get the 'id' column from the pivot table
pivot_id = pivot_table['id']
# Get the 'record_id' column from the CSV file
csv_record_id = csv_data['record_id']
# Find the common entries
common_entries = pivot_id[pivot_id.isin(csv_record_id)]
# Get the count of common entries
num_common_entries = len(common_entries)
print("Number of common entries from research conference data:", num_common_entries, '/', len(csv_record_id))

# Find the IDs in the pivot table that are not in the CSV file
missing_ids = pivot_id[~pivot_id.isin(csv_record_id)]
print('number of new ids added: ' + str(len(missing_ids)))
# Print the missing IDs
# for missing_id in missing_ids:
#     print("ID found in pivot table but not in CSV:", missing_id)


# # Read the CSV file into a DataFrame
# csv_data = pd.read_csv('key.csv')
# # Get the 'record_id' column from the CSV file
# key_ids = csv_data['record_id']
# # Find the IDs in the pivot table that are not in the CSV file
# missing_key_ids = key_ids[~key_ids.isin(pivot_id)]
# print('These are the keys thatwere not found', list(missing_key_ids))
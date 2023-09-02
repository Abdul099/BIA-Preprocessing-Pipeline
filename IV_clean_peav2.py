import pandas as pd
import os
import re
import csv

# set the path to the directory containing the csv file
directory = os.path.join(os.path.dirname(__file__), '..')

# read the csv file into a pandas dataframe
df = pd.read_csv(os.path.join(directory, 'peav.csv'))

# for id in df['id'].unique():
#     print(id)

### STEP 4: Group together similar attribute names
pd.set_option('display.max_rows', None)
unique_attrs = df['attribute'].unique()
print(f"Number of Unique Attributes: {len(unique_attrs)}") 

# Load the excel file into a dataframe
allowable_attrs = pd.read_excel('InBody 770_S10 variable comparison.xlsx').iloc[:114]

# Extract the columns you want to compare with the 'attribute' column
columns_to_compare = allowable_attrs.iloc[:, [0, 2, 3, 4]]

# Create a boolean mask that indicates which rows of 'df' meet the condition
mask = df['attribute'].isin(columns_to_compare.values.flatten())

# Create a new dataframe based on the boolean mask
new_df = df[mask]
unique_ids = set(new_df['id'].astype(str))
unique_attrs = new_df['attribute'].unique()

print('number of unique attributes before resolving differences: '+ str(len(unique_attrs)))

attribute_key = pd.read_excel('InBody 770_S10 variable comparison.xlsx', header=None)
attribute_key = attribute_key.iloc[:115]
attribute_key.iloc[:, 1] = attribute_key.iloc[:, 1].astype(int)

def resolve_differences(attribute_value, attribute_key, id=None):
    row, col = (attribute_key == attribute_value).values.nonzero()
    # if (id == 'CHU-46-32-LR' and ('BFM' in attribute_value or 'Control' in attribute_value)):
    #     print('hi')
    if len(row) > 0:
        col_index = col[0]
        row_index = row[0]
        if col_index==0:
            col_index = col[1]
            row_index = row[1]
        if col_index == 2:
            return attribute_key.iloc[row_index, 0]
        elif col_index == 3 or col_index == 4:
            row_num = attribute_key.iloc[row_index, 5]
            if row_num >114:
                return attribute_value
            try:
                a = attribute_key.iloc[int(row_num-1), 0]
            except:
                print('error')
            return attribute_key.iloc[int(row_num-1), 0]
        return attribute_value
        #return row[0], col_index - for debugging
    else:
        print(f"The value {attribute_value} was not found in the dataframe.")


for index, row in new_df.iterrows():
    attribute = row['attribute']
    replacement = resolve_differences(attribute_key=attribute_key, attribute_value=attribute, id =row['id'])
    new_df.at[index,'attribute'] = replacement

new_df = new_df.dropna(subset=['attribute'])
unique_attrs = new_df['attribute'].unique()
print('number of unique attributes  after resolving differences: ' + str(len(unique_attrs)))
print('number of unique ids at the end: ' + str(len(unique_ids)))

# Use the 'isin' function to create a boolean mask for rows with the specified attributes
mask = new_df['attribute'].isin(["ID", "1. ID", "2. ID"])
# Invert the boolean mask using the tilde (~) to get rows that do not match the attributes to remove
new_df = new_df[~mask]

# print(unique_attrs)
new_df.to_csv(os.path.join(directory, 'unified_attributes_peav.csv'), index=False)
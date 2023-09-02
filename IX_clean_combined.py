import os
import sys
import pandas as pd

# set the path to the directory containing the csv file
directory = os.path.join(os.path.dirname(__file__), '..', 'final_dataframes', 'MAIN.csv')
df = pd.read_csv(directory)

# Drop rows with less than 100 non-null entries
df= df.dropna(thresh=100, axis=0)
 
print(df)

##start with basic preprocessing

non_empty_counts = {}

# Start iterating from column number 119
for col in df.columns[118:]:
    non_empty_count = df[col].count()
    non_empty_counts[col] = non_empty_count

# Sort the dictionary by non-empty entry counts in descending order
sorted_columns = sorted(non_empty_counts, key=non_empty_counts.get, reverse=True)

# Print the sorted columns and their non-empty entry counts
for col in sorted_columns:
    count = non_empty_counts[col]
    print(f"Column '{col}' has {count} non-empty entries.")

# Keep columns that start with '1000' regardless of empty entries
columns_to_keep = [col for col in df.columns if col.startswith('1000')]
print(columns_to_keep)
one_thousand_subset = df[['record_id'] + columns_to_keep]
print(one_thousand_subset)

df_lvef = df.copy()
df_lvef = df_lvef.dropna(subset=['lvef'])

# Drop columns with missing values 
df_lvef = df_lvef.dropna(axis=1)

df_lvef = pd.merge(df_lvef, one_thousand_subset, on='record_id', how='inner')

print(df_lvef)

# Define a custom function to map values based on conditions
def map_lvef(value, threshold = 40):
    value = float(value)
    if value >= threshold:
        return 1
    elif value >0 and value <threshold:
        return 0
    else:
        return value

# Apply the custom function to the 'cfs' column and create a new column 'cfs_mapped'
df_lvef['lvef_40'] = df_lvef['lvef'].apply(map_lvef)
df_lvef['lvef_35'] = df_lvef['lvef'].apply(map_lvef, threshold = 35)

print(df_lvef)
df_lvef.to_csv('./final_dataframes/BINARY_LVEF_cleaned_input_output.csv', index=False)

df_cfs = df.copy()
df_cfs = df_cfs.dropna(subset=['cfs'])

# Drop columns with missing values 
df_cfs = df_cfs.dropna(axis=1)

df_cfs = pd.merge(df_cfs, one_thousand_subset, on='record_id', how='inner')

# Define a custom function to map values based on conditions
def map_cfs(value, threshold = 4):
    value = float(value)
    if value >= threshold:
        return 1
    elif value >0 and value <threshold:
        return 0
    else:
        return value

# Apply the custom function to the 'cfs' column and create a new column 'cfs_mapped'
df_cfs['cfs_4'] = df_cfs['cfs'].apply(map_cfs)
df_cfs['cfs_5'] = df_cfs['cfs'].apply(map_cfs, threshold = 5)

print(df_cfs)
df_cfs.to_csv('./final_dataframes/BINARY_CFS_cleaned_input_output.csv', index=False)


df_gripmax = df.copy()
df_gripmax = df_gripmax.dropna(subset=['grip_max'])

# Drop columns with missing values 
df_gripmax = df_gripmax.dropna(axis=1)

df_gripmax = pd.merge(df_gripmax, one_thousand_subset, on='record_id', how='inner')

def map_grip_max(row):
    value = float(row['grip_max'])
    gender = row['Gender']
    
    if gender == 'M':
        threshold = 27
    elif gender == 'F':
        threshold = 16
    else:
        threshold = 4  # Default threshold
        
    if value >= threshold:
        return 1
    elif 0 < value < threshold:
        return 0
    else:
        return value

# Apply the custom function to the 'grip_max' column and create a new column 'grip_max_mapped'
df_gripmax['grip_max_M27F16'] = df_gripmax.apply(map_grip_max, axis=1)
df_gripmax.drop('hf', axis=1, inplace=True)
print(df_gripmax)
df_gripmax.to_csv('./final_dataframes/BINARY_GRIPMAX_cleaned_input_output.csv', index=False)




df_hf = df.copy()
df_hf = df_hf.dropna(subset=['hf'])

# Drop columns with missing values 
df_hf = df_hf.dropna(axis=1)
df_hf = pd.merge(df_hf, one_thousand_subset, on='record_id', how='inner')

print(df_hf)
df_hf.to_csv('./final_dataframes/HF_cleaned_input_output.csv', index=False)


df_death = df.copy()
df_death = df_death.dropna(subset=['death'])

# Drop columns with missing values 
df_death = df_death.dropna(axis=1)
df_death = pd.merge(df_death, one_thousand_subset, on='record_id', how='inner')

print(df_death)
df_death.to_csv('./final_dataframes/Death_cleaned_input_output.csv', index=False)
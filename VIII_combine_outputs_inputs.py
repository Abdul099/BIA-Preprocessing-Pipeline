import os
import sys
import pandas as pd

# set the path to the directory containing the csv file
output_directory = os.path.join(os.path.dirname(__file__), '..', 'raw_output.csv')
output_df = pd.read_csv(output_directory)
output_df['record_id'] = output_df['record_id'].str.replace('XAI', 'DOAC')

input_directory = os.path.join(os.path.dirname(__file__), '..', 'BIA_data_compilation_Aug_4_23.csv')
input_df = pd.read_csv(input_directory)
input_df = input_df.rename(columns={'id': 'record_id'})
merged_df = pd.merge(input_df,output_df, on='record_id', how='inner')
print(merged_df)
merged_df.to_csv('merged_input_output.csv', index=False)

# Find all 'record_id' values in input_df
all_record_ids = input_df['record_id'].tolist()

# Find 'record_id' values in input_df but not in output_df
missing_record_ids = [record_id for record_id in all_record_ids if record_id not in output_df['record_id'].tolist()]

print("Record IDs in input_df but not in output_df:")
print(missing_record_ids)
print(str(len(missing_record_ids)) + " missing id's in total")

all_record_ids = output_df['record_id'].tolist()
missing_record_ids = [record_id for record_id in all_record_ids if record_id not in input_df['record_id'].tolist()]
print("Record IDs in output_df but not in input_df:")
print(missing_record_ids)
print(str(len(missing_record_ids)) + " missing id's in total")


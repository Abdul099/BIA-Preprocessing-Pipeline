import os
import sys
import pandas as pd
from sklearn.model_selection import KFold
from sklearn.model_selection import StratifiedKFold

file_path = os.path.join(os.path.dirname(__file__), '..', 'final_dataframes',"Multi_Weighed_BINARY_CFS_cleaned_input_output.csv")
df = pd.read_csv(file_path)

for x in range(1, 101):
    column_name = f'cfs_w{x}'
    df[column_name] = df['cfs_5'] * x + (1 - df['cfs_5'])

# Identify columns starting with a digit
digit_columns = [col for col in df.columns if col[0].isdigit()]

# Mark duplicate rows based on digit columns
duplicates_mask = df.duplicated(subset=digit_columns, keep='first')

# Identify the removed rows and their duplicates
removed_rows = df[duplicates_mask]
kept_duplicates = df[duplicates_mask == False]

# Remove duplicate rows
df = df[~duplicates_mask]

# Print removed rows and their duplicates
for index, row in removed_rows.iterrows():
    duplicate_index = kept_duplicates.index[(kept_duplicates[digit_columns] == row[digit_columns]).all(axis=1)]
    print(f"Removed Row: {row}, Duplicate Kept: {kept_duplicates.loc[duplicate_index[0]]}")

print("Remaining DataFrame without duplicates:")
print(df)
output_directory = os.path.join(os.path.dirname(__file__), '..', 'final_dataframes', 'MAIN.csv')
df.to_csv(output_directory)


# Number of folds
num_folds = 5

# Initialize KFold
skf = StratifiedKFold(n_splits=num_folds, shuffle=True, random_state=42)
output_directory = os.path.join(os.path.dirname(__file__), '..', 'final_dataframes')

cumulative_test = []
# Split the DataFrame into train and test sets for each fold
for fold, (train_idx, test_idx) in enumerate(skf.split(df, df['cfs']), start=1):
    train_data = df.iloc[train_idx]
    test_data = df.iloc[test_idx]
    
    # Create a folder for the current fold
    fold_directory = os.path.join(output_directory, f"cfs5_fold_{fold}")
    os.makedirs(fold_directory, exist_ok=True)
    
    # Save train and test sets as CSV files
    train_file_path = os.path.join(fold_directory, f"cfs5_Cf{fold}_train.csv")
    test_file_path = os.path.join(fold_directory, f"cfs5_Cf{fold}_test.csv")
    
    train_data.to_csv(train_file_path, index=False)
    test_data.to_csv(test_file_path, index=False)
    cumulative_test.append(test_data) 
    print(f"Fold {fold} data saved in {fold_directory}")

common_row_counts = {}

#sanity check to ensure no overlap among test sets
# Iterate through pairs of DataFrames
for i in range(len(cumulative_test)):
    for j in range(i + 1, len(cumulative_test)):
        # Merge DataFrames to find common rows
        common_rows = pd.merge(cumulative_test[i], cumulative_test[j], how='inner')
        common_row_count = len(common_rows)
        
        # Store the count in the dictionary
        common_row_counts[f'Common_rows_{i+1}_and_{j+1}'] = common_row_count

# Print the counts
for key, count in common_row_counts.items():
    print(f'{key}: {count} common rows')
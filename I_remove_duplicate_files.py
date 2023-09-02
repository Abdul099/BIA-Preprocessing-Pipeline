import os
import pandas as pd 
import shutil 

def compare_csv_files(file1, file2):
    df1 = pd.read_csv(file1, low_memory=False)
    df2 = pd.read_csv(file2, low_memory=False)

    # Get the columns that contain 'id' and 'date'
    id_cols1 = [col for col in df1.columns if ('id' in col.lower() or 'mrn' in col.lower()) and 'study' not in col.lower()]
    date_cols1 = [col for col in df1.columns if 'date' in col.lower() or 'created' in col.lower()]
    id_cols2 = [col for col in df2.columns if  ('id' in col.lower() or 'mrn' in col.lower()) and 'study' not in col.lower()]
    date_cols2 = [col for col in df2.columns if 'date' in col.lower() or 'created' in col.lower()]

    if len(id_cols1)==0 or len(date_cols1)==0: # if file 1 is trash --> file 2 is kept
        print(os.path.basename(file1) + " was not coonsidered")
        return 1
    
    elif len(id_cols2)==0 or len(date_cols2)==0: #if file2 is trash --> file 1 replaces file 2 (very unlikely unless its the first file that got in)
        print(os.path.basename(file2) + " was not considered")
        return -1
    
    id_cols1 = [id_cols1[0]]
    date_cols1 = [date_cols1[0]]
    id_cols2 = [id_cols2[0]]
    date_cols2 = [date_cols2[0]]

    # Check if df1 is a subset of df2
    if set(df1[id_cols1 + date_cols1].itertuples(index=False)) <= set(df2[id_cols2 + date_cols2].itertuples(index=False)):
        return 1
    # Check if df2 is a subset of df1
    elif set(df2[id_cols2 + date_cols2].itertuples(index=False)) <= set(df1[id_cols1 + date_cols1].itertuples(index=False)):
        return -1
    else:
        return 0
    
#####################################################################################################################
#STEP 1: Convert all excels to csv and save them in the tmp file
#####################################################################################################################

# Get the path of the directory containing the script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Get the path of the "Raw Data" folder
data_dir = os.path.join(script_dir, '..', 'Raw Data')

# Create a new directory to save the filtered CSV files
tmp_dir = os.path.join(script_dir, '..', 'Tmp')

filtered_dir = os.path.join(script_dir, '..', 'Filtered Raw')

if not os.path.exists(tmp_dir):
    os.makedirs(tmp_dir)

if not os.path.exists(filtered_dir):
    os.makedirs(filtered_dir)

# Create an empty list to hold the paths of all CSV files
csv_files = []

# Recursively search for Excel files in the "Raw Data" folder and all subfolders
for root, dirs, files in os.walk(data_dir):
    for file in files:
        if file.endswith('.xlsx'):
            print(file)
            # Read the Excel file into a pandas dataframe
            excel_file = os.path.join(root, file)
            df = pd.read_excel(excel_file)

            # Remove empty rows
            df = df.dropna(how='all')
            base_name = os.path.splitext(file)[0]
            csv_file = os.path.join(tmp_dir, f"{base_name}.csv")
            
            # Check if the file already exists in csv_files
            counter = 1
            while csv_file in csv_files:
                csv_file = os.path.join(tmp_dir, f"{base_name}_renamed_{counter}.csv")
                counter += 1
            csv_files.append(csv_file)
            df.to_csv(csv_file, index=False)

#############################################################################################################################################
#STEP 2: Copy all original csv files to tmp and filter out all the redundant csv files from excel and original csv then save in final folder
#############################################################################################################################################

# Recursively search for CSV files in the "Raw Data" folder and all subfolders
for root, dirs, files in os.walk(data_dir):
    for file in files:
        if file.endswith('.csv') or file.endswith('.CSV'):
            csv_files.append(os.path.join(root, file))
print('these are the csv files', csv_files)
# Remove '<' and '>' characters from all CSV files
for file in csv_files:
    with open(file, 'r') as f:
        lines = f.readlines()
    with open(file, 'w') as f:
        for line in lines:
            f.write(line.replace('<', '').replace('>', ''))

final_list = []
for new_file in csv_files:
    verdict = 0
    # for existing_file in final_list:
    #     verdict = compare_csv_files(new_file, existing_file)
    #     if verdict == 0:
    #         continue #move on to the next file that already exists in the csv file list
    #     elif verdict ==1: #if new file is already a subset of existing file break the for loop and dont bother looking again
    #         break
    #     else: #case of -1
    #         final_list.remove(existing_file)
    if verdict ==0:
        final_list.append(new_file)


print("\nFiles Kept")
for file in final_list:
    print(os.path.basename(file)) 
print("Out of " + str(len(csv_files)) + ", " + str(len(csv_files)-len(final_list)) + " files removed")

####################################################################################
#Step 3: Copy the final list of csv files into the new directory - Filtered Raw
####################################################################################

def rename_duplicate_files(file_list):
    file_names = [os.path.splitext(os.path.basename(file))[0] for file in file_list]
    counts = {}

    for i, file in enumerate(file_list):
        file_name = file_names[i]
        if file_name in counts.keys():
            print(file_name)
            counts[file_name] += 1
            new_file_name = f"{file_name}_{counts[file_name]}_renamed"
            new_file_path = os.path.join(os.path.dirname(file), f"{new_file_name}.csv")
            print(new_file_name)
            os.rename(file, new_file_path)
            file_list[i] = new_file_path
        else:
            counts[file_name] = 0

    return file_list

final_list = rename_duplicate_files(final_list)

for file in final_list: 
    file_name = os.path.basename(file)
    dest_file = os.path.join(filtered_dir, file_name)
    shutil.copy(file, dest_file)
shutil.rmtree(tmp_dir)
print("Files copied to " + filtered_dir)


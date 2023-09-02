import pandas as pd
import os
import re

# set the path to the directory containing the csv file
directory = os.path.join(os.path.dirname(__file__), '..')

# read the csv file into a pandas dataframe
df = pd.read_csv(os.path.join(directory, 'wide_format.csv'))

###STEP 1: FIND THE MACHINES

def is_filename_in_folder(filename, folder_path):
    for root, _, files in os.walk(folder_path):
        for file in files:
            if filename in file.split('.')[0] or file.split('.')[0] in filename:
                return True
    return False

def determine_machine(row):
    raw_data_folder = os.path.join(os.path.dirname(__file__), '..', 'Raw Data')

    # Get the source file from the row
    source_file = row['source_file']

    # Check if the source file is in any of the subfolders of "InBody 770"
    inbody_770_folder = os.path.join(raw_data_folder, 'InBody 770')
    if is_filename_in_folder(source_file, inbody_770_folder):
        return 'InBody 770'

    # Check if the source file is in any of the subfolders of "InBody s10"
    inbody_s10_folder = os.path.join(raw_data_folder, 'InBody s10')
    if is_filename_in_folder(source_file, inbody_s10_folder):
        return 'InBody s10'

    # Check if the source file is in any of the subfolders of "Rosie"
    rosie_folder = os.path.join(raw_data_folder, 'Rosie')
    if is_filename_in_folder(source_file, rosie_folder):
        return 'InBody s10'

    # If the source file is not found in any of the subfolders, return 'Unknown'
    return 'Unknown'

df['machine'] = df.apply(determine_machine, axis=1)

#### STEP 2: Update posture for INBODY 770 and unify divided column names :
# Use the 'fillna' method to fill missing values in 'Posture' with corresponding values from '116.Posture'
df['Posture'].fillna(df['116.Posture'], inplace=True)

# Drop the '116.Posture' column since it's no longer needed
df.drop(columns=['116.Posture'], inplace=True)# Define a function to update the 'Posture' column based on the 'machine' column

# Use the 'fillna' method to fill missing values in 'LegLeanMass' with corresponding values from '116.Posture'
df['LegLeanMass'].fillna(df['37.Leg Lean Mass'], inplace=True)

# Drop the '37.Leg Lean Mass' column since it's no longer needed
df.drop(columns=['37.Leg Lean Mass'], inplace=True)


def update_posture(machine, posture):
    if machine == 'InBody 770':
        return 'Standing Posture'
    else:
        return posture

# Apply the 'update_posture' function to the 'Posture' column using 'loc'
df['Posture'] = df.apply(lambda row: update_posture(row['machine'], row['Posture']), axis=1)


#### STEP 3: FIX Discrepencies for certain columns (e.g. unit conversions)
import re

def convert_to_cm(height):
    # Check if the height is in the format "5ft. 03.1in."
    if isinstance(height, str) and re.match(r'\d+ft\. \d+\.\d+in\.', height):
        feet, inches = height.split('ft.')[0], height.split('ft.')[1].split('in.')[0]
        feet = int(feet.strip())
        inches = float(inches.strip())
        total_inches = feet * 12 + inches
        cm = total_inches * 2.54
        return cm, True
    # Check if the height is in the format "5' 6.0''" or "5' 6.0\""
    elif isinstance(height, str) and re.match(r'(\d+)\'\s*((\d+\.\d+)\'\'|"")', height):
        feet, inches = height.split("'")[0], height.split("'")[1].split('"')[0]
        feet = int(feet.strip())
        inches = float(inches.strip())
        total_inches = feet * 12 + inches
        cm = total_inches * 2.54
        return cm, True
    else:
        return height, False
    
df['Height_Modified'] = False
#Apply the 'convert_to_cm' function to the 'height' column
df['Height'], df['Height_Modified'] = zip(*df['Height'].apply(convert_to_cm))

# Use the 'str.replace' method to remove the word "level" from the entries in the column
df['VFL(VisceralFatLevel)'] = df['VFL(VisceralFatLevel)'].str.replace('level ', '')
df['VFL(VisceralFatLevel)'] = pd.to_numeric(df['VFL(VisceralFatLevel)'])


# Define the desired order of columns
desired_order = ['id', 'date', 'source_file', 'machine', 'Height', 'Weight', 'AGE', 'Gender', 'Posture', 'BMI(BodyMassIndex)']
# Get the list of remaining columns (excluding the desired ones)
remaining_columns = [col for col in df.columns if col not in desired_order]

# Reorder the DataFrame to match the desired order and keep the remaining columns
df = df[desired_order + remaining_columns]

# Display the DataFrame to check the updated order of columns
print(df)


def modify_weight(row, col_name='Weight'):
    if row['Height_Modified']:  # Check if the height was modified earlier
        return row[col_name] / 2.2
    return row[col_name]

for col_name in ['Weight','ECWofLeftArm','ECWofLeftLeg','ECWofRightArm','ECWofRightLeg','ECWofTrunk','ICW(IntracellularWater)',
                 'ICWofLeftArm','ICWofLeftLeg','ICWofRightArm','ICWofRightLeg','ICWofTrunk', 'LBM(LeanBodyMass)','LBMofLeftArm',
                 'LBMofLeftLeg','LBMofRightArm','LBMofRightLeg','LBMofTrunk','LegLeanMass', 'TBW(TotalBodyWater)', 'TBWofLeftArm',
                 'TBWofLeftLeg','TBWofRightArm','TBWofRightLeg','TBWofTrunk']:
    print(col_name)
    df[col_name] = df.apply(modify_weight, axis=1, col_name=col_name)



df.to_csv('wide_format_cleaned.csv', index=False)
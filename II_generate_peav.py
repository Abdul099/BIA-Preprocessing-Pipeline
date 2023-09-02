import csv
import os

# set the path to the directory containing the csv files
directory = os.path.join(os.path.dirname(__file__), '../Filtered Raw')

# initialize the rows list
rows = []

id_vals = []

# loop through each file in the directory
for filename in os.listdir(directory):
    if (filename.endswith('.csv') or filename.endswith('.CSV')) and filename not in ['BIA,SECA,DXA,PSOAS DATA 20160829.csv', 'Frailty_DATA_2023-03-09_0746.csv']:
        # open the csv file
        with open(os.path.join(directory, filename), 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # get the column names
            fieldnames = reader.fieldnames
            
            # find the id and date column indices
            id_indices = [i for i, field in enumerate(fieldnames) if ('id' in str(field.lower()) or 'mrn' in str(field.lower())) and i<10]
            id_index = id_indices[0] if id_indices else None
            date_index = next((i for i, field in enumerate(fieldnames) if ('date' in field.lower() or 'created' in field.lower()) and not any(x in field.lower() for x in ['birth', 'dob'])), None)
            
            count = 0
            while id_index is None or date_index is None:
                # if the headers are not found in the top row, try looking for the headers in rows 2-4 (see file data_bia_2016-01-11.csv)
                csvfile.readline()
                reader = csv.DictReader(csvfile)
                fieldnames = reader.fieldnames
                id_index = next((i for i, field in enumerate(fieldnames) if ('id' in str(field.lower()) or 'mrn' in str(field.lower())) and i<10), None)
                date_index = next((i for i, field in enumerate(fieldnames) if ('date' in field.lower() or 'created' in field.lower()) and not any(x in field.lower() for x in ['birth', 'dob'])), None)
                count +=1
                if count ==3:
                    break

            print(filename)
            if(id_index is None):
                print("No ID Found")
                continue
            if(date_index is None):
                print("No Date Found")
            
            if fieldnames and id_index and date_index:
                print(fieldnames[id_index])
                print(fieldnames[date_index])
            
            count = 0
            # loop through each row in the csv file
            for row in reader:
                count_nonempty = sum(1 for value in row.values() if value and '=' not in value)
                if count_nonempty <= 2: # if the row has 2 non-empty elements or less we do not consider it
                    continue
                # get the id and date values
                id_value = row[fieldnames[id_index]].lstrip("0") if id_index is not None else None
                id_vals.append(id_value)
                date_value = row[fieldnames[date_index]] if date_index is not None else None
                if count == 0: 
                    print(id_value)
                    print(date_value)
                    for index in id_indices:
                        if len(id_value)>0:
                            break
                        id_index = index
                        id_value = row[fieldnames[id_index]] if id_index is not None else None
                        print(id_value)
                count +=1
                # loop through each column in the row and add a new row to the rows list
                for i, field in enumerate(fieldnames):
                    if i not in [id_index, date_index] and not '=' in id_value:
                        rows.append({
                            'csv_file': filename,
                            'id': id_value,
                            'date': date_value,
                            'attribute': field,
                            'value': row[field]
                        })
    print("\n")
# write the rows to a new csv file
with open(os.path.join(os.path.dirname(__file__), '..', 'raw_peav.csv'), 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['csv_file', 'id', 'date', 'attribute', 'value'])
    writer.writeheader()
    writer.writerows(rows)


print(sorted(id_vals))
print(len(set(id_vals)))
import csv
import arrow
import pandas as pd
from os.path import exists


input_file = 'my_csv.csv'
data_path = 'my_data_path/'

minutes_to_shift_by = 5

# Initialization of the starting and first ending
starting_datetime_obj = arrow.get('2020.11.26 21:00:00.000')
ending_datetime_obj = starting_datetime_obj.shift(minutes=+minutes_to_shift_by)

list_of_dicts = list()
columns = ['A', 'B', 'C', 'D', 'E']


def line_generator():
    with open(input_file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        # Skip the header
        next(csv_reader)
        for line in csv_reader:
            yield line


LG = line_generator()

for line in LG:
    my_dict = dict()
    my_dict[columns[0]] = arrow.get(line[0]).timestamp
    my_dict[columns[1]] = line[1]
    my_dict[columns[2]] = line[2]
    my_dict[columns[3]] = line[3]
    my_dict[columns[4]] = line[4]
    list_of_dicts.append(my_dict)

    # If the 5 minute interval has ended
    if arrow.get(line[0]) >= ending_datetime_obj:
        # This is how I declared my filename, yours may differ
        filename = data_path + str(line[0]).replace('\n', '').replace('.', '-').replace(':', '-') + '.csv'

        # Create the dataframe of the list of dictionaries
        df = pd.DataFrame(columns=columns)

        new_list = list_of_dicts.pop(-1)

        # Dont add the last item to the dataframe, as it belongs to the next file / iteration
        for d in list_of_dicts[:-1]:
            df = df.append(d, ignore_index=True)

        # Write the dataframe to the disk
        df.to_csv(filename, index=False)

        # Ensure that the dataframe was written to the disk
        assert exists(filename)

        # Reset / Clear the list of dictionaries
        list_of_dicts = list()

        # Add the last element of the previous list_of_dicts, as the if condition above only executes, if it is past
        # the interval time period, therefore it does not belong the previous file/dataframe, but rather the new one
        list_of_dicts.append(new_list)

        # Set the next interval(s)
        # These are globals
        starting_datetime_obj = ending_datetime_obj
        ending_datetime_obj = ending_datetime_obj.shift(minutes=+minutes_to_shift_by)

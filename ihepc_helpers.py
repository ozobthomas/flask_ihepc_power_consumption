import zipfile
from datetime import datetime
import time
import urllib
import os
from ihepcmodels import PowerConsumption

columns = ['date', 'time', 'global_active_power', 'global_reactive_power',
 'voltage', 'global_intensity', 'sub_metering_1', 'sub_metering_2',
 'sub_metering_3']
float_features = ['global_active_power', 'global_reactive_power',
 'voltage', 'global_intensity', 'sub_metering_1', 'sub_metering_2',
 'sub_metering_3']

def to_float_errorless(value):
    float_value = None
    try:
        float_value = float(value)
    except ValueError:
        pass
    return float_value


def parse_line(line):
    line_dict = None
    try:
        line_split = str(line, 'utf-8', 'ignore').strip().replace('?','').split(';')
        line_dict = {columns[i]:line_split[i] for i in range(0,9)}
        for feature in float_features:
            line_dict[feature] = to_float_errorless(line_dict[feature])
            
        date_time_string = "%s %s" % (line_dict['date'],line_dict['time'])
        line_dict['date_time'] = datetime.strptime(date_time_string,'%d/%m/%Y %H:%M:%S')
        line_dict['date'] = datetime.strptime(line_dict['date'],'%d/%m/%Y')
        del line_dict['time']
    except ValueError:
        print(line, line_dict)
        raise ValueError()
    return line_dict

def load(file, db_session, batch_size=20000):
    line_number = 0
    parsed = []
    with zipfile.ZipFile(file) as archive:
        for unzipped_file in archive.infolist():
            with archive.open(unzipped_file,mode='r') as f:
                for line in f:
                    line_number += 1
                    if line_number == 1:
                        continue
                    line_dict = parse_line(line)
                    line_dict['line_no'] = line_number
                    parsed.append(line_dict)
    print ("------------------------")
    print (type(parsed), len(parsed))
    batches = [parsed[i: i+batch_size] for i in range(0, len(parsed), batch_size)]

    for b in batches:
        db_session.bulk_insert_mappings(PowerConsumption, b)

    db_session.commit()
    return line_number

def truncate(db_session):
    db_session.execute("delete from power_consumption")
    db_session.commit()

def download(from_url, to_directory):
        dest_dir = to_directory
        dest_filename = "%s.household_power_consumption.zip" % (time.strftime('%Y%m%d.%H%M%S'))
        local_filename, headers = urllib.request.urlretrieve(from_url,
                                                  os.path.join(dest_dir, dest_filename))
        return {'fileid':dest_filename, 'file_name':local_filename, 'headers':headers}

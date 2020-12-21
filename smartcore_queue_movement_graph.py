import datetime
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

# view all rows/columns in dataframe
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

def get_file_extract_date(file):
    date_str = file[16:27].replace('-','')
    date = datetime.datetime.strptime(date_str, "%d%b%Y").date()
    
    return date

def read_smartcore_data():
    # local variables
    data_dir = 'A:\JHC Support Team\Jerry Edwards\smartcore_sostenuto_rec\smartcore_graphs'
    temp_document = '~$'
    smartcore_data = pd.DataFrame({'client_ref': [],
                                   'status': [],
                                   'queue_type': [],
                                   'queue_position': [],
                                   'extract_date': [],
                                   'incident': []})

    os.chdir(data_dir)
    files = glob.glob('*.xlsx')

    for file in files:
        
        if temp_document not in file:
            extract_date = get_file_extract_date(file)
            
            data = pd.read_excel(file, index_col=0, header=0)
            data = data[['Client Reference', 'Client Status', 'Queue Type', 'Queue Position']]
            data['extract_date'] = extract_date
            data['incident'] = data.index
            data.columns = ['client_ref', 'status', 'queue_type', 'queue_position','extract_date','incident']
            smartcore_data = pd.concat([smartcore_data, data])
    
    # ignore rows with: blank queue position number, queue type = Plan or PRJ, status = Closed
    smartcore_data = smartcore_data.dropna(axis=0, subset=['queue_position'])
    smartcore_data = smartcore_data.drop(smartcore_data.index[smartcore_data.queue_type == 'Plan'])
    smartcore_data = smartcore_data.drop(smartcore_data.index[smartcore_data.queue_position == 'PRJ'])
    smartcore_data = smartcore_data.drop(smartcore_data.index[smartcore_data.status == 'Closed'])
    
    # convert queue position to integer
    smartcore_data['incident'] = pd.to_numeric(smartcore_data['incident'], downcast='signed')
    smartcore_data['queue_position'] = pd.to_numeric(smartcore_data['queue_position'], downcast='signed')
    
    # order by INC number
    smartcore_data = smartcore_data.sort_values(by=['incident'])
    
    # reset index
    smartcore_data = smartcore_data.reset_index(inplace=False,drop=True)
    
    return smartcore_data

def group_data_by_incident(smartcore_data):
    incident_dict = {}
    queue_position_list = []
    extract_date_list = []
    save_incident = ''
    
    for i, row in smartcore_data.iterrows():
        
        # only add to list if not the first record
        if i > 0 and i != len(smartcore_data):
            if save_incident == row.incident:
                queue_position_list.append(row.queue_position)
                extract_date_list.append(row.extract_date)
                save_incident = None
            else:
                incident_dict[row.incident] = [queue_position_list[:]]
                incident_dict[row.incident].append(extract_date_list[:])
                queue_position_list.clear()
                extract_date_list.clear()
                queue_position_list.append(row.queue_position)
                extract_date_list.append(row.extract_date)
                
        # append the last list started to the dict
        elif i == len(smartcore_data):
            incident_dict[row.incident] = queue_position_list[:]
            incident_dict[row.incident].append(extract_date_list[:])
            
        # else this is i=0 so just add to list
        else: 
            queue_position_list.append(row.queue_position)
            extract_date_list.append(row.extract_date)
        
        save_incident = row.incident
    
    #print(incident_dict)
    return incident_dict
    
def plot_graph(incident_dict):

    ys, xs = zip(*incident_dict.values())
    keys_list = list(incident_dict)
    
    for i in range(0, len(incident_dict)):
        plt.plot(xs[i], ys[i], label = keys_list[i])
        
        for j in range(0, len(xs[i])):
            if j == (len(xs[i]) - 1):
                plt.annotate(keys_list[i], 
                             (xs[i][j], ys[i][j]),
                             textcoords="offset points",
                             xytext=(0,10),
                             ha='right',
                             size=5)

    plt.title('Incident Queue Position')
    plt.xlabel('Date')
    plt.ylabel('Queue Position')
#    plt.legend(bbox_to_anchor=(1.05, 1), ncol=1)
    plt.show()             
    
        
# mainline code
smartcore_data = read_smartcore_data()
#print(smartcore_data)
incident_dict = group_data_by_incident(smartcore_data)
#print(incident_dict)
plot_graph(incident_dict)

"""
Created on Sat Dec 19 13:40:05 2020

@author: edwardsje
"""

import glob
import pandas as pd
from datetime import datetime

def read_data():

    # if you have the excel doc open, it creates a temp doc with $ in it so let's ignore these
    temp_doc = '$'
    xls_files = glob.glob('*.xls')
    xlsx_files = glob.glob('*.xlsx')

    # read in both files, they will either have extension .xls or .xlsx
    for xls_file in xls_files: 
        print('loading xls files...')
        if temp_doc not in xls_file:
            if 'Incident_Search' in xls_file:
                smartcore_data = set_smartcore_data(xls_file)
            elif 'sostenuto' in xls_file:
                sostenuto_data = set_sostenuto_data(xls_file)
       
    for xlsx_file in xlsx_files: 
        print('loading xlsx files...')
        if temp_doc not in xlsx_file:
            if 'Incident_Search' in xlsx_file:
                smartcore_data = set_smartcore_data(xlsx_file)
            elif 'sostenuto' in xlsx_file:
                sostenuto_data = set_sostenuto_data(xlsx_file)
    
    # drop the last row of sostenuto data because it's false data
    #sostenuto_data = sostenuto_data[:-1]
    
    return smartcore_data, sostenuto_data


def set_smartcore_data(file):
    print('loading smartcore data...')
    data = pd.read_excel(file, header=0)
    smartcore_data = data[['Incident Number','Legacy Job No.','Heading','Client Reference','State','Current Priority','Raised On','Found in Version','Target Delivery']]
    return smartcore_data


def set_sostenuto_data(file):
     print('loading sostenuto data...')
     data = pd.read_excel(file, header=4)
     sostenuto_data = data[['Problem ID','Legacy Problem ID','Problem Summary','State','JHC Job ID','Owned By Account']]
     return sostenuto_data
 
    
def reconcile_data(smartcore_data, sostenuto_data):
    
    reconciled_data = pd.DataFrame({})
    unreported_prbs = pd.DataFrame({})
    
    # for each prb in the sostenuto data, find it in the smartcore data and merge
    for i, prb in sostenuto_data.iterrows():
        for j, inc in smartcore_data.iterrows():

            # get rid of any rubbish records by checking it is a PRB first
            if 'PRB' in str(prb['Problem ID']):
            
                # reconcile on prb or inc
                if (str(prb['Problem ID']).strip() == str(inc['Client Reference']).strip()) or (str(prb['JHC Job ID']).strip() == ('INC-' + str(inc['Incident Number'])).strip()):
                    prb_inc = pd.concat([prb, inc], ignore_index=True)
                    reconciled_data = reconciled_data.append(prb_inc, ignore_index=True)
                    print('reconciled ' + prb['Problem ID'] + '...')
                    break
            
                # we didn't find the PRB in smartore
                if j == len(smartcore_data)-1:
                    unreported_prbs = unreported_prbs.append(prb)
                    print('unreconciled ' + prb['Problem ID'] + '...')
    
    return reconciled_data, unreported_prbs


def categorise_reconciled_data(rec):
    
    print('categorising incidents...')
    
    open_closed = pd.DataFrame({})
    open_open = pd.DataFrame({})
    closed_closed = pd.DataFrame({})
    
    for i, row in rec.iterrows():
        # 1. closed in smartcore and sostenuto
        if (row['Smartcore State'] == 'Closed' or 'Solution Delivered' in row['Smartcore State']) and row['Sos State'] in ['Closed','Resolved']:
          closed_closed = closed_closed.append(row)   
            
        # 2. open in smartcore and sostenuto    
        elif row['Smartcore State'] != 'Closed' and 'Solution Delivered' not in row['Smartcore State'] and row['Sos State'] == 'Open':
            open_open = open_open.append(row)
            
        # 3. everything else that requires attention i.e. closed in smartcore, but open in sos and vice versa
        else:
            open_closed = open_closed.append(row)
    
    return open_closed, open_open, closed_closed
            

def create_excel_sheet(open_closed_data, open_open_data, closed_closed_data, unrec):
    
    column_names = ['Smartcore INC','Sos PRB','Sos Legacy PRB','FNZ Job','Heading','Owner','Smartcore State','Sos State','Sos INC','Current Priority','Raised On','Found in Version','Target Delivery']

    open_closed_data = open_closed_data[column_names]
    open_open_data = open_open_data[column_names]
    closed_closed_data = closed_closed_data[column_names]
    
    today = datetime.today().strftime('%Y-%m-%d')
    filename = 'smartcore_sosteunto_rec_' + today + '.xlsx'
    
    try:
        file = pd.ExcelWriter(filename)
        open_closed_data.to_excel(file, 'open-closed', index = False)
        unrec.to_excel(file,'unreconciled', index = False)
        open_open_data.to_excel(file, 'open-open', index = False)
        closed_closed_data.to_excel(file, 'closed-closed', index = False)
        file.save()
    except:
        print('error writing excel document - please make sure document is closed')
        
        
# mainline code ---------------------------------------------------------------
smartcore_data, sostenuto_data = read_data()

rec, unrec = reconcile_data(smartcore_data, sostenuto_data)

rec.columns = ['Sos PRB','Sos Legacy PRB','Sos Summary','Sos State','Sos INC','Owner','Smartcore INC','FNZ Job','Heading','Smartcore PRB','Smartcore State','Current Priority','Raised On','Found in Version','Target Delivery']
del rec['Smartcore PRB']

# with the reconciled data, split into 3 dataframes:
# 1. closed in smartcore and sostenuto
# 2. open in smartcore, open in sostenuto
# 3. closed in smartcore and open in sostenuto and vice versa (i.e. requires attention)

open_closed_data, open_open_data, closed_closed_data = categorise_reconciled_data(rec)

create_excel_sheet(open_closed_data, open_open_data, closed_closed_data, unrec)

print('reconciliation complete')

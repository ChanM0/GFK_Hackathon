import pandas as pd
import numpy as np
from pandas import Series, DataFrame

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 1000)

# address must be changed depending on OS and machine
address = './CSV_FILES/GfK_POS_Tracking-Smart_Audio_Home_Systems_DE_&_UK_python.csv'
data = pd.read_csv(address,low_memory=False)

data.columns = ['COUNTRY','CHANNEL' ,'BRAND','MODEL', 'ID','YEAR','PERIOD','SMART CONNECT','SMART H. ECOSYS',
'OPERATING AI','AIRPLAY','GOOGLECAST/HOME','BLUETOOTH','ETHERNET','WIFI','HEIGHT IN MM','WIDTH IN MM','HIGH-RES AUDIO',
'TYPE OF DOCKING','MULTIROOM','NO.SPEAKERBOXES','OUTPUT CHANNEL','Streaming Connection','Streaming Technology',
'USB CONNECTION','WATTAGE TOTAL','Sales Units','Sales Value EUR','Price EUR','Sales Value USD','Price USD']

data.columns.str.replace(' ', '_')

Germany = data[data.COUNTRY=='Germany']
GB = data[data.COUNTRY=='Great Britain']
temp = data['SMART CONNECT']
print(data['SMART CONNECT'].value_counts())
print(' ')

# print(pd.crosstab(data['OPERATING AI'], data['SMART CONNECT']))
#
# AI = data[data.OPERATING_AI!='n.a.' or data.OPERATING_AI!='NOT APPLICABLE']
# print(temp)
#
# Germany.to_csv('./CSV_FILES/Germany.csv')
# GB.to_csv('./CSV_FILES/GreatBritain.csv')

# temp = data[data.]
print('')
# print(pd.crosstab(data['ID'], data['CHANNEL']))
print('')
temp2 = data[data.MODEL=='Suppressed']
print(temp2)

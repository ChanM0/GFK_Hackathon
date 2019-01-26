import pandas as pd
import numpy as np
from pandas import Series, DataFrame
from decimal import Decimal


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 1000)

country_options = ['Germany','Great,Britain']
channel_options = ['Consumer,Electronic,Stores','Mass,Merchandisers/DIYSs']
year_options = [2016,2017,2018]
smart_connect_options = ['SMART,APP,CTRL.','VOICE,INTERAC.','VOICE,CONT.ONLY','n.a.','NO',Decimal('nan')]
smart_h_ecosys_options = ['n.a.','NOT,APPLICABLE','GOOG+AMZN','AMAZON,ALEXA','GOOGLE,HOME','APPLE,HOMEKIT',Decimal('nan')]
operating_ai_options = ['n.a.','NOT,APPLICABLE','GOOGLE,ASSISTAN','APPLE,SIRI','AMAZON,ALEXA',Decimal('nan')]
airplay_options = ['NO,AIRPLAY','AIRPLAY',Decimal('nan')]
googlecast_home_options = ['n.a.','NO,GOOGLECAST','GOOGLECAST/HOME',Decimal('nan')]
bluetooth_options = ['WITH,BLUETOOTH','W/O,BLUETOOTH','OPTIONAL',Decimal('nan')]
ethernet_options = ['NO,ETHERNET','ETHERNET',Decimal('nan')]
wifi_options = ['WIFI,BUILT-IN','NO,WIFI','WIFI,READY',Decimal('nan')]
high_res_audio_options = ['NO,HIGH-RES,AUD','HIGH-RES,AUDIO','n.a.',Decimal('nan')]
type_of_docking_options = ['BLUETOOTH,ONLY','NO,DOCKING/STRE','IOS/AIRPLAY','OTH.DOCK/STREAM','ANDR/GOOGLE,HOM','MULTIPLE']
multiroom_options = ['MULTIROOM','NO,MULTIROOM',Decimal('nan')]
no_speakerboxes_options = ['1','n.a.','0','2']
output_channel_options = [2,0]
streaming_connection_options = ['Bluetooth+Wifi','Bluetooth+Ethernet+Wifi','Wifi+Ethernet','Wifi','Ethernet','Bluetooth+Ethernet',Decimal('nan')]
streaming_technology_options = ['BT+UPNP','Proprietary,+BT','Bluetooth','n.a.','UPNP','BT+Airplay+UPNP',
                                'Proprietary,+BT,+Airplay,+UPNP','Airplay+UPNP','BT+Airplay,+UPNP,+Google,Cast/Home',
                                'Proprietary','Airplay','BT+UPNP,+Google,Cast/Home','BT+Google,Cast/Home','Bluetooth+Airplay',
                                'BT+Airplay,+Google,Cast/Home','Proprietary,+BT,+Airplay','Proprietary+Airplay',Decimal('nan')]
usb_connection_options = ['NO,USB','USB,TYPE-A/B',Decimal('nan')]
wattage_total_options = ['unknown','40','70','30','110','80','n.a.','150','90','60','45','300','5'
                         ,'240','140','154','50','20','29','75','14','55','92','38','15','320','25'
                         ,'10','72','13','100','130','200','120','18','65','250','180','175','36'
                         ,'17','450',Decimal('nan'),'35']

data = pd.DataFrame()

pivot_data_table = pd.DataFrame()
data_stats_table = pd.DataFrame()

address = './CSV_FILES/By_Country_POS_Tracking/GreatBritain.csv'

Germany_DF = pd.read_csv(address)
total_units = Germany_DF[['ID','MODEL', 'Sales Units']] # new DF with desired values
total_units = total_units.sort_values(by=['ID']) # sort DF by ID
total_units = total_units.reset_index(drop=True) # re-index
total_units['Sales Units'] = pd.to_numeric(total_units['Sales Units'], errors='coerce')
# print(total_units.head(3))

# begin running sum for each id algorithm
i = 0
r_sum = 0
stored_id = total_units['ID'].loc[i]
r_sum_DF = pd.DataFrame(columns=['MODEL','total_units_sold'])
# print(r_sum_DF)

for i in range(len(total_units)):
    if stored_id == total_units['ID'].loc[i]:
        r_sum = r_sum + total_units['Sales Units'].loc[i]
    else:
        # temp_DF = pd.DataFrame([[total_units['MODEL'].loc[i-1], r_sum]], columns=['MODEL','total_units_sold'])
        r_sum_DF.loc[len(r_sum_DF)] = [total_units['MODEL'].loc[i-1], r_sum]
        r_sum = total_units['Sales Units'].loc[i]
        stored_id = total_units['ID'].loc[i]

r_sum_DF = r_sum_DF.sort_values(by=['total_units_sold'],ascending=False)
r_sum_DF = r_sum_DF.reset_index(drop=True)
r_sum_DF.to_csv('./CSV_FILES/By_Country_POS_Tracking/GreatBritain_Units_Sold_by_ID.csv',index = None, header = True)

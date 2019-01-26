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

address = './CSV_FILES/GfK_POS_Tracking-Smart_Audio_Home_Systems_DE_&_UK_python.csv'

unique_ids =[]

class sanitizeDataFrame():
    def __init__(self,data):
        self.data = data
        self.monthsOrdered = ['16-Dec', '17-Jan', '17-Feb', '17-Mar', '17-Apr', '17-May', '17-Jun', '17-Jul', '17-Aug', '17-Sep', '17-Oct', '17-Nov', '17-Dec', '18-Jan', '18-Feb', '18-Mar', '18-Apr', '18-May','18-Jun', '18-Jul', '18-Aug', '18-Sep', '18-Oct', '18-Nov']
        self.columnsThatNeedATypeChange = ['sales_units', 'sales_value_eur', 'price_eur', 'sales_value_usd', 'price_usd']
        self.columnsToBeSortedBy = ['id','channel' ,'year','period']
        self.pivotcolumnsToBeSortedBy = ['id']
        self.setColumnNames = ['COUNTRY','CHANNEL' ,'BRAND','MODEL', 'ID','YEAR','PERIOD','SMART CONNECT','SMART H. ECOSYS', 'OPERATING AI','AIRPLAY','GOOGLECAST/HOME','BLUETOOTH','ETHERNET','WIFI','HEIGHT IN MM','WIDTH IN MM','HIGH-RES AUDIO', 'TYPE OF DOCKING','MULTIROOM','NO.SPEAKERBOXES','OUTPUT CHANNEL','Streaming Connection','Streaming Technology', 'USB CONNECTION','WATTAGE TOTAL','Sales Units','Sales Value EUR','Price EUR','Sales Value USD','Price USD']
        self.measurements_columns = ['height_in_mm','width_in_mm']
        self.key_stats_columns = [ 'id', 'country', 'channel', 'year', 'period', 'height_in_mm', 'width_in_mm','sales_units', 'sales_value_eur', 'price_eur', 'sales_value_usd', 'price_usd']
        self.pivot_key_columns = ['id','height_in_mm', 'width_in_mm','brand', 'model','smart_connect', 'smart_h._ecosys', 'operating_ai', 'airplay', 'googlecast/home', 'bluetooth', 'ethernet', 'wifi', 'high-res_audio', 'type_of_docking', 'multiroom', 'no.speakerboxes', 'output_channel', 'streaming_connection', 'streaming_technology', 'usb_connection', 'wattage_total']


        self.sanitize_df()

    def set_column_names(self):
        self.data.columns = self.setColumnNames

    def set_column_names_to_lower(self):
        self.data.columns = self.data.columns.str.replace(' ', '_').str.replace('.','').str.lower()

    def set_column_period_to_ordered_months(self):
        self.data.period.astype('category',categories=self.monthsOrdered)

    def set_to_numeric_for_these_columns(self,cols):
        for col in cols:
            self.data[col] = self.data[col].str.replace(',','').str.lower()
            self.data[col] = self.data[col].apply(pd.to_numeric, errors='coerce')

    def set_to_numeric_for_mesurement_columns(self,cols):
        for col in cols:
            self.data[col] = self.data[col].apply(pd.to_numeric, errors='coerce')

    def sort_values_based_on_these_columns(self,cols):
        self.data = self.data.sort_values(cols,inplace=False)

    def sanitize_df(self):
        self.set_column_names()
        self.set_column_names_to_lower()
        self.set_column_period_to_ordered_months()
        self.set_to_numeric_for_these_columns(self.columnsThatNeedATypeChange)
        self.set_to_numeric_for_mesurement_columns(self.measurements_columns)
        self.sort_values_based_on_these_columns(self.columnsToBeSortedBy)

    def get_df(self):
        return self.data

    def get_pivot_and_stats_data_tables(self):
        pivot_data_table = self.data.filter(items=self.pivot_key_columns)
        data_stats_table = self.data.filter(items=self.key_stats_columns)
        pivot_data_table = self.sort_df_values_based_on_these_columns(pivot_data_table,self.pivotcolumnsToBeSortedBy)
        data_stats_table = self.sort_df_values_based_on_these_columns(data_stats_table,self.columnsToBeSortedBy)
        return pivot_data_table,data_stats_table

    def sort_df_values_based_on_these_columns(self,df,cols):
        df = df.sort_values(cols,inplace=False)
        return df


class determineStats():
    def __init__(self):
        self.sales_columns =  ['sales_units', 'sales_value_eur', 'price_eur', 'sales_value_usd', 'price_usd']
        self.measurements_columns = ['height_in_mm','width_in_mm']
        self.pivot_key_columns = ['id','height_in_mm', 'width_in_mm','brand', 'model','smart_connect', 'smart_h._ecosys', 'operating_ai', 'airplay', 'googlecast/home', 'bluetooth', 'ethernet', 'wifi', 'high-res_audio', 'type_of_docking', 'multiroom', 'no.speakerboxes', 'output_channel', 'streaming_connection', 'streaming_technology', 'usb_connection', 'wattage_total']
        pass

    def give_describe_characterisitcs(self,firstRow,df,col):
        self.sumCol = col + '_sum'
        self.sumCol = col + '_sum'
        self.meanCol = col + '_mean'
        self.medianCol = col + '_median'
        self.modeCol = col + '_mode'
        self.stdCol = col + '_std'
        self.minCol = col + '_min'
        self.maxCol = col + '_max'
        firstRow[self.sumCol]  = df[col].sum()
        firstRow[self.meanCol] = df[col].mean()
        firstRow[self.medianCol] = df[col].median()
        firstRow[self.modeCol] = df[col].mode()
        firstRow[self.stdCol] = df[col].std()
        firstRow[self.minCol] = df[col].min()
        firstRow[self.maxCol] = df[col].max()
        return firstRow

    def get_describe_characterisitcs_for_sales_columns(self,firstRow,df,cols):
        for col in cols:
            self.sumCol = col + '_sum'
            self.sumCol = col + '_sum'
            self.meanCol = col + '_mean'
            self.medianCol = col + '_median'
            self.modeCol = col + '_mode'
            self.stdCol = col + '_std'
            self.minCol = col + '_min'
            self.maxCol = col + '_max'
            firstRow[self.sumCol]  = df[col].sum()
            firstRow[self.meanCol] = df[col].mean()
            firstRow[self.medianCol] = df[col].median()
            firstRow[self.modeCol] = df[col].mode()
            firstRow[self.stdCol] = df[col].std()
            firstRow[self.minCol] = df[col].min()
            firstRow[self.maxCol] = df[col].max()
#             firstRow = self.give_describe_characterisitcs(self,firstRow,df,col)
        return firstRow

    def get_describe_characterisitcs_for_measurement_columns(self,firstRow,df,cols):
        dfWithOutHeightAndWidth = df.dropna(subset=['height_in_mm','width_in_mm'],how='any')
        for col in cols:
            self.sumCol = col + '_sum'
            self.sumCol = col + '_sum'
            self.meanCol = col + '_mean'
            self.medianCol = col + '_median'
            self.modeCol = col + '_mode'
            self.stdCol = col + '_std'
            self.minCol = col + '_min'
            self.maxCol = col + '_max'
            firstRow[self.sumCol]  = df[col].sum()
            firstRow[self.meanCol] = df[col].mean()
            firstRow[self.medianCol] = df[col].median()
            firstRow[self.modeCol] = df[col].mode()
            firstRow[self.stdCol] = df[col].std()
            firstRow[self.minCol] = df[col].min()
            firstRow[self.maxCol] = df[col].max()
#             firstRow = self.give_describe_characterisitcs(firstRow,dfWithOutHeightAndWidth,col)
        return firstRow

    def sort_by_id(self,df):
        i = 1
        dfBasedOnID = pd.DataFrame()
        for uid in unique_ids:
            allDataBasedOnThisID = df[df['id']==uid]
            try:
                firstRow = allDataBasedOnThisID.iloc[0]
                try:
                    firstRow = self.get_describe_characterisitcs_for_sales_columns(firstRow,allDataBasedOnThisID,self.sales_columns)
                    try:
                        dfBasedOnID = dfBasedOnID.append(firstRow, ignore_index=True)
                    except:
                        print('cant append to dataframe')
                except:
                    print('cant return first row for sales columns')
            except:
                print('no Id')
            i += 1
            print(i)
            if(i > 20):
                break

        return dfBasedOnID

    def sort_by_id_only(self,df):
        i = 1
        dfBasedOnID = pd.DataFrame(columns=self.pivot_key_columns)
        print(unique_ids)
        for uid in unique_ids:
            allDataBasedOnThisID = df[df['id']==uid]
            try:
                firstRow = allDataBasedOnThisID.iloc[0]
                try:
                    dfBasedOnID = dfBasedOnID.append(firstRow, ignore_index=True)
                except:
                    print('cant append to dataframe')
            except:
                print('no Id')
            i += 1
            if(i > 20):
                break

        return dfBasedOnID

    def get_dimensions(self,df):
        i = 1
        newDf = pd.DataFrame(columns=self.pivot_key_columns)
        try:
            firstRow = df.iloc[0]
            try:
                for col in self.measurements_columns:
                    self.sumCol = col + '_sum'
                    self.sumCol = col + '_sum'
                    self.meanCol = col + '_mean'
                    self.medianCol = col + '_median'
                    self.modeCol = col + '_mode'
                    self.stdCol = col + '_std'
                    self.minCol = col + '_min'
                    self.maxCol = col + '_max'
                    firstRow[self.sumCol]  = df[col].sum()
                    firstRow[self.meanCol] = df[col].mean()
                    firstRow[self.medianCol] = df[col].median()
                    firstRow[self.modeCol] = df[col].mode()
                    firstRow[self.stdCol] = df[col].std()
                    firstRow[self.minCol] = df[col].min()
                    firstRow[self.maxCol] = df[col].max()
                try:
                    return firstRow
                except:
                    print('cant append to dataframe')
            except:
                print('cant for loop')
        except:
            print('cant get df location 0')

        return dfBasedOnID


def find_unique_of_this_column(data,col):
    return data[col].unique()




def read_csv():
    data = pd.read_csv(address,low_memory=False)
    return data


def main():
    '''
    send list of files to be parsed
    '''
    global unique_ids
    global data
    global pivot_data_table, data_stats_table
    data = read_csv()
    santizeData = sanitizeDataFrame(data)
    data = santizeData.get_df()
    pivot_data_table,data_stats_table = santizeData.get_pivot_and_stats_data_tables()
    unique_ids = find_unique_of_this_column(data,'id')


#     create_specific_dfs(data)

if __name__ == "__main__": main()


print(data_stats_table)

def create_specific_dfs(data):
    pass

statsObj = determineStats()
print(pivot_data_table.columns)

obj2 = statsObj.sort_by_id_only(pivot_data_table)
obj2

obj3 = statsObj.get_dimensions(obj2)
obj3

obj = statsObj.sort_by_id(data_stats_table)
obj

data.columns

columnsLower = ['country', 'channel', 'year',
       'smart_connect', 'smart_h._ecosys', 'operating_ai', 'airplay',
       'googlecast/home', 'bluetooth', 'ethernet', 'wifi', 'high-res_audio', 'type_of_docking', 'multiroom',
       'no.speakerboxes', 'output_channel', 'streaming_connection',
       'streaming_technology', 'usb_connection', 'wattage_total']

country_options = ['Germany' 'Great Britain']

for col in columnsLower:
    print(col)
    print(data[col].unique())

columnsLower = [
       'smart_connect', 'smart_h._ecosys', 'operating_ai', 'airplay',
       'googlecast/home', 'bluetooth', 'ethernet', 'wifi', 'height_in_mm',
       'width_in_mm', 'high-res_audio', 'type_of_docking', 'multiroom',
       'no.speakerboxes', 'output_channel', 'streaming_connection',
       'streaming_technology', 'usb_connection', 'wattage_total',
       'sales_units', 'sales_value_eur', 'price_eur', 'sales_value_usd',
       'price_usd']

columnsLower = ['country', 'channel', 'brand', 'model', 'id', 'year', 'period',
       'smart_connect', 'smart_h._ecosys', 'operating_ai', 'airplay',
       'googlecast/home', 'bluetooth', 'ethernet', 'wifi', 'height_in_mm',
       'width_in_mm', 'high-res_audio', 'type_of_docking', 'multiroom',
       'no.speakerboxes', 'output_channel', 'streaming_connection',
       'streaming_technology', 'usb_connection', 'wattage_total',
       'sales_units', 'sales_value_eur', 'price_eur', 'sales_value_usd',
       'price_usd']

data.columns

len(unique_ids)

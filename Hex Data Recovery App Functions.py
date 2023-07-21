import re
import pandas
from datetime import datetime


Standard_Direction_list = ['NE','SE','NW','SW','A','N','S','E','W'] # A list of possible directions or orientations of sensors
All_height_values = list(range(-1,120,1))[::-1]

Grand_Dict = {}

def compile_sensors(df):

        Sensor_List = []
        sensor_column = 'sensor_name'

        class Sensor:
            def __init__(self, abbrev, measurement_name,unit, possible_heights, possible_directions):
                self.measurement_name = measurement_name
                self.abbrev = abbrev

                self.unit = unit
                self.possible_heights = possible_heights
                self.possible_directions = possible_directions
                self.pretty_label = ''
                self.data_frame = pandas.DataFrame()
                self.tower = ''
                self.full_name = ''
                self.sensor_id = 0000
            def add_to_list(self,list):
                    list.append(self)
                    # print('Added!')

            def add_to_Dict(self,keyname,dict):
                dict[keyname] = self
            
            def remove_flags(self, df, datesdf):
                        datesdf = datesdf[datesdf['sensor_id'] == self.sensor_id] #filter to only flags of relevant sensor

                        # print(datesdf['sensor_id'] == me.sensor_id)

                        # print(Grand_Dict['CO_ACW6009_WD30mA'].sensor_id)


                        # print('max date: ', df.index.max(), " min date:", df.index.min())
                        # print(len(df["6/2021":"7/2022"]))
                        # print(len(datesdf))
                        # print("before len: ", len(df))
                        max = df.index.max() #recent 
                        min = df.index.min() #farthest away
                        # seg_lens = [] 
                        for index, row in datesdf.iterrows(): #for every flag
                            # print(row['startdate'], ' and ', row['enddate'])
                            # print('segement length:' , len(df[row['startdate']:row['enddate']]))
                            # seg_lens.append(len(df[str(row['startdate']):str(row['enddate'])]))
                            # df.drop(df[str(row['startdate']):str(row['enddate'])].index, inplace = True) #method 1
                            if row['enddate'] < min or row['startdate'] > max:
                                pass
                            else:
                                df = df[~df.index.isin(df[str(row['startdate']):str(row['enddate'])].index)]
                        
                        print("after drop len: ", len(df))
                        return df

            def assign_dataframe(self,df,datesdf):
                
                
                headers = df.columns.values.tolist()
                tseries = df['timestamp']
                temp = pandas.DataFrame( )

                # temp['timestamp'] = to_alter['timestamp']
                # temp.index = temp['timestamp'] 
                for col_name in headers:
                    if col_name.startswith(self.full_name):
                        temp[col_name] = df[col_name]
                        temp = temp.set_index([tseries])
                temp = self.remove_flags(temp,datesdf)
                ind = pandas.date_range(start=temp.index.min(), end= temp.index.max(), name = 'timestamp', freq = '10min')
                # print(ind)                    
                temp = temp.reindex(index = ind)
                self.data_frame = temp
                self.data_frame.sort_values(by='timestamp', ascending= False, inplace= True)


        class Anemometer(Sensor):
            def __init__(self,sensor_name, height,dir):
                super().__init__('WS', 'Wind Speed','m/s', All_height_values, Standard_Direction_list) # These variables can be initialized statically since all Anomometers will share these features

                self.sensor_name = sensor_name
                self.height = height
                self.dir = dir
                # self.full_name = tower+'_'+self.sensor_name

            def clone(self,sensor_name, height,dir):
                return Anemometer(sensor_name, height,dir)

        

        class Wind_Vane(Sensor):
            def __init__(self,sensor_name, height):                              # o dir needed, these always have an A for direction
                super().__init__('WD', 'Wind Direction','Degrees', All_height_values, ['A']) # These variables can be initialized statically since all Wind Vanes will share these features

                self.sensor_name = sensor_name
                self.height = height
                
                self.dir = 'A'
                # self.full_name = tower+'_'+self.sensor_name

            def clone(self,sensor_name, height):
                return Wind_Vane(sensor_name, height)

                

        class Thermometer(Sensor):
            def __init__(self,sensor_name, height):                              # o dir needed, these always have an A for direction
                
                super().__init__("TEMP", 'Temperature','Degrees', All_height_values, ['A']) # These variables can be initialized statically since all Thermometers will share these features

                self.sensor_name = sensor_name
                self.height = height
                
                self.dir = 'A'
                # self.full_name = tower+'_'+self.sensor_name

            def clone(self,sensor_name, height):
                    return Thermometer(sensor_name, height)

        class Barometer(Sensor):
            def __init__(self,sensor_name, height):                              # o dir needed, these always have an A for direction
                
                super().__init__("BP", 'Barometric Pressure','Hg?', All_height_values, ['A']) # These variables can be initialized statically since all Barometers will share these features

                self.sensor_name = sensor_name
                self.height = height
                self.dir = 'A'
                # self.full_name = tower+'_'+self.sensor_name

            def clone(self,sensor_name, height):
                    return Barometer(sensor_name, height)


        class Hygrometer(Sensor):
            def __init__(self,sensor_name, height):                              # o dir needed, these always have an A for direction
                
                super().__init__("RH", 'Relative Humidity','Percent?', All_height_values, ['A']) # These variables can be initialized statically since all Hygrometers will share these features

                self.sensor_name = sensor_name
                self.height = height
                self.dir = 'A'
                # self.full_name = tower+'_'+self.sensor_name

            def clone(self,sensor_name, height):
                    return Hygrometer(sensor_name, height)

        class Voltometer(Sensor):
            def __init__(self,sensor_name, height):                              # o dir needed, these always have an A for direction
                
                super().__init__("VOLT", 'Voltage','Volts', All_height_values, ['A']) # These variables can be initialized statically since all Voltometers will share these features

                self.sensor_name = sensor_name
                self.height = height
                self.dir = 'A'
                # self.full_name = tower+'_'+self.sensor_name

            def clone(self,sensor_name, height):
                    return Voltometer(sensor_name, height)

        class Mystery(Sensor):
            def __init__(self,sensor_name, height, dir,abbrev, measurement_name,unit, possible_heights, possible_directions):                              # o dir needed, these always have an A for direction
                super().__init__( abbrev, measurement_name,unit, possible_heights, possible_directions) # These variables can be initialized statically since all Voltometers will share these features

                self.sensor_name = sensor_name
                self.height = height
                self.dir = dir
                # self.full_name = tower+'_'+self.sensor_name

            


        Dummy_Anemometer = Anemometer('Dummy_1',0,'A')                    # Have to initialize and add all sensor types once so they can be used for reference later.

        Dummy_Anemometer.add_to_list(Sensor_List)

        Dummy_Wind_Vane = Wind_Vane('Dummy_1',0)
        Dummy_Wind_Vane.add_to_list(Sensor_List)

        Dummy_Thermometer = Thermometer('Dummy_1',0)
        Dummy_Thermometer.add_to_list(Sensor_List)

        Dummy_Barometer = Barometer('Dummy_1',0)
        Dummy_Barometer.add_to_list(Sensor_List)

        Dummy_Hygrometer = Hygrometer('Dummy_1',0)
        Dummy_Hygrometer.add_to_list(Sensor_List)

        Dummy_Voltometer = Voltometer('Dummy_1',0)
        Dummy_Voltometer.add_to_list(Sensor_List)


        counter = 0

        # Sensor_Labels = [''] * len(df[sensor_column])                # create empty label list for pretty labels later |||||||| dont need anymore?


        for index,row in df.iterrows():  # iterates through every sensor in the tower
            sensor1 = row[sensor_column]
            tower = row['da_name']

            found = False                   # using this vairiable to keep track of if the asset is found among sensors we have defined
            for sensor2 in Sensor_List:          # for every sensor, checks if the name is in the sensor list we made
                if sensor1.startswith(sensor2.abbrev): # if the sensor is in the list
                    temp_measurement_name = sensor2.measurement_name
                    temp_unit = sensor2.unit

                    dir_found = False                                          # using this to keep track of if direction is found
                    for direction in sensor2.possible_directions:
                        if re.search(direction,sensor1,3) == None :          #Scans original tower name for direction, if direction is found, stores it
                            pass
                        else:
                            temp_dir = direction
                            dir_found= True
                            break
                    if dir_found == False:                                                # if no direction was found
                        print('No Direction was found for ',sensor1,' Perhaps the Standard Direction List needs to be appended?')
                        temp_dir = ''


                    height_found = False                                        #using to keep track of if height is found
                    for height in sensor2.possible_heights:
                        if re.search(str(height),sensor1) == None :          #Scans original tower measurement_name for string version of height, if height is found, stores it
                            pass
                        else:
                            temp_h = height
                            height_found = True
                            break
                        #??????????????SCRAPPING THIS FOR NOW????????????????????????????????????????
                    # if height_found == False:
                    #                                                             # if no height was matched
                    #     for h in All_height_values:                                              # look from 1- 110 meters for a height
                    #         if re.search(str(h),sensor1) == None:                 # If new, previously unknown height is found
                    #             pass
                    #         else:
                    #             temp_h = str(h)
                    #             height_found = True
                    #             sensor2.add_height(h)
                    #             print("A new height of ",str(h)," was found for ",sensor2.measurement_name," and has been added to the list of possible heights. Manually update the list after executing this code to keep this new height for the future")
                    #             break
                    #??????????????????????????????????????????????????????????????????
                    if height_found == False:                        # If still no height is matched
                        print(' Hmmmmm, Thats Strange... No Height was found for ',sensor1)
                        temp_h = ''

                    #At this point all temp variables should be found, now to store everything
                    temp_pretty_name = 'tower '+ tower + " "+ temp_measurement_name +' at [' + str(temp_h) +'m] '+ temp_dir + ' in '+ temp_unit
                    
                    if temp_dir != 'A' and temp_dir != '':  #This means the object needs a dir parameter

                        # print(sensor2.abbrev,' so cloning a (guess), also direction is: ' , temp_dir,sensor1, temp_h )
                        dict_val = sensor2.clone(sensor1,temp_h, temp_dir)
                        dict_val.pretty_label = temp_pretty_name
                    else:
                        dict_val = sensor2.clone(sensor1,temp_h)
                        dict_val.pretty_label = temp_pretty_name

                    dict_val.sensor_id = row['sensor_id']
                    dict_val.tower = tower
                    dict_val.full_name = dict_val.tower+'_'+dict_val.sensor_name


                    counter += 1
                    # dictname = tower+'_'+sensor1

                    Grand_Dict[dict_val.full_name] = dict_val
                    # print(sensor1, ', ', counter,' in order, found')
                    
                    found = True
                    break                                                        # If the sensor is found, the for loop is broken which stops iteration through sensor list and moves on to the next tower sensor
                else:
                    pass
                    # print(sensor1,'is not a ',sensor2.measurement_name)
                    
            if found:
                pass    
            else:
                print('Sensor ', sensor1,' , ',counter +1,' in order, was not found')  #If this code is reached it means the sensor was not in sensor list

                
                splitted_unknown = re.split('[0-9]+', sensor1)
                temp_abbrev  = splitted_unknown[0]
                temp_measurement_name = ('tower '+tower+' Unknown '+ sensor1)
                temp_unit =  'Unknown units'
                

                if len(splitted_unknown)>1:
                    temp_dir = splitted_unknown[1][1::1] +''
                else:
                    temp_dir = ''


                height_found = False
                for height in All_height_values:
                    if re.search(str(height),sensor1) == None :          #Scans original tower measurement_name for string version of height, if height is found, stores it
                        pass
                    else:
                        temp_h = str(height)
                        height_found = True
                        break
                if height_found == False:
                                                                            # if no height was matched
                    for h in All_height_values:                                              # look from 1- 110 meters for a height
                        if re.search(str(h),sensor1) == None:                 # If new, previously unknown height is found
                            pass
                        else:
                            temp_h = h
                            height_found = True
                            sensor2.add_height(h)
                            print("A new height of ",str(h)," was found for ",sensor2.measurement_name," and has been added to the list of possible heights. Manually update the list after executing this code to keep this new height for the future")
                            break
                if height_found == False:                        # If still no height is matched
                    print(' Hmmmmm, Thats Strange... No Height was found for ',sensor1)
                    temp_h = ''

                temp_pretty_name = 'tower '+ tower+ ' '+ temp_measurement_name +' at [' + str(temp_h) +'m] '+ temp_dir + ' in '+ temp_unit
                
                stranger = Mystery(sensor1,temp_h,temp_dir,temp_abbrev,temp_measurement_name, temp_unit, All_height_values, Standard_Direction_list)

                dict_val = stranger
                dict_val.pretty_label = temp_pretty_name
                dict_val.sensor_id = row['sensor_id']
                dict_val.tower = tower
                dict_val.full_name = dict_val.tower+'_'+dict_val.sensor_name


                
                # dictname = tower+'_'+sensor1
                Grand_Dict[dict_val.full_name] = dict_val

                print('Sensor ', counter + 1,' not found but added under ', stranger.measurement_name)

                counter += 1
            
        
        # print('Done! (check Grand Dictionary)')
        return Grand_Dict

def extract_from_sensor(dictionary,metric):                     # Will return a list with the value of each item
    temp_list = [None] * len(dictionary)
    # print(len(temp_list))
    counter = 0
    for key in dictionary:                                # for every item of the dictionary
        try:
         dictionary[key].abbrev
        except:
            print("An exception occurred, are you using this function on Grand_Dict")

        else:
            for attr, value in dictionary[key].__dict__.items():         # iterates through every attribute and the corresonding value stored in that attribute for the object
                # print(attr)
                # print(temp_list[counter] )
                if  attr == metric:                                         # if the attribute is the metric we are looking for, store it and breake the loop
                   temp_list[counter] = value
                   counter += 1
                   break
    return temp_list

def get_recovery3(Dict,sensor, column,span):
    
    # print(ind)

    # print('first:',df[column].isna().astype(int).sum(),'len:',len(df))
    # print(df[column].isna().astype(int).sum())
    if len(Dict[sensor].data_frame) == 0:
        print(sensor,' has no dataframe')
    else:
        def thumper(x):
                # print(x)
                conversion_factor = 6 * 24 
                # return x.sum()
                return (100 - (((x/ (span * conversion_factor))*100)))

        bools = column + "_nan_bools"
        Dict[sensor].data_frame.sort_values(by = 'timestamp' , ascending = False)
        Dict[sensor].data_frame[bools] = Dict[sensor].data_frame[column].isna().astype(int)
        rolling = str(span)+' rolling'
        # Flipping here to bypass weird aggregation rules

        tmp = Dict[sensor].data_frame[bools].reindex(index=Dict[sensor].data_frame.index[::-1])

        tmp = tmp.rolling(f"{span}D").sum()
        tmp = tmp.reindex(index=Dict[sensor].data_frame.index)

        Dict[sensor].data_frame[rolling] =  tmp
        # print( Dict[sensor].data_frame[roll])
        finished = thumper(Dict[sensor].data_frame[rolling])
        # print('next: one:',df[recovery_name].sum(),'len:',len(df[recovery_name]))

        recovery_name = f"_{span}_day_recovery"
        recovery_name_full = sensor+recovery_name

        Dict[sensor].data_frame[recovery_name_full]= finished
        Dict[sensor].data_frame.drop(labels = bools, axis = 1,inplace= True)
        Dict[sensor].data_frame.drop(labels = rolling, axis = 1,inplace= True)

def fetch_recoveries3(Dict, sensor_list, range_list):
    for sensor in sensor_list:
        for range in range_list:
            get_recovery3(Dict, sensor, sensor,range)

def final_extraction(Dict,sensors):
    all_Sensor_dfs = []
    unfiltered = extract_from_sensor(Grand_Dict,'data_frame')
    for sensor_df in unfiltered: 
        if len(sensor_df) == 0:
            pass
        else:
            all_Sensor_dfs.append(sensor_df)
    final_df = pandas.DataFrame(index = all_Sensor_dfs[0].index)
    for i in range(0, len(sensors)):
        final_df = final_df.merge(Dict[sensors[i]].data_frame, how ='outer',right_index= True,left_on='timestamp')
        # print('processing...')
    return final_df


    
def final_recovery_extraction(dataframe):
    cols  = list(dataframe.columns)
    for col in cols:
        if not(col.endswith('recovery')):
            dataframe.drop(col,inplace = True, axis =1)
    return dataframe
        # print('processing...')

def construct_dash_dash(Dict, dash_inputs, exclude, data_recovery_span,indexi, recovery_clause):
    dict_keys = extract_from_sensor(Grand_Dict,'full_name')
    dash  = pandas.DataFrame()
    startdate = ''
    for span in data_recovery_span:
        recovery_name  = f"_{span}_day_recovery"
        temp_2 = []
        for sensor in dict_keys:
            if len(Dict[sensor].data_frame) == 0:
                temp_2.append(float("Nan"))
            else:
                column = sensor + recovery_name
                # temp_2.append(str( Dict[sensor].data_frame.index[0] )[:10:] +": " + str((Dict[sensor].data_frame[column][0:144].sum()/144)))
                temp_2.append((Dict[sensor].data_frame[column][0:144].sum()/144))
                startdate = str(Dict[sensor].data_frame.index[0] )[:10:]

        dash[recovery_name] = temp_2

    for metric in dash_inputs:
        dash[metric] = extract_from_sensor(Grand_Dict,metric)

    cols = list(dash)
    filtered = filter(lambda col: col not in indexi , cols)


    ind  = pandas.MultiIndex.from_frame(dash[indexi]) # , names=['Tower', 'Sensor', 'Direction']

    # dash = dash.pivot(index= index, columns = columns)

    dash.index =ind

    dash = dash[filtered]

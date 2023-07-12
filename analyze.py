import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


# Process

# 1.) download data and store in repository under folder system

# 2.) process data into dataframs using panads read_csv/read_excel , variables are stations or locations

# 3.) after creating dataframes, standardize data variable headers, and datetime format, add location variable to dataframes for later sorting among differnt datasets

# 4.) Aggregate multiple datasets based on shared column, typically datetime column, fill with NameError

# 5.) Analyze super dataset using variables of interest.




# Step 1


#Fluxnet
fluxnet_ch4_file_locations = ["Fluxnet\FLX_FI-Hyy_FLUXNET-CH4_2016-2016_1-1","Fluxnet\FLX_FI-Lom_FLUXNET-CH4_2006-2010_1-1","Fluxnet\FLX_FI-Si2_FLUXNET-CH4_2012-2016_1-1",
                                "Fluxnet\FLX_FI-Sii_FLUXNET-CH4_2013-2018_1-1", "Fluxnet\FLX_RU-Che_FLUXNET-CH4_2014-2016_1-1", "Fluxnet\FLX_RU-Cok_FLUXNET-CH4_2008-2016_1-1",
                                "Fluxnet\FLX_RU-Vrk_FLUXNET-CH4_2008-2008_1-1","Fluxnet\FLX_SE-Deg_FLUXNET-CH4_2014-2018_1-1","Fluxnet\FLX_SE-St1_FLUXNET-CH4_2012-2014_1-1",
                                "Fluxnet\FLX_US-Atq_FLUXNET-CH4_2013-2016_1-1","Fluxnet\FLX_US-Ivo_FLUXNET-CH4_2013-2016_1-1"




                            ]


def flux_prep(filename,H_or_D):
    index_insert = filename.find("CH4_")
    start_point = filename.find("Fluxnet\\")
    # print(index_insert)
    if H_or_D == "H":
        return(filename + "\\" + filename[(start_point+ 8):(index_insert+4)] + "HH_"+ filename[(index_insert+4):]+".csv")
    else:
        return(filename + "\\" + filename[(start_point+ 8):(index_insert+4)] + "HH_"+ filename[(index_insert+4):]+".csv")


class Fluxnet_db:
    def __init__(self):
        self.HH_df = {}
        self.DD_df = {}
        self.aggregate = pd.DataFrame()
        self.stations = []
    def add_to_list(self,list):
                    list.append(self)
                    # print('Added!')

    def add_to_Dict(self,keyname,dict):
                dict[keyname] = self

    def apply_across_all_dfs(self,function):
        # temp = self.HH_df
        for attr, value in self.HH_df.items():
            self.HH_df[attr] = function(value)
        for attr, value in self.DD_df.items():
            self.DD_df[attr] = function(value)


    def imprint_identity_across_all_dfs(self):
        for attr, value in self.HH_df.items():
            station = attr
            columns ={}
            

            # print(value.columns)
            # print(list(value.columns))


            for header in (list(value.columns))[2:]:
                columns[header] = station+ '_'+header
            self.HH_df[attr] = value.rename(columns=columns)

        for attr, value in self.DD_df.items():
            station = attr
            columns ={}

            # print(value.columns)
            # print(list(value.columns))


            for header in (list(value.columns))[2:]:
                columns[header] = station+ '_'+header
            self.DD_df[attr] = value.rename(columns=columns)

    # def final_extraction(dataframe):
    #     all_Sensor_dfs = []
    #     unfiltered = extract_from_sensor(Grand_Dict,'data_frame')
    #     for sensor_df in unfiltered: 
    #         if len(sensor_df) == 0:
    #             pass
    #         else:
    #             all_Sensor_dfs.append(sensor_df)
    #     final_df = pandas.DataFrame(index = all_Sensor_dfs[0].index)
    #     for i in range(0, len(sensors)):
    #         final_df = final_df.merge(Dict[sensors[i]].data_frame, how ='outer',right_index= True,left_on='timestamp')
    #         # print('processing...')
    #     return final_df


    def flux_aggregate():
        for attr, value in self.HH_df.items():
            # value = df.drop((list(value.columns))[0], axis='columns')
            if counter == 0:
                self.aggregate = value
            else:
                self.aggregate = self.aggregate.merge(value, how= "outer", right_index = True, on = self.aggregate.index ) 
            
# need full outer join!!



            # print(value.columns)
            # print(list(value.columns))


        for header in (list(value.columns))[2:]:
                columns[header] = station+ '_'+header
        self.HH_df[attr] = value.rename(columns=columns)



# gfg_csv_data = df.to_csv('GfG.csv', index = True)
    def save_dfs_as_csv(self):
        for attr, value in self.HH_df.items():
            value.to_csv((attr+"_HH.csv"))
        for attr, value in self.DD_df.items():
            value.to_csv((attr+"_DD.csv"))



                

Fluxnet_db = Fluxnet_db()
def fluxnet_dataframe_gather(location_list): 
    for station in location_list:
        # start_point = station.find("Fluxnet\\")
        station_name = station[12:18]
        Fluxnet_db.stations.append(station_name)
        print(station_name+ " read into df")
        Fluxnet_db.HH_df[station_name] = pd.read_csv(flux_prep(station,"H"))
        # Fluxnet_db.HH_df[station_name]["Station"] = station_name

        Fluxnet_db.DD_df[station_name] = pd.read_csv(flux_prep(station,"D"))
        # Fluxnet_db.DD_df[station_name]["Station"] = station_name
#Step 2

#Fluxnet

fluxnet_dataframe_gather(fluxnet_ch4_file_locations)

# Fluxnet_db.HH_df["FI-Hyy"].info()

#STEP 3

# Universal process: data, headers, header_dict
#in, data with misc header variables
#out, data with standardized header variables

header_dict = {"TIMESTAMP_START" : "TIMESTAMP_START",
               "TIMESTAMP_END" : "TIMESTAMP_END",
               "SW_IN" : "shortwave_in",
               "SW_OUT" : "shortwave_out",
               "LW_IN" : "longwave_in",
               "LW_OUT" : "longwave_out",
               "PPFD_IN" : "photosynthetic_photon_flux_density_in",
               "PPFD_OUT" : "photosynthetic_photon_flux_density_out",
               "NETRAD" : "net_radiation",
               "USTAR" : "friction_velo",
               "WD" : "wind_dir",
               "WS" : "wind_spd",
               "LE" : "latent_heat_turbulent_flux",
               "PA" : "atmos_pressure",
               "TA" : "air_temp",
               "VPD" : "vapor_pressure_deficit",
               "RH" : "relative_humidity",
               "NEE" : "net_ecosystem_exchange",
               "GPP" : "gross_primary_productivity",
               "RECO" : "ecosystem_respiration",
               "FCH4" : "ch4_turbulent_flux",
               "TS" : "soil_temp",
               "WTD" : "water_table_depth",
               "SWC" : "soil_water_content",
               "G" : "soil_heat_flux",
               "H" : "sensible_heat_turbulent_flux",
               "P" : "precipitation",
                }




            # def assign_dataframe(self,df,datesdf):
                
                
            #     headers = df.columns.values.tolist()
            #     tseries = df['timestamp']
            #     temp = pandas.DataFrame( )

            #     # temp['timestamp'] = to_alter['timestamp']
            #     # temp.index = temp['timestamp'] 
            #     for col_name in headers:
            #         if col_name.startswith(self.full_name):
            #             temp[col_name] = df[col_name]
            #             temp = temp.set_index([tseries])
            #     temp = self.remove_flags(temp,datesdf)
            #     ind = pandas.date_range(start=temp.index.min(), end= temp.index.max(), name = 'timestamp', freq = '10min')
            #     # print(ind)                    
            #     temp = temp.reindex(index = ind)
            #     self.data_frame = temp
            #     self.data_frame.sort_values(by='timestamp', ascending= False, inplace= True)


def iterative_flux_search_rename( header, key, running_appendage, key_start):
            if header == key:
                return (header_dict[key_start] +running_appendage)
            elif header == (key +"_F") or (key +"_F") in header :
                return iterative_flux_search_rename(header,(key +"_F"),running_appendage+"+_gap_filled",key_start)


            elif header == (key +"_DT") or (key +"_DT") in header :
                return iterative_flux_search_rename(header,(key +"_DT"),running_appendage+"+_lr_curve_estimate",key_start)


            elif header == (key +"_NT") or (key +"_NT") in header :
                return iterative_flux_search_rename(header,(key +"_NT"),running_appendage+"+_night_data_estimate",key_start)

            elif header == (key +"_RANDUNC") or (key +"_RANDUNC") in header :
                return iterative_flux_search_rename(header,(key +"_RANDUNC"),running_appendage+"+_random_uncertainty",key_start)

            elif header == (key +"_ANNOPTLM") or (key +"_ANNOPTLM") in header :
                return iterative_flux_search_rename(header,(key +"_ANNOPTLM"),running_appendage+"+_gap_filled_using_ai",key_start)

            elif header == (key +"_UNC") or (key +"_UNC") in header :
                return iterative_flux_search_rename(header,(key +"_UNC"),running_appendage+"+_uncertainty_from_ai_gap_fill",key_start)

            elif header == (key +"_QC") or (key +"_QC") in header :
                return iterative_flux_search_rename(header,(key +"_QC"),running_appendage+"+_quality_check_based_on_gap_filled_data",key_start)
            else:
                return None


# data.rename(columns={'gdp':'log(gdp)'}, inplace=True)

def flux_format(dataframe):
    headers  = list(dataframe.columns)
    # delinquents = list()
    for header in headers:
        for key in header_dict:
            if header.startswith(key):              #some similarity 
                if header == key:
                    # dataframe.columns.str.replace(header,header_dict[key])
                    dataframe.rename(columns={header:header_dict[key]}, inplace=True)
                    print(header+ " replaced successfully")
                    # print(dataframe[header_dict[key]])
                    break
                modded_name = iterative_flux_search_rename( header, key, "", key)
                if modded_name != None:
                    dataframe.rename(columns={header:modded_name}, inplace=True)
                    break
                print(header+" was not found in header dictionary" +" even though "+ key + " seems familiar...")
                dataframe.rename(columns={header:header.replace(key,header_dict[key])}, inplace=True)

            # print(header+" was not found in header dictionary")
    return dataframe

# flux_format(fi_hyy_data)
# fi_hyy_data.info() # Show the information about the DataFrame

# print(iterative_flux_search_rename( "FCH4_F_ANNOPTLM_QC", "FCH4", "", "FCH4"))

# Fluxnet_db.apply_across_all_dfs(flux_format)

# Fluxnet_db.HH_df["FI-Hyy"].info()

        # tmp = tmp.reindex(index=Dict[sensor].data_frame.index)
# df['date'] = pd.to_datetime(df['date'],unit='s')
# datetime.fromisoformat(isoformat_date)

def format_time_and_set_time_index(dataframe):
    def int_to_dt(inty):
        str_ver = str(inty)
        if len(str_ver) == 12:
            str_ver = str_ver[:4] + "-" + str_ver[4:6]+ "-" +str_ver[6:8] + "T"+str_ver[8:10]+":"+ str_ver[10:]
        if len(str_ver) == 8:
            str_ver = str_ver[:4] + "-" + str_ver[4:6]+ "-" +str_ver[6:] 
        return(str_ver)
        

    dataframe[list(dataframe.columns)[0]] = dataframe[list(dataframe.columns)[0]].apply(int_to_dt)
    dataframe[list(dataframe.columns)[0]] = dataframe[list(dataframe.columns)[0]].apply(datetime.fromisoformat)

    # dataframe[list(dataframe.columns)[0]] = datetime.fromisoformat(    dataframe[list(dataframe.columns)[0]]  )



    dataframe[list(dataframe.columns)[1]] = dataframe[list(dataframe.columns)[1]].apply(int_to_dt)
    dataframe[list(dataframe.columns)[1]] = dataframe[list(dataframe.columns)[1]].apply(datetime.fromisoformat)
    # dataframe = dataframe.reindex(index= dataframe[list(dataframe.columns)[0]])
    dataframe = dataframe.set_index(list(dataframe.columns)[0])


    return dataframe

    # dataframe[list(dataframe.columns)[1]] = datetime.fromisoformat(    dataframe[list(dataframe.columns)[1]]   )




Fluxnet_db.apply_across_all_dfs(flux_format)

Fluxnet_db.apply_across_all_dfs(format_time_and_set_time_index)

Fluxnet_db.imprint_identity_across_all_dfs()

Fluxnet_db.HH_df["FI-Hyy"].info()

Fluxnet_db.HH_df["FI-Hyy"].head()

# Fluxnet_db.save_dfs_as_csv()

    
            
                





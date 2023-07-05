import pandas as pd
import matplotlib.pyplot as plt


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


#Step 2




#Fluxnet
print(flux_prep("Fluxnet\FLX_FI-Hyy_FLUXNET-CH4_2016-2016_1-1","H"))

# data = pd.read_csv('path/to/your/file.csv')
fi_hyy_data = pd.read_csv(flux_prep("Fluxnet\FLX_FI-Hyy_FLUXNET-CH4_2016-2016_1-1","H"))

print(fi_hyy_data.head())  # Display the first few rows of the DataFrame
print(fi_hyy_data.info())  # Show the information about the DataFrame








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

flux_format(fi_hyy_data)
fi_hyy_data.info() # Show the information about the DataFrame

print(iterative_flux_search_rename( "FCH4_F_ANNOPTLM_QC", "FCH4", "", "FCH4"))

# for key in header_dict:
#     print(key)


                #     dataframe.columns.str.replace(header,header_dict[key])
                #     break
                # elif header == (key +"_F"):
                #     dataframe.columns.str.replace(header,header_dict[key]+"_gap_filled")
                #     break
                # elif header == (key +"_DT"):
                #     dataframe.columns.str.replace(header,header_dict[key]+"_lr_curve_estimate")
                #     break
                # elif header == (key +"_NT"):
                #     dataframe.columns.str.replace(header,header_dict[key]+"_night_data_estimate")
                #     break
                # elif header == (key +"_F"):
                #     dataframe.columns.str.replace(header,header_dict[key]+"_gap_filled")
                #     break
                # elif header == (key +"_F"):
                #     dataframe.columns.str.replace(header,header_dict[key]+"_gap_filled")
                #     break
                # elif header == (key +"_F"):
                #     dataframe.columns.str.replace(header,header_dict[key]+"_gap_filled")
                #     break
                # elif header == (key +"_F"):
                #     dataframe.columns.str.replace(header,header_dict[key]+"_gap_filled")
                #     break








    #     delinquents.append(header)
    # for delinquent in delinquents:
    #     for key in header_dict:
    #         if delinquent == (key +"_F"):
    #            dataframe.columns.str.replace(header,header_dict[key])
    #             break 



            
                





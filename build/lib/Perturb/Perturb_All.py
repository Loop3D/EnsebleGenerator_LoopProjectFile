
import Perturb.LoopProjectFile  as LF
#import ncdump
import scipy.stats as ss
import pandas as pd
import numpy 
#from Perturb.map2loop.map2loop.m2l_utils import  value_from_dtm_dtb
import Perturb.map2loop.map2loop.m2l_utils 
import time
import random
from Perturb.m2l_utils_egen import ddd2dircos
from Perturb.m2l_utils_egen import dircos2ddd
from Perturb.spherical_utils import sample_vMF
#from Config_perturb import netCDF_file_Name
import os
import math
import configparser

os.environ['PROJ_LIB'] = 'C:\\Users\\00103098\\.conda\\envs\\EnGen_Jan\\Library\\share\\proj'

def square(x):
    return x * x



#list in datafrmae stored as string , needs conversion
def stringToList(string):
    # input format : "[42, 42, 42]" , note the spaces after the commas, in this case I have a list of integers
    string = string[1:len(string)-1]
    try:
        if len(string) != 0: 
            tempList = string.split(",")
            newList = list(map(lambda x: int(x), tempList))
        else:
            newList = []
    except:
        newList = [-9999]
    return(newList)
    

def read_config():    
    config = configparser.ConfigParser()
    config.read('Config_perturb.ini')
    #print(config.get(['common']['path_to_m2l_files'])) 
    count = 0
    for section_name in config.sections():
        if 'Parameter' in section_name:
            count  =  count + 1
    print(count)


def interface_perturb(path_to_m2l_files,path_to_perturb_models,netcdf_file_name,egen_runs,dem,source_geomodeller,intf_contact_grp_type,intf_fault_grp_type,\
ori_strati_grp_type ,ori_fault_grp_type,global_intf_contact_grp_type,global_intf_fault_grp_type,global_ori_strati_grp_type,\
global_ori_fault_grp_type,local_bool_intf_contact_grp_type,local_bool_intf_fault_grp_type,local_bool_ori_strati_grp_type,local_bool_ori_fault_grp_type,\
intf_contact_layerid,intf_faultobservations_eventid,ori_strati_layerid,ori_fault_eventid,distribution,loc_distribution,error_gps,kappa,perturb):

    print("interface")
    path_to_perturb_models = path_to_perturb_models. replace(',', '').replace('\'', '')
    params_file = open(path_to_perturb_models +  "interface_MCpara_before_per.csv", "w")
    params_file.write("samples," + str(egen_runs) + "\n")
    params_file.write("error_gps," + str(error_gps) + "\n")
    params_file.write("Group_type," + perturb + "\n")
    params_file.write("distribution," + distribution + "\n")
    params_file.write("global_intf_contact_grp_type ," + global_intf_contact_grp_type + "\n")
    params_file.write("global_intf_fault_grp_type ," + global_intf_fault_grp_type + "\n")
    params_file.write("local interface bool val ," + local_bool_intf_contact_grp_type + "\n")
    params_file.write("local fault bool val ," + local_bool_intf_fault_grp_type + "\n")
    params_file.write("distribution," + distribution + "\n")
    params_file.write("DEM," + str(dem) + "\n")
    params_file.close()

     
    
    egen_runs = egen_runs.replace(',', '').replace('\'', '')
    path_to_m2l_files = path_to_m2l_files.replace(',', '').replace('\'', '')
    netcdf_file_name = netcdf_file_name.replace(',', '').replace('\'', '')
    error_gps = float(error_gps.replace(',', '').replace('\'', ''))

  

    intf_contact_grp_type = str(intf_contact_grp_type).replace(',', '').replace('\'', '')
    global_intf_contact_grp_type = str(global_intf_contact_grp_type).replace(',', '').replace('\'', '')
    local_bool_intf_contact_grp_type = str(local_bool_intf_contact_grp_type).replace(',', '').replace('\'', '')

    intf_fault_grp_type =  str(intf_fault_grp_type).replace(',', '').replace('\'', '')
    global_intf_fault_grp_type = str(global_intf_fault_grp_type).replace(',', '').replace('\'', '')
    local_bool_intf_fault_grp_type = str(local_bool_intf_fault_grp_type).replace(',', '').replace('\'', '')

    

    if global_intf_contact_grp_type == 'True' :
        global_intf_fault_grp_type = False
        local_bool_intf_contact_grp_type = False
        local_bool_intf_fault_grp_type = False
        intf_contact_layerid = []
        intf_faultobservations_eventid =[]

    if local_bool_intf_contact_grp_type == 'True' :
        global_intf_fault_grp_type = False
        local_bool_intf_fault_grp_type = False
        intf_faultobservations_eventid =[]
        intf_contact_layerid = stringToList(intf_contact_layerid)

    if global_intf_fault_grp_type == 'True' :
        global_intf_contact_grp_type = False
        local_bool_intf_contact_grp_type = False
        local_bool_intf_fault_grp_type = False
        intf_contact_layerid = []
        intf_faultobservations_eventid =[]

    if local_bool_intf_fault_grp_type == 'True' :
        global_intf_contact_grp_type = False
        local_bool_intf_contact_grp_type = False
        global_intf_fault_grp_type = False
        intf_contact_layerid = []
        intf_faultobservations_eventid = stringToList(intf_faultobservations_eventid)


    path_interface = path_to_perturb_models +  'Interface' 
   
    if (intf_contact_grp_type =='contacts'):
        resp = LF.Get(path_to_m2l_files + netcdf_file_name,"contacts")
        if resp['errorFlag']:
            print(resp['errorString'])
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['layerId', 'easting','northing','altitude','type'])
            #print(df)

      
        
        output_location = path_interface + '/' + 'contacts'

        if (global_intf_contact_grp_type == 'True' ):
            output_location = output_location + '/' + 'global_intf_contact_grp_type' 
            if(not os.path.isdir(output_location)):
                os.makedirs(output_location) 
        
        elif (local_bool_intf_contact_grp_type  == 'True'):
            output_location = output_location + '/' + 'local_bool_intf_contact_grp_type' 
            if(not os.path.isdir(output_location)):
                os.makedirs(output_location) 
    
            
    elif( intf_fault_grp_type == 'faultobservations'):
        resp = LF.Get(path_to_m2l_files + netcdf_file_name,"faultObservations")
        if resp['errorFlag']:
            #print(resp['errorString'])
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['eventId','easting','northing','altitude','type','dipDir','dip','dipPolarity','val','displacement','posOnly'])
            

        output_location = path_interface + '/' + 'faultobservations'

        if (global_intf_fault_grp_type == 'True' ):
            output_location = output_location + '/' + 'global_intf_fault_grp_type' 
            if(not os.path.isdir(output_location)):
                os.makedirs(output_location)

        if (local_bool_intf_fault_grp_type == 'True' ):
            output_location = output_location + '/' + 'local_bool_intf_fault_grp_type'
            if(not os.path.isdir(output_location)):
                os.makedirs(output_location) 


      

    if dem is True:
        if source_geomodeller is True:
            load_this = glob.glob(f'''./MeshGrids/DTM.igmesh/*.ers''')
            dtm = rasterio.open(load_this[0])
        else:
            dtm = rasterio.open(f'''./dtm/{DTM_name}''')
            if dtm.crs.linear_units != 'metre':
                print("Warning: this DEM is not in a UTM projection.\n Please supply one and try again.")
                ''' this checks to see if the DTM projection is in metres (basic check for LoopS and geomodeller
                doesn't check that the DTM and contact/fault projections are the same. We don't input this data
                projections, so comparison isn't made at this point'''




    '''set distribution type for sampling'''
    if distribution == 'normal':
        dist_func = ss.norm.rvs
    else:
        dist_func = ss.uniform.rvs


    random.seed(time.time())

    # DEM = # import DEM here for sample new elevations for surface elevations. ISSUE: Don't want to resample elevations for interfaces at depth. Depth constraints needs to be flagged as such?
    for m in range(0, int(egen_runs)):
        if(intf_contact_grp_type == 'contacts'):
            new_coords = pd.DataFrame(numpy.zeros((len(resp['value']), 5)), columns=['layerId', 'easting','northing','altitude','type'])  # uniform
            if local_bool_intf_contact_grp_type =='True'  :
                new_coords = df[df["layerId"].isin(intf_contact_layerid)].copy()
                
           
            elif global_intf_contact_grp_type == 'True' : 
                new_coords.layerId = df.layerId.astype(str)
                new_coords.type = df.type.astype(str)

        elif(intf_fault_grp_type == 'faultobservations'):
            new_coords = pd.DataFrame(numpy.zeros((len(resp['value']), 11)), columns=['eventId', 'easting', 'northing', 'altitude', 'type', 'dipDir', 'dip', 'dipPolarity', 'val', 'displacement', 'posOnly'])  # uniform
            if local_bool_intf_fault_grp_type == 'True' :
                new_coords = df[df["eventId"].isin(intf_faultobservations_eventid)].copy()
                
            elif global_intf_fault_grp_type == 'True' :
                new_coords.eventId = df.eventId.astype(str)
                new_coords.type = df.type.astype(str)
                new_coords.dipDir=df.dipDir.astype(float)
                new_coords.dip=df.dip.astype(float)
                new_coords.dipPolarity=df.dipPolarity.astype(float)
                new_coords.val=df.val.astype(float)
                new_coords.displacement=df.displacement.astype(float)
                new_coords.posOnly=df.posOnly.astype(str)
      
        if dem is True:
            if (local_bool_Intf_contact_grp_type == 'True'   and len(intf_contact_layerid) > 0 ) :  #local conact interface  ,intf_faultObservations_eventId,
                for r in range(len(resp['value'])):
                    for ele in intf_contact_layerId :
                        if df._get_value(r,'layerId') == ele :
                            start_x =  df._get_value(r,'easting')
                            new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                            start_y = df._get_value(r, 'northing')
                            new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)
                            elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords1.loc[r, 'easting'], new_coords1.loc[r, 'northing'])])
                            if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                                new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                            else:
                                new_coords.loc[r, 'altitude'] = elevation
                if local_bool_intf_contact_grp_type == 'True'  :
                    file_name = 'local_intf_contact_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location + '/' + file_name, index=False)  

            elif (local_bool_intf_fault_grp_type == 'True'  and len(intf_faultobservations_eventid) > 0 ) :  #local fault interface
                for r in range(len(resp['value'])):
                    for ele in intf_faultObservations_eventId :
                        if df._get_value(r,'eventId') == ele :
                            start_x =  df._get_value(r,'easting')
                            new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                            start_y = df._get_value(r, 'northing')
                            new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)
                            elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords1.loc[r, 'easting'], new_coords1.loc[r, 'northing'])])
                            if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                                new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                            else:
                                new_coords.loc[r, 'altitude'] = elevation

                if  local_bool_intf_fault_grp_type == 'True' :
                    file_name = 'local_intf_fault_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location + '/' + file_name, index=False)  
                
            else: # global , contact, fault
                for r in range(len(resp['value'])):
                    start_x =  df._get_value(r,'easting')
                    new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                    start_y = df._get_value(r, 'northing')
                    new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)
                    elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords1.loc[r, 'easting'], new_coords1.loc[r, 'northing'])])
                    if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                        new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                    else:
                        new_coords.loc[r, 'altitude'] = elevation

                
        else:
            if (local_bool_intf_contact_grp_type == 'True'     and len(intf_contact_layerid) > 0 ) :  #local conact interface  ,intf_faultObservations_eventId,
                for r in range(len(resp['value'])):
                    for ele in intf_contact_layerid :
                        if df._get_value(r,'layerId') == ele :
                           start_x =  df._get_value(r,'easting')
                           new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                           start_y = df._get_value(r, 'northing')
                           new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)
                           new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                
                if local_bool_intf_contact_grp_type == 'True' :
                    file_name = 'local_intf_contact_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location + '/'  +  file_name, index=False)  


            elif (local_bool_intf_fault_grp_type == 'True'    and len(intf_faultobservations_eventid) > 0 ) :  #local fault interface
                for r in range(len(resp['value'])):
                    for ele in intf_faultobservations_eventid :
                        if df._get_value(r,'eventId') == ele :
                           start_x =  df._get_value(r,'easting')
                           new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                           start_y = df._get_value(r, 'northing')
                           new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)
                           new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')

                if  local_bool_intf_fault_grp_type == 'True' :
                    file_name = 'local_intf_fault_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location + '/' + file_name, index=False) 

            elif global_intf_fault_grp_type == 'True'   or global_intf_contact_grp_type == 'True'  :# global 
                for r in range(len(resp['value'])):
                    start_x =  df._get_value(r,'easting')
                    new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                    start_y = df._get_value(r, 'northing')
                    new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)
                    new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
              
                if global_intf_contact_grp_type =='True' :
                    file_name = 'global_intf_contact_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location + '/' + file_name, index=False)  
                    
                elif global_intf_fault_grp_type == 'True'  :
                    file_name = 'global_intf_fault_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location + '/' +  file_name, index=False)  
                        
        
    return





def orient_perturb(path_to_perturb_models,netcdf_file_name,egen_runs,dem,source_geomodeller,intf_contact_grp_type ,Intf_fault_grp_type ,ori_strati_grp_type ,ori_fault_grp_type ,\
global_intf_contact_grp_type,global_intf_fault_grp_type,global_ori_strati_grp_type,global_ori_fault_grp_type,local_bool_intf_contact_grp_type,local_bool_intf_fault_grp_type,\
local_bool_ori_strati_grp_type,local_bool_ori_fault_grp_type,intf_contact_layerid,intf_faultobservations_eventid,ori_strati_layerid,ori_fault_eventid,distribution,\
loc_distribution,error_gps,kappa,perturb):
    # samples is the number of draws, thus the number of models in the ensemble
    # kappa is the assumed error in the orientation, and is roughly the inverse to the width of the distribution
    # i.e. higher numbers = tighter distribution
    # write out parameters for record
    # output_location = './output'
    params_file = open(output_location + "/perturb_" + Group_type + "_orient_params.csv", "w")
    params_file.write("samples," + str(samples) + "\n")
    params_file.write("kappa," + str(kappa) + "\n")
    params_file.write("error_gps," + str(error_gps) + "\n")
    params_file.write("Group_type," + Group_type + "\n")
    params_file.write("location_distribution," + loc_distribution + "\n")
    params_file.write("DEM," + str(DEM) + "\n")
    params_file.close()
    
    #print(global_Ori_strati_Grp_type)
    perturb_location = perturb_location. replace(',', '')
    path_Foliation = perturb_location + '/' + 'Foliation'
    
    
    

    if( Group_type== 'faultObservations'):
        resp = LoopProjectFile.Get(path_to_m2l_files + netCDF_file_Name,"faultObservations")
        if resp['errorFlag']:
            print(resp['errorString'])
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['eventId','easting','northing','altitude','type','dipDir','dip','dipPolarity','val','displacement','posOnly'])
            #print(df)
        
        output_location = path_Foliation + '/' + 'stratigraphicObservations'

    elif( Group_type== 'stratigraphicObservations'):
        resp = LoopProjectFile.Get(path_to_m2l_files + netCDF_file_Name,"stratigraphicObservations")
        if resp['errorFlag']:
            print(resp['errorString'])
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])            
            #print(df)

        output_location = path_Foliation + '/' + 'faultObservations'

    
    if DEM is True:
        if source_geomodeller is True:
            load_this = glob.glob(f'''./MeshGrids/DTM.igmesh/*.ers''')
            dtm = rasterio.open(load_this[0])
        else:
            dtm = rasterio.open(f'''./dtm/{DTM_name}''')
            if dtm.crs.linear_units != 'metre':
                print("Warning: this DEM is not in a UTM projection.\n Please supply one and try again.")
                ''' this checks to see if the DTM projection is in metres (basic check for LoopS and geomodeller
                doesn't check that the DTM and contact/fault projections are the same. We don't input this data
                projections, so comparison isn't made at this point'''




    '''set distribution type for sampling'''
    if loc_distribution == 'normal':
        dist_func = ss.norm.rvs
    else:
        dist_func = ss.uniform.rvs



    random.seed(time.time())


    for m in range(0, samples):
        if(Group_type == 'stratigraphicObservations'):
            print("print 1")
            new_coords = pd.DataFrame(numpy.zeros((len(resp['value']), 9)), columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])  # uniform
            if local_bool_Ori_strati_Grp_type == True:
                new_coords = df[df["layerId"].isin(Ori_strati_layerId)].copy()
                #print(new_coords)
                #print("length of DF")
                #legth_local_strat = (len(new_coords))
            else:
                new_coords.layerId = df.layerId.astype(str)
                new_coords.type = df.type.astype(str)
                new_coords.dipPolarity = df.dipPolarity.astype(float)
                new_coords.layer = df.layer.astype(str)
        elif( Group_type == 'faultObservations'):
            print("print 2")
            new_coords = pd.DataFrame(numpy.zeros((len(resp['value']), 11)), columns=['eventId', 'easting', 'northing', 'altitude', 'type', 'dipDir', 'dip', 'dipPolarity', 'val', 'displacement', 'posOnly'])  # uniform
            if local_bool_Ori_Fault_Grp_type == True:
                new_coords = df[df["eventId"].isin(Ori_Fault_eventId)].copy()
                #print("length of DF")
                #print(len(new_coords))
            else:
                new_coords.eventId = df.eventId.astype(str)
                new_coords.type = df.type.astype(str)
                new_coords.dipPolarity=df.dipPolarity.astype(float)
                new_coords.val=df.val.astype(float)
                new_coords.displacement=df.displacement.astype(float)
                new_coords.posOnly=df.posOnly.astype(str)


        #for r in range(len(resp['value'])):
            
        if (local_bool_Ori_strati_Grp_type == True  and len(Ori_strati_layerId) > 0 ) :  #local conact interface  ,intf_faultObservations_eventId,
            for r in range(len(resp['value'])):
                for ele in Ori_strati_layerId :
                    if df._get_value(r,'layerId') == ele :
                        start_x =  df._get_value(r,'easting')
                        new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                        start_y = df._get_value(r, 'northing')
                        new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)

                        if DEM is True:
                            elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords.loc[r, 'easting'], new_coords.loc[r, 'northing'])])
                            if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                                new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                            else:
                                new_coords.loc[r, 'altitude'] = elevation
                        else:
                            new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
            #print(new_coords)
        elif (local_bool_Ori_Fault_Grp_type == True  and len(Ori_Fault_eventId) > 0 ) :  #local conact interface  ,intf_faultObservations_eventId,
            for r in range(len(resp['value'])):
                for ele in Ori_Fault_eventId :
                    if df._get_value(r,'eventId') == ele :
                        start_x =  df._get_value(r,'easting')
                        new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                        start_y = df._get_value(r, 'northing')
                        new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)

                        if DEM is True:
                            elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords.loc[r, 'easting'], new_coords.loc[r, 'northing'])])
                            if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                                new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                            else:
                                new_coords.loc[r, 'altitude'] = elevation
                        else:
                            new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
            #print(new_coords)

        else:
            #print("global xyz")
            for r in range(len(resp['value'])):
                start_x =  df._get_value(r,'easting')
                new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                start_y = df._get_value(r, 'northing')
                new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)

                if DEM is True:
                    elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords.loc[r, 'easting'], new_coords.loc[r, 'northing'])])
                    if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                        new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                    else:
                        new_coords.loc[r, 'altitude'] = elevation
                else:
                    new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
               


    if Group_type == 'faultObservations':
        if  local_bool_Ori_Fault_Grp_type:
            for s in range(samples):
                new_ori = []
                #new_orient = pd.DataFrame(numpy.zeros((len(resp['value']), 11)), columns=['eventId', 'easting', 'northing', 'altitude', 'type', 'dipDir', 'dip', 'dipPolarity', 'val', 'displacement', 'posOnly'])  # uniforminput_file[["X", "Y", "Z", "azimuth", "dip", "DipPolarity", "formation"]]
                new_orient = pd.DataFrame(columns=['eventId', 'easting', 'northing', 'altitude', 'type', 'dipDir', 'dip', 'dipPolarity', 'val', 'displacement', 'posOnly'])  # uniforminput_file[["X", "Y", "Z", "azimuth", "dip", "DipPolarity", "formation"]]
                for r in range(len(resp['value'])):
                    for ele in  Ori_Fault_eventId:
                        if df._get_value(r,'eventId') == ele :      
                            [l, m, n] = (ddd2dircos(df._get_value(r,'dip'), df._get_value(r,'dipDir')))
                            samp_mu = sample_vMF(numpy.array([l, m, n]), kappa, 1)
                            new_ori.append(dircos2ddd(samp_mu[0, 0], samp_mu[0, 1], samp_mu[0, 2]))
                new_ori = pd.DataFrame(new_ori)
                #print(new_ori)
                #print(new_coords)
                new_orient["easting"], new_orient["northing"], new_orient["altitude"] = new_coords["easting"], new_coords["northing"], new_coords["altitude"]
                new_orient['dipPolarity'], new_orient['val'], new_orient['displacement'], new_orient['posOnly'] = new_coords['dipPolarity'], new_coords['val'], new_coords['displacement'], new_coords['posOnly']
                new_orient['eventId'], new_orient['type'] = new_coords['eventId'],new_coords['type'] 
                new_orient["dipDir"], new_orient["dip"] = new_ori[1], new_ori[0]
                #print(new_orient)
                if(source_geomodeller):
                    new_orient.rename(columns={'azimuth' : 'dipdirection'}, inplace=True)

                file_name = Group_type + "_orientations_" + str(s) + ".csv"

                new_orient.to_csv(output_location + '/' + file_name, index=False)

    
    if Group_type == 'stratigraphicObservations':
        if local_bool_Ori_strati_Grp_type == True :
            for s in range(samples):
                new_ori = []   
                #print(len(new_coords))
                #new_orient = pd.DataFrame(numpy.zeros((len(resp['value']), 9)), columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])  # uniforminput_file[["X", "Y", "Z", "azimuth", "dip", "DipPolarity", "formation"]]
                new_orient = pd.DataFrame(columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])  # uniforminput_file[["X", "Y", "Z", "azimuth", "dip", "DipPolarity", "formation"]]
                for r in range(len(resp['value'])):
                    for ele in Ori_strati_layerId :
                        if df._get_value(r,'layerId') == ele :
                            [l, m, n] = (ddd2dircos(df._get_value(r,'dip'), df._get_value(r,'dipDir')))
                            samp_mu = sample_vMF(numpy.array([l, m, n]), kappa, 1)
                            new_ori.append(dircos2ddd(samp_mu[0, 0], samp_mu[0, 1], samp_mu[0, 2]))
                new_ori = pd.DataFrame(new_ori,columns=["dipDir","dip"])
                #print(new_ori)

                new_orient["easting"], new_orient["northing"], new_orient["altitude"] = new_coords["easting"], new_coords["northing"], new_coords["altitude"]
                new_orient["dipPolarity"], new_orient["layer"] = new_coords["dipPolarity"], new_coords["layer"]
                new_orient["layerId"], new_orient["type"] = new_coords["layerId"],new_coords["type"] 
                new_orient["dipDir"]  = new_ori["dipDir"].values
                new_orient["dip"] = new_ori["dip"].values

                
                if(source_geomodeller):
                    new_orient.rename(columns={'azimuth' : 'dipdirection'}, inplace=True)

                file_name = Group_type + "_orientations_" + str(s) + ".csv"
                new_orient.to_csv(output_location + '/' + file_name, index=False)



    if Group_type == 'faultObservations':
        print("in fault")
        if global_Ori_Fault_Grp_type == True:
            for s in range(samples):
                new_ori = []
                new_orient = pd.DataFrame(numpy.zeros((len(resp['value']), 11)), columns=['eventId', 'easting', 'northing', 'altitude', 'type', 'dipDir', 'dip', 'dipPolarity', 'val', 'displacement', 'posOnly'])  # uniform
                for r in range(len(resp['value'])):
                    [l, m, n] = ddd2dircos(df._get_value(r,'dip'), df._get_value(r,'dipDir'))
                    samp_mu = sample_vMF(numpy.array([l, m, n]), kappa, 1)
                    new_ori.append(dircos2ddd(samp_mu[0, 0], samp_mu[0, 1], samp_mu[0, 2]))
                new_ori = pd.DataFrame(new_ori)
                new_orient["easting"], new_orient["northing"], new_orient["altitude"] = new_coords["easting"], new_coords["northing"], new_coords["altitude"]
                new_orient['dipPolarity'], new_orient['val'], new_orient['displacement'], new_orient['posOnly'] = new_coords['dipPolarity'], new_coords['val'], new_coords['displacement'], new_coords['posOnly']
                new_orient['eventId'], new_orient['type'] = new_coords['eventId'],new_coords['type'] 
                new_orient["dipDir"], new_orient["dip"] = new_ori[1], new_ori[0]

                if(source_geomodeller):
                    new_orient.rename(columns={'azimuth' : 'dipdirection'}, inplace=True)

                file_name = Group_type + "_orientations_" + str(s) + ".csv"

                new_orient.to_csv(output_location + '/' + file_name, index=False)

    if Group_type == 'stratigraphicObservations':
        #print("right")
        if global_Ori_strati_Grp_type ==True:
            #print("true")
            for s in range(samples):
                new_ori = []
                new_orient = pd.DataFrame(numpy.zeros((len(resp['value']), 9)), columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])  # uniform
                #new_orient = resp[['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer']]
                for r in range(len(resp['value'])):
                    [l, m, n] = ddd2dircos(df._get_value(r,'dip'), df._get_value(r,'dipDir'))
                    samp_mu = sample_vMF(numpy.array([l, m, n]), kappa, 1)
                    new_ori.append(dircos2ddd(samp_mu[0, 0], samp_mu[0, 1], samp_mu[0, 2]))
                new_ori = pd.DataFrame(new_ori)
                new_orient["easting"], new_orient["northing"], new_orient["altitude"] = new_coords["easting"], new_coords["northing"], new_coords["altitude"]
                new_orient["dipPolarity"], new_orient["layer"] = new_coords["dipPolarity"], new_coords["layer"]
                new_orient["layerId"], new_orient["type"] = new_coords["layerId"],new_coords["type"] 
                new_orient["dipDir"], new_orient["dip"] = new_ori[1], new_ori[0]
                #new_coords["dipDir"], new_coords["dip"] = new_ori[1], new_ori[0]

                #cprint(new_coords)

                if(source_geomodeller):
                    new_orient.rename(columns={'azimuth' : 'dipdirection'}, inplace=True)

                file_name = Group_type + "_orientations_" + str(s) + ".csv"

                #new_orient.to_csv(output_location + '/' + file_name, index=False)
                new_coords.to_csv(output_location + '/' + file_name, index=False)
                #print(output_location + '/' + file_name)

    return




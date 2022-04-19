
import Perturb.LoopProjectFile  as LF
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
import os
import math
import configparser

os.environ['PROJ_LIB'] = 'C:\\Users\\00103098\\.conda\\envs\\EnGen_Jan\\Library\\share\\proj'

def square(x):
    return x * x



def stringToList(string):
    '''
        Function : list in datafrmae stored as string , needs conversion to list of numbers
        Input : list as string eg "[12,1013]"
        output : list of intergers number
    
    '''
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
    
    
    


def interface_perturb(path_to_m2l_files,path_to_perturb_models,netcdf_file_name,egen_runs,dem,source_geomodeller,intf_contact_grp_type,intf_fault_grp_type,\
ori_strati_grp_type ,ori_fault_grp_type,global_intf_contact_grp_type,global_intf_fault_grp_type,global_ori_strati_grp_type,\
global_ori_fault_grp_type,local_bool_intf_contact_grp_type,local_bool_intf_fault_grp_type,local_bool_ori_strati_grp_type,local_bool_ori_fault_grp_type,\
intf_contact_layerid,intf_faultobservations_eventid,ori_strati_layerid,ori_fault_eventid,distribution,loc_distribution,error_gps,kappa,perturb):

    '''
        Function : function perturb interface data for number of models 
        Input : inputs are huge with integers boolean list of unique ID's,path to netcdf input file , path to output models
        output : interface pertubation to te number of models
    '''
    
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

     
    # .ini file stores all data in dataframe as string  convert it to required format.
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

    
    # flags are stored as string , use them same way 
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

    #Store the perturbed outfile in the corresponding folder with path.
    path_interface = path_to_perturb_models +  'Interface' 
   
    if (intf_contact_grp_type =='contacts'):
        resp = LF.Get(path_to_m2l_files + netcdf_file_name,"contacts")
        if resp['errorFlag']:
            #print(resp['errorString'])
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['layerId', 'easting','northing','altitude','type'])
                  
        
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

    ''' generate random numbers for distribution each time '''
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
            if (local_bool_Intf_contact_grp_type == 'True'   and len(intf_contact_layerid) > 0 ) :  #local conact interface  ,intf_faultObservations_eventId, for local or specified eventid perturbation
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

            elif (local_bool_intf_fault_grp_type == 'True'  and len(intf_faultobservations_eventid) > 0 ) :  #local fault interface perturbation
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
                
            else: # global , contact, fault perturbation
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
            if (local_bool_intf_contact_grp_type == 'True'     and len(intf_contact_layerid) > 0 ) :  #local conact interface perturbation ,intf_faultObservations_eventId,
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


            elif (local_bool_intf_fault_grp_type == 'True'    and len(intf_faultobservations_eventid) > 0 ) :  #local fault interface perturbation
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

            elif global_intf_fault_grp_type == 'True'   or global_intf_contact_grp_type == 'True'  :# global fault and interface perturbation.
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





def orient_perturb(path_to_m2l_files,path_to_perturb_models,netcdf_file_name,egen_runs,dem,source_geomodeller,intf_contact_grp_type,intf_fault_grp_type,\
ori_strati_grp_type ,ori_fault_grp_type,global_intf_contact_grp_type,global_intf_fault_grp_type,global_ori_strati_grp_type,\
global_ori_fault_grp_type,local_bool_intf_contact_grp_type,local_bool_intf_fault_grp_type,local_bool_ori_strati_grp_type,local_bool_ori_fault_grp_type,\
intf_contact_layerid,intf_faultobservations_eventid,ori_strati_layerid,ori_fault_eventid,distribution,loc_distribution,error_gps,kappa,perturb):
     
    '''
        Function : function perturb orienation data for number of models 
        Input : inputs are huge with integers boolean list of unique ID's,path to netcdf input file , path to output models,samples is the number of draws, thus the number of models in the ensemble
                kappa is the assumed error in the orientation, and is roughly the inverse to the width of the distribution
                i.e. higher numbers = tighter distribution
        output : orienation pertubation to te number of models 
    '''
    path_to_perturb_models = path_to_perturb_models. replace(',', '').replace('\'', '')
    params_file = open(path_to_perturb_models +  "orient_MCpara_before_per.csv", "w")
    params_file.write("samples," + str(egen_runs) + "\n")
    params_file.write("error_gps," + str(error_gps) + "\n")
    params_file.write("Group_type," + perturb + "\n")
    params_file.write("distribution," + loc_distribution + "\n")
    params_file.write("global orient strati grp type ," + global_ori_strati_grp_type + "\n")
    params_file.write("global orient fault grp type ," + global_ori_fault_grp_type + "\n")
    params_file.write("local orient strati  grp type ," + local_bool_ori_strati_grp_type + "\n")
    params_file.write("local orient fault grp  val ," + local_bool_ori_fault_grp_type + "\n")
    params_file.write("DEM," + str(dem) + "\n")
    params_file.close()

     # .ini file stores all data in dataframe as string  convert it to required format.
    egen_runs = egen_runs.replace(',', '').replace('\'', '')
    path_to_m2l_files = path_to_m2l_files.replace(',', '').replace('\'', '')
    netcdf_file_name = netcdf_file_name.replace(',', '').replace('\'', '')
    error_gps = float(error_gps.replace(',', '').replace('\'', ''))

  

    ori_strati_grp_type = str(ori_strati_grp_type).replace(',', '').replace('\'', '')
    global_ori_strati_grp_type = str(global_ori_strati_grp_type).replace(',', '').replace('\'', '')
    local_bool_ori_strati_grp_type = str(local_bool_ori_strati_grp_type).replace(',', '').replace('\'', '')

    ori_fault_grp_type =  str(ori_fault_grp_type).replace(',', '').replace('\'', '')
    global_ori_fault_grp_type = str(global_ori_fault_grp_type).replace(',', '').replace('\'', '')
    local_bool_ori_fault_grp_type = str(local_bool_ori_fault_grp_type).replace(',', '').replace('\'', '')

    kappa = str(kappa).replace(',', '').replace('\'', '')
    
    # flags are stored as string , use them same way 
    if global_ori_strati_grp_type == 'True' :
        global_ori_fault_grp_type = False
        local_bool_ori_strati_grp_type = False
        local_bool_ori_fault_grp_type = False
        ori_strati_layerid = []
        ori_fault_eventid =[]

    if local_bool_ori_strati_grp_type == 'True' :
        global_ori_strati_grp_type = False
        global_ori_fault_grp_type = False
        local_bool_ori_fault_grp_type = False
        ori_fault_eventid =[]
        ori_strati_layerid = stringToList(ori_strati_layerid)

    if global_ori_fault_grp_type == 'True' :
        global_ori_strati_grp_type = False
        local_bool_ori_strati_grp_type = False
        local_bool_ori_fault_grp_type = False
        ori_strati_layerid = []
        ori_fault_eventid =[]

    if local_bool_ori_fault_grp_type == 'True' :
        global_ori_strati_grp_type = False
        local_bool_ori_strati_grp_type = False
        global_ori_fault_grp_type = False
        ori_strati_layerid = []
        ori_fault_eventid = stringToList(ori_fault_eventid)
    
    
    #Store the perturbed outfile in the corresponding folder with path.
    path_Orientation = path_to_perturb_models +  'Orienation' 
    
    

    if( ori_fault_grp_type == 'faultobservations'):
        resp = LF.Get(path_to_m2l_files + netcdf_file_name,"faultObservations")
        if resp['errorFlag']:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['eventId','easting','northing','altitude','type','dipDir','dip','dipPolarity','val','displacement','posOnly'])
            
        
        output_location = path_Orientation + '/' + 'faultobservations'

        if (global_ori_fault_grp_type == 'True' ):
            output_location = output_location + '/' + 'global_ori_fault_grp_type' 
            if(not os.path.isdir(output_location)):
                os.makedirs(output_location) 
        
        elif (local_bool_ori_fault_grp_type  == 'True'):
            output_location = output_location + '/' + 'local_bool_ori_fault_grp_type' 
            if(not os.path.isdir(output_location)):
                os.makedirs(output_location) 
        

    elif( ori_strati_grp_type == 'stratigraphicobservations'):
        resp = LF.Get(path_to_m2l_files + netcdf_file_name,"stratigraphicObservations")
        if resp['errorFlag']:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])            
           

        output_location = path_Orientation + '/' + 'stratigraphicobservations'

        if (global_ori_strati_grp_type == 'True' ):
            output_location = output_location + '/' + 'global_ori_strati_grp_type' 
            if(not os.path.isdir(output_location)):
                os.makedirs(output_location) 
        
        elif (local_bool_ori_strati_grp_type  == 'True'):
            output_location = output_location + '/' + 'local_bool_ori_strati_grp_type' 
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
    if loc_distribution == 'normal':
        dist_func = ss.norm.rvs
    else:
        dist_func = ss.uniform.rvs


    ''' generate random numbers for distribution each time '''
    random.seed(time.time())


    for m in range(0, int(egen_runs)):
        if(ori_strati_grp_type == 'stratigraphicobservations'):
            new_coords = pd.DataFrame(numpy.zeros((len(resp['value']), 9)), columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])  # uniform
            if local_bool_ori_strati_grp_type == 'True':
                new_coords = df[df["layerId"].isin(ori_strati_layerid)].copy()
                
            elif global_ori_strati_grp_type == 'True':
                new_coords.layerId = df.layerId.astype(str)
                new_coords.type = df.type.astype(str)
                new_coords.dipPolarity = df.dipPolarity.astype(float)
                new_coords.layer = df.layer.astype(str)
                
        elif( ori_fault_grp_type == 'faultobservations'):
            new_coords = pd.DataFrame(numpy.zeros((len(resp['value']), 11)), columns=['eventId', 'easting', 'northing', 'altitude', 'type', 'dipDir', 'dip', 'dipPolarity', 'val', 'displacement', 'posOnly'])  # uniform
            if local_bool_ori_fault_grp_type == 'True':
                new_coords = df[df["eventId"].isin(ori_fault_eventid)].copy()
                                
            elif global_ori_fault_grp_type == 'True':
                new_coords.eventId = df.eventId.astype(str)
                new_coords.type = df.type.astype(str)
                new_coords.dipPolarity=df.dipPolarity.astype(float)
                new_coords.val=df.val.astype(float)
                new_coords.displacement=df.displacement.astype(float)
                new_coords.posOnly=df.posOnly.astype(str)
                        
            
        if (local_bool_ori_strati_grp_type == 'True'  and len(ori_strati_layerid) > 0 ) :  #local  orienation for x,y,z using ,ori_strati_layerid 
            for r in range(len(resp['value'])):
                for ele in ori_strati_layerid :
                    if df._get_value(r,'layerId') == ele :
                        start_x =  df._get_value(r,'easting')
                        new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                        start_y = df._get_value(r, 'northing')
                        new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)

                        if dem is True:
                            elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords.loc[r, 'easting'], new_coords.loc[r, 'northing'])])
                            if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                                new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                            else:
                                new_coords.loc[r, 'altitude'] = elevation
                        else:
                            new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')

            if local_bool_ori_strati_grp_type == 'True' :
                file_name = 'local_bool_ori_strati_grp_type' + "_" + str(m) + ".csv"
                new_coords.to_csv(output_location + '/'  +  file_name, index=False)


            
        elif (local_bool_ori_fault_grp_type == 'True'  and len(ori_fault_eventid) > 0 ) :  #local  orienation x,y,z using ,ori_fault_eventid
            for r in range(len(resp['value'])):
                for ele in ori_fault_eventid :
                    if df._get_value(r,'eventId') == ele :
                        start_x =  df._get_value(r,'easting')
                        new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                        start_y = df._get_value(r, 'northing')
                        new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)

                        if dem is True:
                            elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords.loc[r, 'easting'], new_coords.loc[r, 'northing'])])
                            if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                                new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                            else:
                                new_coords.loc[r, 'altitude'] = elevation
                        else:
                            new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')

            if local_bool_ori_fault_grp_type == 'True' :
                file_name = 'local_bool_ori_fault_grp_type' + "_" + str(m) + ".csv"
                new_coords.to_csv(output_location + '/'  +  file_name, index=False)
            

        elif global_ori_strati_grp_type == 'True' or global_ori_fault_grp_type == 'True' : #global  orienation  using , which pertubes all data.
            
            for r in range(len(resp['value'])):
                start_x =  df._get_value(r,'easting')
                new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - (error_gps), scale=error_gps)  # value error
                start_y = df._get_value(r, 'northing')
                new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - (error_gps), scale=error_gps)

                if dem is True:
                    elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords.loc[r, 'easting'], new_coords.loc[r, 'northing'])])
                    if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                        new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                    else:
                        new_coords.loc[r, 'altitude'] = elevation
                else:
                    new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')

            if global_ori_strati_grp_type =='True' :
                file_name = 'global_ori_strati_grp_type' + "_" + str(m) + ".csv"
                new_coords.to_csv(output_location + '/' + file_name, index=False)  
                    
            elif global_ori_fault_grp_type == 'True'  :
                file_name = 'global_ori_fault_grp_type' + "_" + str(m) + ".csv"
                new_coords.to_csv(output_location + '/' +  file_name, index=False) 
               


    if ori_fault_grp_type == 'faultobservations':
        if  local_bool_ori_fault_grp_type == 'True':   # local perturbation of  dip and dipderection for fault using ori_fault_eventid.
            for s in range(int(egen_runs)):
                new_ori = []
                new_orient = pd.DataFrame(columns=['eventId', 'easting', 'northing', 'altitude', 'type', 'dipDir', 'dip', 'dipPolarity', 'val', 'displacement', 'posOnly'])  # uniforminput_file[["X", "Y", "Z", "azimuth", "dip", "DipPolarity", "formation"]]
                for r in range(len(resp['value'])):
                    for ele in  ori_fault_eventid:
                        if df._get_value(r,'eventId') == ele :      
                            [l, m, n] = (ddd2dircos(df._get_value(r,'dip'), df._get_value(r,'dipDir')))
                            samp_mu = sample_vMF(numpy.array([l, m, n]), int(kappa), 1)
                            new_ori.append(dircos2ddd(samp_mu[0, 0], samp_mu[0, 1], samp_mu[0, 2]))
                new_ori = pd.DataFrame(new_ori,columns=["dipDir","dip"])
                new_orient["easting"], new_orient["northing"], new_orient["altitude"] = new_coords["easting"], new_coords["northing"], new_coords["altitude"]
                new_orient['dipPolarity'], new_orient['val'], new_orient['displacement'], new_orient['posOnly'] = new_coords['dipPolarity'], new_coords['val'], new_coords['displacement'], new_coords['posOnly']
                new_orient['eventId'], new_orient['type'] = new_coords['eventId'],new_coords['type'] 
                new_orient["dipDir"]  = new_ori["dipDir"].values
                new_orient["dip"] = new_ori["dip"].values
                if(source_geomodeller):
                    new_orient.rename(columns={'azimuth' : 'dipdirection'}, inplace=True)

                if local_bool_ori_fault_grp_type =='True' :
                    file_name = 'local_bool_ori_fault_grp_type' + "_" + str(s) + ".csv"
                    new_orient.to_csv(output_location + '/' + file_name, index=False)

                

    
    if  ori_strati_grp_type == 'stratigraphicobservations':
        if local_bool_ori_strati_grp_type == 'True' : # local perturbation of  dip and dipderection for startigraphy using ori_strati_layerid.
            for s in range(int(egen_runs)):
                new_ori = []   
                new_orient = pd.DataFrame(columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])  # uniforminput_file[["X", "Y", "Z", "azimuth", "dip", "DipPolarity", "formation"]]
                for r in range(len(resp['value'])):
                    for ele in ori_strati_layerid :
                        if df._get_value(r,'layerId') == ele :
                            [l, m, n] = (ddd2dircos(df._get_value(r,'dip'), df._get_value(r,'dipDir')))
                            samp_mu = sample_vMF(numpy.array([l, m, n]), int(kappa), 1)
                            new_ori.append(dircos2ddd(samp_mu[0, 0], samp_mu[0, 1], samp_mu[0, 2]))
                new_ori = pd.DataFrame(new_ori,columns=["dipDir","dip"])
                new_orient["easting"], new_orient["northing"], new_orient["altitude"] = new_coords["easting"], new_coords["northing"], new_coords["altitude"]
                new_orient["dipPolarity"], new_orient["layer"] = new_coords["dipPolarity"], new_coords["layer"]
                new_orient["layerId"], new_orient["type"] = new_coords["layerId"],new_coords["type"] 
                new_orient["dipDir"]  = new_ori["dipDir"].values
                new_orient["dip"] = new_ori["dip"].values
                
                
                if(source_geomodeller):
                    new_orient.rename(columns={'azimuth' : 'dipdirection'}, inplace=True)

                

                if local_bool_ori_strati_grp_type =='True' :
                    file_name = 'local_bool_ori_strati_grp_type' + "_" + str(s) + ".csv"
                    new_orient.to_csv(output_location + '/' + file_name, index=False)



    if ori_fault_grp_type == 'faultobservations':
        if global_ori_fault_grp_type == 'True':  # global perturbation of  dip and dipdirection for fault .
            for s in range(int(egen_runs)):
                new_ori = []
                new_orient = pd.DataFrame(numpy.zeros((len(resp['value']), 11)), columns=['eventId', 'easting', 'northing', 'altitude', 'type', 'dipDir', 'dip', 'dipPolarity', 'val', 'displacement', 'posOnly'])  # uniform
                for r in range(len(resp['value'])):
                    [l, m, n] = ddd2dircos(df._get_value(r,'dip'), df._get_value(r,'dipDir'))
                    samp_mu = sample_vMF(numpy.array([l, m, n]), int(kappa), 1)
                    new_ori.append(dircos2ddd(samp_mu[0, 0], samp_mu[0, 1], samp_mu[0, 2]))
                new_ori = pd.DataFrame(new_ori,columns=["dipDir","dip"])
                new_orient["easting"], new_orient["northing"], new_orient["altitude"] = new_coords["easting"], new_coords["northing"], new_coords["altitude"]
                new_orient['dipPolarity'], new_orient['val'], new_orient['displacement'], new_orient['posOnly'] = new_coords['dipPolarity'], new_coords['val'], new_coords['displacement'], new_coords['posOnly']
                new_orient['eventId'], new_orient['type'] = new_coords['eventId'],new_coords['type'] 
                new_orient["dipDir"]  = new_ori["dipDir"].values
                new_orient["dip"] = new_ori["dip"].values

                if(source_geomodeller):
                    new_orient.rename(columns={'azimuth' : 'dipdirection'}, inplace=True)

                
                if global_ori_fault_grp_type =='True' :
                    file_name = 'global_ori_fault_grp_type' + "_" + str(s) + ".csv"
                    new_orient.to_csv(output_location + '/' + file_name, index=False)



    if  ori_strati_grp_type == 'stratigraphicobservations':
        if global_ori_strati_grp_type == 'True':  # global perturbation of  dip and dipdirection for startigrapy .
           for s in range(int(egen_runs)):
                new_ori = []
                new_orient = pd.DataFrame(numpy.zeros((len(resp['value']), 9)), columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])  # uniform
                for r in range(len(resp['value'])):
                    [l, m, n] = ddd2dircos(df._get_value(r,'dip'), df._get_value(r,'dipDir'))
                    samp_mu = sample_vMF(numpy.array([l, m, n]), int(kappa), 1)
                    new_ori.append(dircos2ddd(samp_mu[0, 0], samp_mu[0, 1], samp_mu[0, 2]))
                new_ori = pd.DataFrame(new_ori,columns=["dipDir","dip"])
                new_orient["easting"], new_orient["northing"], new_orient["altitude"] = new_coords["easting"], new_coords["northing"], new_coords["altitude"]
                new_orient["dipPolarity"], new_orient["layer"] = new_coords["dipPolarity"], new_coords["layer"]
                new_orient["layerId"], new_orient["type"] = new_coords["layerId"],new_coords["type"] 
                new_orient["dipDir"]  = new_ori["dipDir"].values
                new_orient["dip"] = new_ori["dip"].values

                if(source_geomodeller):
                    new_orient.rename(columns={'azimuth' : 'dipdirection'}, inplace=True)

                
                if global_ori_strati_grp_type =='True' :
                    file_name = 'global_ori_strati_grp_type' + "_" + str(s) + ".csv"
                    new_orient.to_csv(output_location + '/' + file_name, index=False)


    return




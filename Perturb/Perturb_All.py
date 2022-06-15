
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
import numpy as np

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
global_intf_contact_grp_type,global_intf_fault_grp_type,local_bool_intf_contact_grp_type,local_bool_intf_fault_grp_type,intf_contact_layerid,intf_faultobservations_eventid,distribution,error_gps,perturb):


    '''
        Function : function perturb interface data for number of models 
        Input : 
            path_to_m2l_files : string type path to maploop file 
            path_to_perturb_models:string type path to perturbed model files. 
            netcdf_file_name: string type path to netcdf file. 
            egen_runs: integer number of models.
            dem: boolean dem value.
            source_geomodeller: boolean type.
            intf_contact_grp_type,intf_fault_grp_type,global_intf_contact_grp_type,global_intf_fault_grp_type,local_bool_intf_contact_grp_type,local_bool_intf_fault_grp_type: boolean flags for interface group type.
            intf_contact_layerid,intf_faultobservations_eventid: list of unique value for local perturbation.
            distribution: string type to specify distribution.
            error_gps:integer error value.
            perturb:string type for perturbation.
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
    error_gps = (error_gps.replace(',', '').replace('\'', ''))

  

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

    #Store the perturbed outfile in the corresponding folder with path(.csv and loop3d files).
    path_interface = path_to_perturb_models +  'Interface' 
   
    if (intf_contact_grp_type =='contacts'):  #contact element in loop file for interface
        resp = LF.Get(path_to_m2l_files + netcdf_file_name,"contacts")
        if resp['errorFlag']:
            #print(resp['errorString'])
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['layerId', 'easting','northing','altitude','type'])
                  
        
        output_location = path_interface + '/' + 'contacts'

        if (global_intf_contact_grp_type == 'True' ):
            output_location1 = output_location + '/' + 'global_intf_contact_grp_type_csv' 
            output_location2 = output_location + '/' + 'global_intf_contact_grp_type_loop3d' 
            if(not os.path.isdir(output_location1)):
                os.makedirs(output_location1)
            if(not os.path.isdir(output_location2)):
                os.makedirs(output_location2)
        
        elif (local_bool_intf_contact_grp_type  == 'True'):
            output_location1 = output_location + '/' + 'local_bool_intf_contact_grp_type_csv' 
            output_location2 = output_location + '/' + 'local_bool_intf_contact_grp_type_loop3d'  
            if(not os.path.isdir(output_location1)):
                os.makedirs(output_location1) 
            if(not os.path.isdir(output_location2)):
                os.makedirs(output_location2)
    
            
    elif( intf_fault_grp_type == 'faultobservations'):  #fault element in loop file for interface
        resp = LF.Get(path_to_m2l_files + netcdf_file_name,"faultObservations")
        if resp['errorFlag']:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['eventId','easting','northing','altitude','type','dipDir','dip','dipPolarity','val','displacement','posOnly'])
            

        output_location = path_interface + '/' + 'faultobservations'

        if (global_intf_fault_grp_type == 'True' ):
            output_location1 = output_location + '/' + 'global_intf_fault_grp_type_csv' 
            output_location2 = output_location + '/' + 'global_intf_fault_grp_type_loop3d'  
            if(not os.path.isdir(output_location1)):
                os.makedirs(output_location1)
            if(not os.path.isdir(output_location2)):
                os.makedirs(output_location2)

        if (local_bool_intf_fault_grp_type == 'True' ):
            output_location1 = output_location + '/' + 'local_bool_intf_fault_grp_type_csv'
            output_location2 = output_location + '/' + 'local_bool_intf_fault_grp_type_loop3d'
            if(not os.path.isdir(output_location1)):
                os.makedirs(output_location1)
            if(not os.path.isdir(output_location2)):
                os.makedirs(output_location2) 



      

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
                            new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - float(error_gps), scale=float(error_gps))  # value error
                            start_y = df._get_value(r, 'northing')
                            new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - float(error_gps), scale=float(error_gps))
                            elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords1.loc[r, 'easting'], new_coords1.loc[r, 'northing'])])
                            if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                                new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                            else:
                                new_coords.loc[r, 'altitude'] = elevation
                if local_bool_intf_contact_grp_type == 'True'  :
                    file_name = 'local_intf_contact_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location1 + '/' + file_name, index=False)
                    interface_contact_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)

            elif (local_bool_intf_fault_grp_type == 'True'  and len(intf_faultobservations_eventid) > 0 ) :  #local fault interface perturbation
                for r in range(len(resp['value'])):
                    for ele in intf_faultObservations_eventId :
                        if df._get_value(r,'eventId') == ele :
                            start_x =  df._get_value(r,'easting')
                            new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - float(error_gps), scale=float(error_gps))  # value error
                            start_y = df._get_value(r, 'northing')
                            new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - float(error_gps), scale=float(error_gps))
                            elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords1.loc[r, 'easting'], new_coords1.loc[r, 'northing'])])
                            if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                                new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                            else:
                                new_coords.loc[r, 'altitude'] = elevation

                if  local_bool_intf_fault_grp_type == 'True' :
                    file_name = 'local_intf_fault_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location1 + '/' + file_name, index=False)  
                    interface_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)
                
            else: # global , contact, fault perturbation
                for r in range(len(resp['value'])):
                    start_x =  df._get_value(r,'easting')
                    new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - float(error_gps), scale=float(error_gps))  # value error
                    start_y = df._get_value(r, 'northing')
                    new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - float(error_gps), scale=float(error_gps))
                    elevation = m2l_utils.value_from_dtm_dtb(dtm, "", "",False,  [(new_coords1.loc[r, 'easting'], new_coords1.loc[r, 'northing'])])
                    if elevation == -999:  # points outside of the dtm will get a elevation of -999, this is to check for that. If outside, it uses the existing elevation
                        new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                    else:
                        new_coords.loc[r, 'altitude'] = elevation
                        
                if global_intf_contact_grp_type =='True' :
                    file_name = 'global_intf_contact_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location1 + '/' + file_name, index=False)
                    interface_contact_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)  
                    
                elif global_intf_fault_grp_type == 'True'  :
                    file_name = 'global_intf_fault_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location1 + '/' +  file_name, index=False)
                    interface_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name) 

                
        else:
            if (local_bool_intf_contact_grp_type == 'True'     and len(intf_contact_layerid) > 0 ) :  #local conact interface perturbation ,intf_faultObservations_eventId,
                for r in range(len(resp['value'])):
                    for ele in intf_contact_layerid :
                        if df._get_value(r,'layerId') == ele :
                           start_x =  df._get_value(r,'easting')
                           new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - float(error_gps), scale=float(error_gps))  # value error
                           start_y = df._get_value(r, 'northing')
                           new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - float(error_gps), scale=float(error_gps))
                           new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
                
                if local_bool_intf_contact_grp_type == 'True' :
                    file_name = 'local_intf_contact_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location1 + '/'  +  file_name, index=False)
                    interface_contact_perturbed_csv_loop3d(output_location1 , output_location2 , file_name) 


            elif (local_bool_intf_fault_grp_type == 'True'    and len(intf_faultobservations_eventid) > 0 ) :  #local fault interface perturbation
                for r in range(len(resp['value'])):
                    for ele in intf_faultobservations_eventid :
                        if df._get_value(r,'eventId') == ele :
                           start_x =  df._get_value(r,'easting')
                           new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - float(error_gps), scale=float(error_gps))  # value error
                           start_y = df._get_value(r, 'northing')
                           new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - float(error_gps), scale=float(error_gps))
                           new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')

                if  local_bool_intf_fault_grp_type == 'True' :
                    file_name = 'local_intf_fault_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location1 + '/' + file_name, index=False)
                    interface_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name) 

            elif global_intf_fault_grp_type == 'True'   or global_intf_contact_grp_type == 'True'  :# global fault and interface perturbation.
                for r in range(len(resp['value'])):
                    start_x =  df._get_value(r,'easting')
                    new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - float(error_gps), scale=float(error_gps))  # value error
                    start_y = df._get_value(r, 'northing')
                    new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - float(error_gps), scale=float(error_gps))
                    new_coords.loc[r, 'altitude'] = df._get_value(r, 'altitude')
              
                if global_intf_contact_grp_type =='True' :
                    file_name = 'global_intf_contact_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location1 + '/' + file_name, index=False)  
                    interface_contact_perturbed_csv_loop3d(output_location1 , output_location2 , file_name) 
                    
                elif global_intf_fault_grp_type == 'True'  :
                    file_name = 'global_intf_fault_grp_type' + "_" + str(m) + ".csv"
                    new_coords.to_csv(output_location1 + '/' +  file_name, index=False)
                    interface_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name) 
                        
        
    return





def orient_perturb(path_to_m2l_files,path_to_perturb_models,netcdf_file_name,egen_runs,dem,source_geomodeller,ori_strati_grp_type ,ori_fault_grp_type,global_ori_strati_grp_type,\
global_ori_fault_grp_type,local_bool_ori_strati_grp_type,local_bool_ori_fault_grp_type,ori_strati_layerid,ori_fault_eventid,distribution,loc_distribution,error_gps,kappa,perturb):

     
    '''
        Function : function perturb orienation data for number of models 
        Input : path_to_m2l_files : string type path to maploop file 
            path_to_perturb_models:string type path to perturbed model files. 
            netcdf_file_name: string type path to netcdf file. 
            egen_runs: integer number of models.
            dem: boolean dem value.
            source_geomodeller: boolean type.
            ori_strati_grp_type ,ori_fault_grp_type,global_ori_strati_grp_type,global_ori_fault_grp_type,local_bool_ori_strati_grp_type,local_bool_ori_fault_grp_type: boolean flags for orienation group type.
            intf_contact_layerid,intf_faultobservations_eventid: list of unique value for local perturbation.
            distribution: string type to specify distribution.
            error_gps:integer error value.
            perturb:string type for perturbation
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
    error_gps = (error_gps.replace(',', '').replace('\'', ''))

  

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
    
    

    if( ori_fault_grp_type == 'faultobservations'):  #fault  element in loop file for orienation
        resp = LF.Get(path_to_m2l_files + netcdf_file_name,"faultObservations")
        if resp['errorFlag']:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['eventId','easting','northing','altitude','type','dipDir','dip','dipPolarity','val','displacement','posOnly'])
            
        
        output_location = path_Orientation + '/' + 'faultobservations'

        if (global_ori_fault_grp_type == 'True' ):
            output_location1 = output_location + '/' + 'global_ori_fault_grp_type_csv' 
            output_location2 = output_location + '/' + 'global_ori_fault_grp_type_loop3d'
            if(not os.path.isdir(output_location1)):
                os.makedirs(output_location1)
            if(not os.path.isdir(output_location2)):
                os.makedirs(output_location2)  
        
        elif (local_bool_ori_fault_grp_type  == 'True'):
            output_location1 = output_location + '/' + 'local_bool_ori_fault_grp_type_csv' 
            output_location2 = output_location + '/' + 'local_bool_ori_fault_grp_type_loop3d'
            if(not os.path.isdir(output_location1)):
                os.makedirs(output_location1)
            if(not os.path.isdir(output_location2)):
                os.makedirs(output_location2) 
        

    elif( ori_strati_grp_type == 'stratigraphicobservations'): #startigraphic element in loop file for orienarion
        resp = LF.Get(path_to_m2l_files + netcdf_file_name,"stratigraphicObservations")
        if resp['errorFlag']:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(resp['value'],columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])            
           

        output_location = path_Orientation + '/' + 'stratigraphicobservations'

        if (global_ori_strati_grp_type == 'True' ):
            output_location1 = output_location + '/' + 'global_ori_strati_grp_type_csv' 
            output_location2 = output_location + '/' + 'global_ori_strati_grp_type_loop3d' 
            if(not os.path.isdir(output_location1)):
                os.makedirs(output_location1)
            if(not os.path.isdir(output_location2)):
                os.makedirs(output_location2) 
        
        elif (local_bool_ori_strati_grp_type  == 'True'):
            output_location1 = output_location + '/' + 'local_bool_ori_strati_grp_type_csv' 
            output_location2 = output_location + '/' + 'local_bool_ori_strati_grp_type_loop3d' 
            if(not os.path.isdir(output_location1)):
                os.makedirs(output_location1)
            if(not os.path.isdir(output_location2)):
                os.makedirs(output_location2) 

    
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
                        new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - float(error_gps), scale=float(error_gps))  # value error
                        start_y = df._get_value(r, 'northing')
                        new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - float(error_gps), scale=float(error_gps))

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
                new_coords.to_csv(output_location1 + '/'  +  file_name, index=False)
                orientaion_stratigraphy_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)


            
        elif (local_bool_ori_fault_grp_type == 'True'  and len(ori_fault_eventid) > 0 ) :  #local  orienation x,y,z using ,ori_fault_eventid
            for r in range(len(resp['value'])):
                for ele in ori_fault_eventid :
                    if df._get_value(r,'eventId') == ele :
                        start_x =  df._get_value(r,'easting')
                        new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - float(error_gps), scale=float(error_gps))  # value error
                        start_y = df._get_value(r, 'northing')
                        new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - float(error_gps), scale=float(error_gps))

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
                new_coords.to_csv(output_location1 + '/'  +  file_name, index=False)
                orienation_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)
            

        elif global_ori_strati_grp_type == 'True' or global_ori_fault_grp_type == 'True' : #global  orienation  using , which pertubes all data.
            
            for r in range(len(resp['value'])):
                start_x =  df._get_value(r,'easting')
                new_coords.loc[r, 'easting'] = dist_func(size=1, loc=start_x - float(error_gps), scale=float(error_gps))  # value error
                start_y = df._get_value(r, 'northing')
                new_coords.loc[r, 'northing'] = dist_func(size=1, loc=start_y - float(error_gps), scale=float(error_gps))

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
                new_coords.to_csv(output_location1 + '/' + file_name, index=False)
                orientaion_stratigraphy_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)                
                    
            elif global_ori_fault_grp_type == 'True'  :
                file_name = 'global_ori_fault_grp_type' + "_" + str(m) + ".csv"
                new_coords.to_csv(output_location1 + '/' +  file_name, index=False)
                orienation_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)
                
               


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
                    new_orient.to_csv(output_location1 + '/' + file_name, index=False)
                    orienation_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)

                

    
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
                    new_orient.to_csv(output_location1 + '/' + file_name, index=False)
                    orientaion_stratigraphy_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)



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
                    new_orient.to_csv(output_location1 + '/' + file_name, index=False)
                    orienation_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)



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
                    new_orient.to_csv(output_location1 + '/' + file_name, index=False)
                    orientaion_stratigraphy_perturbed_csv_loop3d(output_location1 , output_location2 , file_name)


    return
    
    
    
    
    

def drillhole_perturb(path_to_m2l_files,path_to_perturb_models,netcdf_file_name1,netcdf_file_name2,netcdf_file_name3,egen_runs,dem,source_geomodeller,\
    drillhole_grp_type1,drillhole_grp_type2,drillhole_grp_type3,global_bool_drillhole,local_bool_drillhole,drillhole_collarid,distribution,erraz,errdip,errlength,perturb):
    '''
        Function : function perturb drillhole data for number of models given in ini file.
        Input : path_to_m2l_files : string type path to maploop file 
            path_to_perturb_models:string type path to perturbed model files. 
            netcdf_file_name1: string type path to netcdf files used for drillhole collar file. 
            netcdf_file_name2: string type path to netcdf files used for drillhole survey file. 
            netcdf_file_name3: string type path to netcdf files used for drillhole lithology file. 
            egen_runs: integer number of models to generate after perturbation.
            dem: boolean dem value.
            source_geomodeller: boolean type.
            drillhole_grp_type1,drillhole_grp_type2,drillhole_grp_type3 : flags for netcdf file elemnts names to extract drillhole data.
            distribution: string type to specify distribution.
            global_bool_drillhole,local_bool_drillhole :global , local flag for drillhole perturbation.
            drillhole_collarid : list of collarID to perturb for local drillhole.
            error_gps:integer error value.
            perturb:string type for perturbation.
        output : drillhole pertubation to te number of models. 
    '''

    # write input data to csv file.
    path_to_perturb_models = path_to_perturb_models. replace(',', '').replace('\'', '')
    params_file = open(path_to_perturb_models +  "Drillhole_MCpara_before_per.csv", "w")
    params_file.write("samples," + str(egen_runs) + "\n")
    params_file.write("drillhole_grp_type1," + drillhole_grp_type1 + "\n")
    params_file.write("drillhole_grp_type2," + drillhole_grp_type2 + "\n")
    params_file.write("drillhole_grp_type3," + drillhole_grp_type3 + "\n")
    params_file.write("distribution," + distribution + "\n")
    params_file.write("global drillhole  ," + global_bool_drillhole + "\n")
    params_file.write("lcal drillhole  ," + local_bool_drillhole + "\n")
    params_file.write("azimuth error drillhole  ," + erraz + "\n")
    params_file.write("dip error drillhole  ," + errdip + "\n")
    params_file.write("length error drillhole  ," + errlength + "\n")
    params_file.write("DEM," + str(dem) + "\n")
    params_file.close()

    #data from .ini file are as string with comma ("," ).convert before using.
    egen_runs = egen_runs.replace(',', '').replace('\'', '')
    path_to_m2l_files = path_to_m2l_files.replace(',', '').replace('\'', '')
    netcdf_file_name1 = netcdf_file_name1.replace(',', '').replace('\'', '')
    netcdf_file_name2 = netcdf_file_name2.replace(',', '').replace('\'', '')
    netcdf_file_name3 = netcdf_file_name3.replace(',', '').replace('\'', '')

    path_to_perturb_models =  path_to_perturb_models.replace(',', '').replace('\'', '')
    drillhole_grp_type1 = str(drillhole_grp_type1).replace(',', '').replace('\'', '')
    drillhole_grp_type2 = str(drillhole_grp_type2).replace(',', '').replace('\'', '')
    drillhole_grp_type3 = str(drillhole_grp_type3).replace(',', '').replace('\'', '')
    

    global_bool_drillhole = str(global_bool_drillhole).replace(',', '').replace('\'', '')
    local_bool_drillhole = str(local_bool_drillhole).replace(',', '').replace('\'', '')
    

    dem = str(dem).replace(',', '').replace('\'', '')
    erraz = (erraz.replace(',', '').replace('\'', ''))
    errdip = (errdip.replace(',', '').replace('\'', ''))
    errlength = (errlength.replace(',', '').replace('\'', ''))
    



    if global_bool_drillhole == 'True' :
        local_bool_drillhole = False
        drillhole_collarid = []
    if local_bool_drillhole == 'True' :
        global_bool_drillhole = False
        drillhole_collarid = stringToList(drillhole_collarid)
        
   
    
    
    #path to the output files geberated after perturbation.
    path_drillhole = path_to_perturb_models +  'Drillhole' 
    output_location = path_drillhole
    
    

    if (global_bool_drillhole == 'True' ):
        output_location1 = output_location + '/' + 'global_bool_drillhole_csv' 
        output_location2 = output_location + '/' + 'global_bool_drillhole_loop3d' 
        if(not os.path.isdir(output_location1)):
            os.makedirs(output_location1) 
        if(not os.path.isdir(output_location2)):
            os.makedirs(output_location2)
            
            
    elif (local_bool_drillhole  == 'True'):
        output_location1 = output_location + '/' + 'local_bool_drillhole_csv' 
        output_location2 = output_location + '/' + 'local_bool_drillhole_loop3d' 
        if(not os.path.isdir(output_location1)):
            os.makedirs(output_location1) 
        if(not os.path.isdir(output_location2)):
            os.makedirs(output_location2) 
   
   #flags to read collar,survey,litology elements in loopproectFile.
    if (drillhole_grp_type1 =='drillholelog' and drillhole_grp_type2 =='drillholesurveys' and drillhole_grp_type3 =='drillholeobservations'):
        resp1 = LF.Get(path_to_m2l_files + netcdf_file_name1,"drillholeLog")
        if resp1['errorFlag']:
           collar_df = pd.DataFrame()
        else:
           collar_df = pd.DataFrame.from_records(resp1['value'],columns=['collarId', 'holeName','surfaceX','surfaceY','surfaceZ'])
           new_coords1 = pd.DataFrame(numpy.zeros((len(resp1['value']), 5)), columns=['collarId', 'holeName','surfaceX','surfaceY','surfaceZ'])  # we dont perturb collar data we use first row.
           new_coords1.collarId = collar_df.collarId.astype(str)
           new_coords1.holeName = collar_df.holeName.astype(str)
           new_coords1.surfaceX = collar_df.surfaceX.astype(float)
           new_coords1.surfaceY = collar_df.surfaceY.astype(float)
           new_coords1.surfaceZ = collar_df.surfaceZ.astype(float)
           file_name1 = 'drillhole_collar' +  ".csv"
           new_coords1.to_csv(output_location1 + '/'  +  file_name1, index=False)
           drillhole_collar_csv_loop3d(output_location1 , output_location2 , file_name1)
        

        resp2 = LF.Get(path_to_m2l_files + netcdf_file_name2,"drillholeSurveys")  #survey data from loopprojectFile.
        if resp2['errorFlag']:
           survey_df = pd.DataFrame()
        else:
           survey_df = pd.DataFrame.from_records(resp2['value'],columns=['collarId', 'depth','angle1','angle2','unit'])
        
        

        resp3 = LF.Get(path_to_m2l_files + netcdf_file_name3,"drillholeObservations")     #lithology data from loopprojectFile.
        if resp3['errorFlag']:
           lithology_df = pd.DataFrame()
        else:
           lithology_df = pd.DataFrame.from_records(resp3['value'],columns=['collarId', 'fromX','fromY','fromZ','layerId','toX','toY', 'toZ','from','to','propertyCode','property1','property2','unit'])
        

  
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

    for m in range(0, int(egen_runs)):
        if (drillhole_grp_type1 =='drillholelog' and drillhole_grp_type2 =='drillholesurveys' and drillhole_grp_type3 =='drillholeobservations'):
            new_coords2 = pd.DataFrame(numpy.zeros((len(resp2['value']), 5)), columns=['collarId', 'depth','angle1','angle2','unit'])  # uniform
            if local_bool_drillhole == 'True':
                new_coords2 = survey_df[survey_df["collarId"].isin(drillhole_collarid)].copy()
                
            elif global_bool_drillhole == 'True':
                new_coords2.depth = survey_df.depth.astype(int)
                new_coords2.collarId = survey_df.collarId.astype(int)
                new_coords2.unit = survey_df.unit.astype(float)
                
            new_coords3 = pd.DataFrame(numpy.zeros((len(resp3['value']), 14)), columns=['collarId', 'fromX','fromY','fromZ','layerId','toX','toY', 'toZ','from','to','propertyCode','property1','property2','unit'])  # uniform
            if local_bool_drillhole == 'True':
                new_coords3 = lithology_df[lithology_df["collarId"].isin(drillhole_collarid)].copy()
                
            elif global_bool_drillhole == 'True':
                new_coords3.collarId = lithology_df.collarId.astype(int)
                new_coords3.fromX = lithology_df.fromX.astype(float)
                new_coords3.fromY = lithology_df.fromY.astype(float)
                new_coords3.fromZ = lithology_df.fromZ.astype(float)
                new_coords3.layerId = lithology_df.layerId.astype(str)
                new_coords3.toX = lithology_df.toX.astype(float)
                new_coords3.toY = lithology_df.toY.astype(float)
                new_coords3.toZ = lithology_df.toZ.astype(float)
                new_coords3.propertyCode = lithology_df.propertyCode.astype(float)
                new_coords3.property1 = lithology_df.property1.astype(float)
                new_coords3.property2 = lithology_df.property2.astype(float)
                new_coords3.unit = lithology_df.unit.astype(float)
                


        
        
            if (local_bool_drillhole == 'True'   and len(drillhole_collarid) > 0 ) :  #local drillhole perturbation considered by collarid. 
                first_depth_from_zero =  False
                prev_collarID = 0
                for r1 in range(len(resp2['value'])):
                    if survey_df._get_value(r1,'collarId') != prev_collarID:
                        first_depth_from_zero = False
                    for r2 in range(len(resp3['value'])):
                        for ele in drillhole_collarid :
                            if survey_df._get_value(r1,'collarId') == ele and  lithology_df._get_value(r2,'collarId') == ele :
                                if  survey_df._get_value(r1,'depth') == 0 and  lithology_df._get_value(r2,'from') ==  0 and first_depth_from_zero == False : #check for depth , from starts with 0 for new holeID
                                    first_depth_from_zero = True
                                    start_x =  survey_df._get_value(r1,'angle1')
                                    new_coords2.loc[r1, 'angle1'] = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  # value error
                                    start_y =  survey_df._get_value(r1,'angle2')
                                    new_coords2.loc[r1, 'angle2'] = dist_func(size=1, loc=(start_y) - float(errdip), scale=float(errdip))  # value error
                    
                                    litho_to =  lithology_df._get_value(r2,'to') 
                                    litho_from =  lithology_df._get_value(r2,'from') 
                                    length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                    start_z =  dist_func(size=1, loc= length - float(errlength), scale=float(errlength))
                                    new_coords3.loc[r2, 'from'] = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  # value error
                                    new_coords3.loc[r2, 'to'] = dist_func(size=1, loc= litho_to - float(errlength), scale= float(errlength))  # value error
                                elif survey_df._get_value(r1,'depth') == 0 and  lithology_df._get_value(r2,'from') !=  0 and first_depth_from_zero == True :  # check for second row in lithology of each hole.
                                    start_x =  survey_df._get_value(r1,'angle1')
                                    new_coords2.loc[r1, 'angle1'] = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  # value error
                                    start_y =  survey_df._get_value(r1,'angle2')
                                    new_coords2.loc[r1, 'angle2'] = dist_func(size=1, loc=(start_y) - float(errdip), scale= float(errdip))  # value error
                    
                                    litho_to =  lithology_df._get_value(r2,'to') 
                                    litho_from =  lithology_df._get_value(r2,'from') 
                                    length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                    start_z =  dist_func(size=1, loc= length - float(errlength), scale= float(errlength))
                                    new_coords3.loc[r2, 'from'] = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  # value error
                                    new_coords3.loc[r2, 'to'] = dist_func(size=1, loc= litho_to - float(errlength), scale=float(errlength))  # value error

                                elif survey_df._get_value(r1,'depth') != 0 and  lithology_df._get_value(r2,'from') !=  0 and first_depth_from_zero == True :  # check for second row in lithology of each hole.
                                    start_x =  survey_df._get_value(r1,'angle1')
                                    new_coords2.loc[r1, 'angle1'] = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  # value error
                                    start_y =  survey_df._get_value(r1,'angle2')
                                    new_coords2.loc[r1, 'angle2'] = dist_func(size=1, loc=(start_y) - float(errdip), scale= float(errdip))  # value error
                    
                                    litho_to =  lithology_df._get_value(r2,'to') 
                                    litho_from =  lithology_df._get_value(r2,'from') 
                                    length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                    start_z =  dist_func(size=1, loc= length - float(errlength), scale= float(errlength))
                                    new_coords3.loc[r2, 'from'] = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  # value error
                                    new_coords3.loc[r2, 'to'] = dist_func(size=1, loc= litho_to - float(errlength), scale=float(errlength))  # value error

                                elif survey_df._get_value(r1,'depth') != 0 and  lithology_df._get_value(r2,'from') ==  0 and first_depth_from_zero == True :  #check for second row in survey table.
                                    start_x =  survey_df._get_value(r1,'angle1')
                                    new_coords2.loc[r1, 'angle1'] = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  # value error
                                    start_y =  survey_df._get_value(r1,'angle2')
                                    new_coords2.loc[r1, 'angle2'] = dist_func(size=1, loc=(start_y) - float(errdip), scale= float(errdip))  # value error
                    
                                    litho_to =  lithology_df._get_value(r2,'to') 
                                    litho_from =  lithology_df._get_value(r2,'from') 
                                    length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                    start_z =  dist_func(size=1, loc= length - float(errlength), scale= float(errlength))
                                    new_coords3.loc[r2, 'from'] = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  # value error
                                    new_coords3.loc[r2, 'to'] = dist_func(size=1, loc= litho_to - float(errlength), scale=float(errlength))  # value error


                                elif survey_df._get_value(r1,'depth') != 0 or  lithology_df._get_value(r2,'from') !=  0 and first_depth_from_zero == False : #if depth doest start from 0 in survey or lithology,  add a row with depth 0 and existing first row values
                                    first_depth_from_zero = True
                                    
                                    if survey_df._get_value(r1,'depth') != 0 :
                                        start_x =  survey_df._get_value(r1,'angle1')
                                        angle1 = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  # value error
                                        new_coords2.loc[r1, 'angle1'] = angle1 
                                        start_y =  survey_df._get_value(r1,'angle2')
                                        angle2 = dist_func(size=1, loc=(start_y) - float(errdip), scale=float(errdip))  # value error
                                        new_coords2.loc[r1, 'angle2'] = angle2 
                                        collarID_ = survey_df._get_value(r1,'collarId')
                                        new_coords2.loc[r1, 'depth'] = '0'
                                        unit = survey_df._get_value(r1,'unit')
                                        new_coords2.loc[len(new_coords2.index)] = [collarID_, '0', angle1,angle2,unit] 
                                    if lithology_df._get_value(r2,'from') !=  0 :
                                        litho_to =  lithology_df._get_value(r2,'to') 
                                        litho_from =  lithology_df._get_value(r2,'from') 
                                        length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                        start_z =  dist_func(size=1, loc= length - float(errlength), scale=float(errlength))
                                        from_ = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  # value error
                                        new_coords3.loc[r2, 'from'] = from_
                                        to_= dist_func(size=1, loc= litho_to - float(errlength), scale= float(errlength))  # value error
                                        new_coords3.loc[r2, 'to'] = to_
                                    
                                        collarID_ = lithology_df._get_value(r2,'collarId')
                                        fromX_ = lithology_df._get_value(r2,'fromX')
                                        fromY_ = lithology_df._get_value(r2,'fromY')
                                        fromZ_ = lithology_df._get_value(r2,'fromZ')
                                        layerId_ = lithology_df._get_value(r2,'layerId')
                                        toX_ = lithology_df._get_value(r2,'toX')
                                        toY_ = lithology_df._get_value(r2,'toY')
                                        toZ_ = lithology_df._get_value(r2,'toZ')
                                        propertyCode_ = lithology_df._get_value(r2,'propertyCode')
                                        property1_ = lithology_df._get_value(r2,'property1')
                                        property2_ = lithology_df._get_value(r2,'property2')
                                        unit_ = lithology_df._get_value(r2,'unit')
                                        new_coords3.loc[len(new_coords3.index)] = [collarID_, '0', from_,to_,fromX_,fromY_,fromZ_,'layerId_',toX_,toY_,toZ_,propertyCode_,property1_,property2_,unit] 
                        prev_collarID = ele                
                
            if local_bool_drillhole == 'True' :
                file_name2 = 'local_drillhole_survey' + "_" + str(m) + ".csv"
                sorted_new_coords2 = new_coords2.sort_values(by=['collarId','depth'], ascending=[True,True])
                sorted_new_coords2.to_csv(output_location1 + '/'  +  file_name2, index=False)
                drillhole_survey_perturbed_csv_loop3d(output_location1 , output_location2 , file_name2)

                file_name3 = 'local_drillhole_lithology' + "_" + str(m) + ".csv"
                sorted_new_coords3 = new_coords3.sort_values(by=['collarId','from'], ascending=[True,True])
                sorted_new_coords3.to_csv(output_location1 + '/'  +  file_name3, index=False)
                drillhole_lithology_perturbed_csv_loop3d(output_location1 , output_location2 , file_name3)


            
            if global_bool_drillhole == 'True' :  #global drillhole had all drillholes.
                first_depth_from_zero =  False
                cur_collarId = 0
                prev_collarID = 0
                for r1 in range(len(resp2['value'])):
                    cur_collarId = survey_df._get_value(r1,'collarId')
                    prev_collarID = cur_collarId
                    if prev_collarID != cur_collarId:
                        first_depth_from_zero = False
                        

                    for r2 in range(len(resp3['value'])):
                    
                        if survey_df._get_value(r1,'collarId') == cur_collarId and  lithology_df._get_value(r2,'collarId') == cur_collarId :
                            if  survey_df._get_value(r1,'depth') == 0 and  lithology_df._get_value(r2,'from') ==  0 and first_depth_from_zero == False : 
                                first_depth_from_zero = True
                                start_x =  survey_df._get_value(r1,'angle1')
                                new_coords2.loc[r1, 'angle1'] = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  
                                start_y =  survey_df._get_value(r1,'angle2')
                                new_coords2.loc[r1, 'angle2'] = dist_func(size=1, loc=(start_y) - float(errdip), scale= float(errdip))  
                    
                                litho_to =  lithology_df._get_value(r2,'to') 
                                litho_from =  lithology_df._get_value(r2,'from') 
                                length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                start_z =  dist_func(size=1, loc= length - float(errlength), scale=float(errlength))
                                new_coords3.loc[r2, 'from'] = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  
                                new_coords3.loc[r2, 'to'] = dist_func(size=1, loc= litho_to - float(errlength), scale=float(errlength))  
                                

                            elif survey_df._get_value(r1,'depth') == 0 and  lithology_df._get_value(r2,'from') !=  0 and first_depth_from_zero == True : 
                                start_x =  survey_df._get_value(r1,'angle1')
                                new_coords2.loc[r1, 'angle1'] = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  
                                start_y =  survey_df._get_value(r1,'angle2')
                                new_coords2.loc[r1, 'angle2'] = dist_func(size=1, loc=(start_y) - float(errdip), scale=float(errdip))  
                    
                                litho_to =  lithology_df._get_value(r2,'to') 
                                litho_from =  lithology_df._get_value(r2,'from') 
                                length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                start_z =  dist_func(size=1, loc= length - float(errlength), scale=float(errlength))
                                new_coords3.loc[r2, 'from'] = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  
                                new_coords3.loc[r2, 'to'] = dist_func(size=1, loc= litho_to - float(errlength), scale=float(errlength))  

                            elif survey_df._get_value(r1,'depth') != 0 and  lithology_df._get_value(r2,'from') ==  0 and first_depth_from_zero == True :
                                    start_x =  survey_df._get_value(r1,'angle1')
                                    new_coords2.loc[r1, 'angle1'] = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  
                                    start_y =  survey_df._get_value(r1,'angle2')
                                    new_coords2.loc[r1, 'angle2'] = dist_func(size=1, loc=(start_y) - float(errdip), scale= float(errdip))  
                    
                                    litho_to =  lithology_df._get_value(r2,'to') 
                                    litho_from =  lithology_df._get_value(r2,'from') 
                                    length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                    start_z =  dist_func(size=1, loc= length - float(errlength), scale= float(errlength))
                                    new_coords3.loc[r2, 'from'] = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  
                                    new_coords3.loc[r2, 'to'] = dist_func(size=1, loc= litho_to - float(errlength), scale=float(errlength))  

                            elif survey_df._get_value(r1,'depth') != 0 and  lithology_df._get_value(r2,'from') !=  0 and first_depth_from_zero == True :
                                    start_x =  survey_df._get_value(r1,'angle1')
                                    new_coords2.loc[r1, 'angle1'] = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  
                                    start_y =  survey_df._get_value(r1,'angle2')
                                    new_coords2.loc[r1, 'angle2'] = dist_func(size=1, loc=(start_y) - float(errdip), scale= float(errdip))  
                    
                                    litho_to =  lithology_df._get_value(r2,'to') 
                                    litho_from =  lithology_df._get_value(r2,'from') 
                                    length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                    start_z =  dist_func(size=1, loc= length - float(errlength), scale= float(errlength))
                                    new_coords3.loc[r2, 'from'] = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  
                                    new_coords3.loc[r2, 'to'] = dist_func(size=1, loc= litho_to - float(errlength), scale=float(errlength))  

                            elif survey_df._get_value(r1,'depth') != 0 or  lithology_df._get_value(r2,'from') !=  0 and first_depth_from_zero == False : 
                                first_depth_from_zero = True
                                if survey_df._get_value(r1,'depth') != 0 :
                                    start_x =  survey_df._get_value(r1,'angle1')
                                    angle1 = dist_func(size=1, loc=(start_x) - float(erraz), scale=float(erraz))  
                                    new_coords2.loc[r1, 'angle1'] = angle1 
                                    start_y =  survey_df._get_value(r1,'angle2')
                                    angle2 = dist_func(size=1, loc=(start_y) - float(errdip), scale=float(errdip))  
                                    new_coords2.loc[r1, 'angle2'] = angle2 
                                    collarID_ = survey_df._get_value(r1,'collarId')
                                    new_coords2.loc[r1, 'depth'] = '0'
                                    unit = survey_df._get_value(r1,'unit')
                                    new_coords2.loc[len(new_coords2.index)] = [collarID_, '0', angle1,angle2,unit] 
                                if lithology_df._get_value(r2,'from') !=  0 :
                                    litho_to =  lithology_df._get_value(r2,'to') 
                                    litho_from =  lithology_df._get_value(r2,'from') 
                                    length = (lithology_df._get_value(r2,'to')) - (lithology_df._get_value(r2,'from'))
                                    start_z =  dist_func(size=1, loc= length - float(errlength), scale=float(errlength))
                                    from_ = dist_func(size=1, loc= litho_from - float(errlength), scale=float(errlength))  
                                    new_coords3.loc[r2, 'from'] = from_
                                    to_= dist_func(size=1, loc= litho_to - float(errlength), scale= float(errlength))  
                                    new_coords3.loc[r2, 'to'] = to_
                                    
                                    collarID_ = lithology_df._get_value(r2,'collarId')
                                    fromX_ = lithology_df._get_value(r2,'fromX')
                                    fromY_ = lithology_df._get_value(r2,'fromY')
                                    fromZ_ = lithology_df._get_value(r2,'fromZ')
                                    layerId_ = lithology_df._get_value(r2,'layerId')
                                    toX_ = lithology_df._get_value(r2,'toX')
                                    toY_ = lithology_df._get_value(r2,'toY')
                                    toZ_ = lithology_df._get_value(r2,'toZ')
                                    propertyCode_ = lithology_df._get_value(r2,'propertyCode')
                                    property1_ = lithology_df._get_value(r2,'property1')
                                    property2_ = lithology_df._get_value(r2,'property2')
                                    unit_ = lithology_df._get_value(r2,'unit')
                                    new_coords3.loc[len(new_coords3.index)] = [collarID_, '0', from_,to_,fromX_,fromY_,fromZ_,'layerId_',toX_,toY_, toZ_,propertyCode_,property1_,property2_,unit] 
                              

                    prev_collarID = cur_collarId
                            

            if global_bool_drillhole == 'True' :
                file_name2 = 'global_drillhole_survey' + "_" + str(m) + ".csv"
                sorted_new_coords2 = new_coords2.sort_values(by=['collarId','depth'], ascending=[True,True])
                sorted_new_coords2.to_csv(output_location1 + '/'  +  file_name2, index=False)  
                drillhole_survey_perturbed_csv_loop3d(output_location1 , output_location2 , file_name2)

                file_name3 = 'global_drillhole_lithology' + "_" + str(m) + ".csv"
                sorted_new_coords3 = new_coords3.sort_values(by=['collarId','from'], ascending=[True,True])
                sorted_new_coords3.to_csv(output_location1 + '/'  +  file_name3, index=False)
                drillhole_lithology_perturbed_csv_loop3d(output_location1 , output_location2 , file_name3)
                
                
                
                
def interface_contact_perturbed_csv_loop3d(output_location1 , output_location2 , file_name):
    '''
        Function: generates perturbed interface contact csv files to loop3d files in the path given.
        Input:filename with path in output_location1.
        Output:loop3d file in output_location2
    '''
    
    intf_contact = pd.read_csv(output_location1 + '/' + file_name)
    file_name = file_name.strip('.csv')
    LF.CreateBasic(output_location2+ '/' + file_name + ".loop3d")
    #intf_contact = pd.read_csv(output_location1 + '/' + file_name)
    contactData  = np.zeros(intf_contact.shape[0] ,LF.contactObservationType)   
    contactData ['layerId'] = intf_contact['layerId']
    contactData ['easting'] = intf_contact['easting']
    contactData ['northing'] = intf_contact['northing']
    contactData['altitude'] = intf_contact['altitude']
    contactData['type'] = intf_contact['type']
    resp = LF.Set(output_location2 + '/' + file_name + ".loop3d",
                               "contacts",
                               data=contactData,
                               verbose=True)


    if resp["errorFlag"]:
        print(resp["errorString"])

    #use for testing or reading loop3d file
    #resp = LF.Get(output_location2 + '/' + file_name + ".loop3d","contacts")     
    #if resp['errorFlag']:
    #    df = pd.DataFrame()
    #else:
    #    df = pd.DataFrame.from_records(resp['value'],columns=['layerId','easting','northing','altitude','type'])       

    #print(df)   


def interface_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name):
    '''
        Function: generates perturbed interface fault csv files to loop3d files in the path given.
        Input:filename with path in output_location1.
        Output:loop3d file in output_location2
    '''
    intf_fault = pd.read_csv(output_location1 + '/' + file_name)
    file_name = file_name.strip('.csv')
    LF.CreateBasic(output_location2+ '/' + file_name + ".loop3d")
    contactData  = np.zeros(intf_fault.shape[0] ,LF.faultObservationType)   
    contactData ['eventId'] = intf_fault['eventId']
    contactData ['easting'] = intf_fault['easting']
    contactData ['northing'] = intf_fault['northing']
    contactData['altitude'] = intf_fault['altitude']
    contactData['type'] = intf_fault['type']
    contactData['dipDir'] = intf_fault['dipDir']
    contactData['dip'] = intf_fault['dip']
    contactData['dipPolarity'] = intf_fault['dipPolarity']
    contactData['val'] = intf_fault['val']
    contactData['displacement'] = intf_fault['displacement']
    contactData['posOnly'] = intf_fault['posOnly']
        
    resp = LF.Set(output_location2 + '/' + file_name + ".loop3d",
                               "faultObservations",
                               data=contactData,
                               verbose=True)


    if resp["errorFlag"]:
        print(resp["errorString"])

    #use for testing or reading loop3d file
    #resp = LF.Get(output_location2 + '/' + file_name + ".loop3d","faultObservations")     
    #if resp['errorFlag']:
    #    df = pd.DataFrame()
    #else:
    #    df = pd.DataFrame.from_records(resp['value'],columns=['eventId','easting','northing','altitude','type','dipDir','dip','dipPolarity','val','displacement','posOnly'])      

    #print(df)   
        
        
def orientaion_stratigraphy_perturbed_csv_loop3d(output_location1 , output_location2 , file_name):
    '''
        Function: generates perturbed orienation stratigraphy csv files to loop3d files in the path given.
        Input:filename with path in output_location1.
        Output:loop3d file in output_location2
    '''
    orienation_strati = pd.read_csv(output_location1 + '/' + file_name)
    file_name = file_name.strip('.csv')
    #print(file_name)
    LF.CreateBasic(output_location2+ '/' + file_name + ".loop3d")
        
    orientaionData  = np.zeros(orienation_strati.shape[0] ,LF.stratigraphicObservationType)   
    orientaionData ['layerId'] = orienation_strati['layerId']
    orientaionData ['easting'] = orienation_strati['easting']
    orientaionData ['northing'] = orienation_strati['northing']
    orientaionData['altitude'] = orienation_strati['altitude']
    orientaionData['type'] = orienation_strati['type']
    orientaionData['dipDir'] = orienation_strati['dipDir']
    orientaionData['dip'] = orienation_strati['dip']
    orientaionData['dipPolarity'] = orienation_strati['dipPolarity']
    orientaionData['layer'] = orienation_strati['layer']
        
        
    resp = LF.Set(output_location2 + '/' + file_name + ".loop3d",
                               "stratigraphicObservations",
                               data=orientaionData,
                               verbose=True)


    if resp["errorFlag"]:
        print(resp["errorString"])

    #use for testing or reading loop3d file
    #resp = LF.Get(output_location2 + '/' + file_name + ".loop3d","stratigraphicObservations")     
    #if resp['errorFlag']:
    #    df = pd.DataFrame()
    #else:
    #    df = pd.DataFrame.from_records(resp['value'],columns=['layerId', 'easting','northing','altitude','type','dipDir','dip','dipPolarity','layer'])  

        #print(df)   
        
        
def orienation_fault_perturbed_csv_loop3d(output_location1 , output_location2 , file_name):
    '''
        Function: generates perturbed orienation fault csv files to loop3d files in the path given.
        Input:filename with path in output_location1.
        Output:loop3d file in output_location2
    '''
    intf_fault = pd.read_csv(output_location1 + '/' + file_name)
    file_name = file_name.strip('.csv')
    LF.CreateBasic(output_location2+ '/' + file_name + ".loop3d")
    contactData  = np.zeros(intf_fault.shape[0] ,LF.faultObservationType)   
    contactData ['eventId'] = intf_fault['eventId']
    contactData ['easting'] = intf_fault['easting']
    contactData ['northing'] = intf_fault['northing']
    contactData['altitude'] = intf_fault['altitude']
    contactData['type'] = intf_fault['type']
    contactData['dipDir'] = intf_fault['dipDir']
    contactData['dip'] = intf_fault['dip']
    contactData['dipPolarity'] = intf_fault['dipPolarity']
    contactData['val'] = intf_fault['val']
    contactData['displacement'] = intf_fault['displacement']
    contactData['posOnly'] = intf_fault['posOnly']
        
    resp = LF.Set(output_location2 + '/' + file_name + ".loop3d",
                               "faultObservations",
                               data=contactData,
                               verbose=True)


    if resp["errorFlag"]:
        print(resp["errorString"])

    #use for testing or reading loop3d file
    #resp = LoopProjectFile.Get(output_location2 + '/' + file_name + ".loop3d","faultObservations")     
    #if resp['errorFlag']:
    #    df = pd.DataFrame()
    #else:
        #    df = pd.DataFrame.from_records(resp['value'],columns=['eventId','easting','northing','altitude','type','dipDir','dip','dipPolarity','val','displacement','posOnly'])       

    #print(df)   

def drillhole_collar_csv_loop3d(output_location1 , output_location2 , file_name):
    '''
        Function: generates collar csv file(one file only) to loop3d files in the path given.
        Input:filename with path in output_location1.
        Output:loop3d file in output_location2
    '''
    collar = pd.read_csv(output_location1 + '/' + file_name)
    file_name = file_name.strip('.csv')
    LF.CreateBasic(output_location2+ '/' + file_name + ".loop3d")
    
    drillholeData  = np.zeros(collar.shape[0] ,LF.drillholeDescriptionType)   
    drillholeData ['collarId'] = collar['collarId']
    drillholeData ['holeName'] = collar['holeName']
    drillholeData ['surfaceX'] = collar['surfaceX']
    drillholeData['surfaceY'] = collar['surfaceY']
    drillholeData['surfaceZ'] = collar['surfaceZ']
    resp = LF.Set(output_location2 + '/' + file_name + ".loop3d",
                               "drillholeLog",
                               data=drillholeData,
                               verbose=True)
    if resp["errorFlag"]:
        print(resp["errorString"])

    #resp = LoopProjectFile.Get(output_location2 + '/' + file_name + ".loop3d","drillholeLog")     
    #if resp['errorFlag']:
    #    df = pd.DataFrame()
    #else:
       #df = pd.DataFrame.from_records(resp['value'],columns=['collarId','holeName','surfaceX','surfaceY','surfaceZ'])       

    #print(df)


def drillhole_survey_perturbed_csv_loop3d(output_location1 , output_location2 , file_name):
    '''
        Function: generates perturbed survay csv files to loop3d files in the path given.
        Input:filename with path in output_location1.
        Output:loop3d file in output_location2
    '''
    survey = pd.read_csv(output_location1 + '/' + file_name)
    file_name = file_name.strip('.csv')
    LF.CreateBasic(output_location2+ '/' + file_name + ".loop3d")
    
    drillholeData  = np.zeros(survey.shape[0] ,LF.drillholeSurveyType)   
    drillholeData ['collarId'] = survey['collarId']
    drillholeData ['depth'] = survey['depth']
    drillholeData ['angle1'] = survey['angle1']
    drillholeData['angle2'] = survey['angle2']
    drillholeData['unit'] = survey['unit']
    resp = LF.Set(output_location2 + '/' + file_name + ".loop3d",
                               "drillholeSurveys",
                               data=drillholeData,
                               verbose=True)
    if resp["errorFlag"]:
        print(resp["errorString"])

    #use for testing or reading loop3d file
    #resp = LoopProjectFile.Get(output_location2 + '/' + file_name + ".loop3d,"drillholeSurveys")     
    #if resp['errorFlag']:
 #      df = pd.DataFrame()
    #else:
#       df = pd.DataFrame.from_records(resp['value'],columns=['collarId','depth','angle1','angle2','unit'])

    #print(df)



def drillhole_lithology_perturbed_csv_loop3d(output_location1 , output_location2 , file_name):
    '''
        Function: generates perturbed lithology csv files to loop3d files in the path given.
        Input:filename with path in output_location1.
        Output:loop3d file in output_location2
    '''
    lithology1 = pd.read_csv(output_location1 + '/' + file_name)
    file_name = file_name.strip('.csv')
    LF.CreateBasic(output_location2+ '/' + file_name + ".loop3d")


    drillholeData2  = np.zeros(lithology1.shape[0] ,LF.drillholeObservationType)   
    drillholeData2['collarId'] = lithology1['collarId']
    drillholeData2['from'] =  lithology1['from']
    drillholeData2['to'] = lithology1['to']
    drillholeData2['fromX'] = lithology1['fromX']
    drillholeData2['fromY'] = lithology1['fromY']
    drillholeData2['fromZ'] = lithology1['fromZ']
    drillholeData2['layerId']  = lithology1['layerId']
    drillholeData2['toX'] = lithology1['toX']
    drillholeData2['toY'] = lithology1['toY']
    drillholeData2['toZ'] =  lithology1['toZ']
    drillholeData2['propertyCode'] = lithology1['propertyCode']
    drillholeData2['property1'] = lithology1['property1']
    drillholeData2['property2'] =lithology1['property2']
    drillholeData2['unit'] =lithology1['unit']

    resp = LF.Set(output_location2 + '/' + file_name + ".loop3d",
                               "drillholeObservations",
                               data=drillholeData2,
                               verbose=True)
    if resp["errorFlag"]:
        print(resp["errorString"])
    
    #use for testing or reading loop3d file
    #resp = LoopProjectFile.Get(output_location2 + '/' + file_name + ".loop3d,"drillholeObservations")     
    #if resp['errorFlag']:
#       df = pd.DataFrame()
    #else:
    #    df = pd.DataFrame.from_records(resp['value'],columns=['collarId','from','to','fromX','fromY','fromZ','layerId','toX','toY','toZ','propertyCode','property1','property2','unit'])

    #print(df)






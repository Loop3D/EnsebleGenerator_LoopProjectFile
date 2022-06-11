import LoopProjectFile
import configparser
import pandas as pd
from configparser import SafeConfigParser
from Perturb import interface_perturb,orient_perturb,drillhole_perturb,square 
from ast import literal_eval
import numpy as np
from map2loop.map2loop.m2l_export import  export_to_projectfile



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


config = configparser.ConfigParser()
config.read('Config_perturb.ini')

path_to_perturb_models = config['COMMON']['path_to_perturb_models']
path_to_perturb_models = path_to_perturb_models.replace(',', '').replace('\'', '')
with open(path_to_perturb_models + 'Config_perturb.ini', 'w') as configfile:
    config.write(configfile)


count = 0
for section_name in config.sections():
    if 'Parameter' in section_name:
        count  =  count + 1


new_coords = pd.DataFrame(columns=['path_to_m2l_files',
'path_to_perturb_models',
'netcdf_file_name',
'egen_runs',
'dem',
'source_geomodeller',

'intf_contact_grp_type' ,
'intf_fault_grp_type' ,
'ori_strati_grp_type' ,
'ori_fault_grp_type' ,

'global_intf_contact_grp_type',
'global_intf_fault_grp_type',
'global_ori_strati_grp_type',
'global_ori_fault_grp_type',

'local_bool_intf_contact_grp_type',
'local_bool_intf_fault_grp_type',
'local_bool_ori_strati_grp_type',
'local_bool_ori_fault_grp_type',

'intf_contact_layerid',
'intf_faultobservations_eventid',
'ori_strati_layerid',
'ori_fault_eventid',

'netcdf_file_name1',
'netcdf_file_name2',
'netcdf_file_name3',

'drillhole_grp_type1',
'drillhole_grp_type2' ,
'drillhole_grp_type3',

'global_bool_drillhole',
'local_bool_drillhole',
'drillhole_collarid',

'erraz',
'errdip',
'errlength',


'distribution',
'loc_distribution',
'error_gps',
'kappa',
'perturb'
])  



for section_name in config.sections():
    if 'Parameter' in section_name:
        #print(' Options:', config.options(section_name))
        for name, value in config.items(section_name):
            #print ('  %s = %s' % (name, value))
            if  name in new_coords.columns:
                new_coords.at[count, name] = value 
                       
        for name1,value1 in config.items('COMMON'):
            if  name1 in new_coords.columns:
                new_coords.at[count, name1] = value1

        count = count -1 

#path_to_m2l_files = config['COMMON']['path_to_m2l_files']
#path_to_m2l_files = path_to_m2l_files.replace(',', '').replace('\'', '')
#print(path_to_m2l_files)

#dh2loop = config['COMMON']['dh2loop']
#dh2loop = dh2loop.replace(',', '').replace('\'', '')
#print(dh2loop)
#string_val = config.get('COMMON', 'path_to_perturb_models')
#print(string_val)


#csv file
filename = 'Parameter' + ".csv"
new_coords = new_coords.fillna('AB')
new_coords.to_csv('Parameter.csv',index = False)


#new_coords = new_coords.fillna('AB')

#print(new_coords)


#response = LoopProjectFile.Get('bh.loop3d','drillholeObservations')
#if response["errorFlag"]: 
 #   print(response["errorString"])
##   print(response["value"])
    




for index,x in new_coords.iterrows():
    perturb = str(x.perturb).replace(',', '').replace('\'', '')
    if perturb == 'interface':
        print('interface')
        Mod_DF = new_coords.apply(lambda x:interface_perturb(x.path_to_m2l_files,
        x.path_to_perturb_models,
        x.netcdf_file_name,
        x.egen_runs,
        x.dem,
        x.source_geomodeller,

        x.intf_contact_grp_type ,
        x.intf_fault_grp_type ,
       

        x.global_intf_contact_grp_type,
        x.global_intf_fault_grp_type,
       

        x.local_bool_intf_contact_grp_type,
        x.local_bool_intf_fault_grp_type,
       

        x.intf_contact_layerid,
        x.intf_faultobservations_eventid,
       


        x.distribution,
        x.error_gps,
        x.perturb ), 
        axis=1)

    elif perturb == 'orient':
       print('orient')
       Mod_DF = new_coords.apply(lambda x:orient_perturb(x.path_to_m2l_files,
       x.path_to_perturb_models,
       x.netcdf_file_name,
       x.egen_runs,
       x.dem,
       x.source_geomodeller,

        
       x.ori_strati_grp_type ,
       x.ori_fault_grp_type ,

       
       x.global_ori_strati_grp_type,
       x.global_ori_fault_grp_type,

        
       x.local_bool_ori_strati_grp_type,
       x.local_bool_ori_fault_grp_type,

       
       x.ori_strati_layerid,
       x.ori_fault_eventid,


       x.distribution,
       x.loc_distribution,
       x.error_gps,
       x.kappa,
       x.perturb ), 
       axis=1)

       

    if perturb == 'drillhole':
        print('drillhole')
        Mod_DF = new_coords.apply(lambda x:drillhole_perturb(x.path_to_m2l_files,
        x.path_to_perturb_models,
        x.netcdf_file_name1,
        x.netcdf_file_name2,
        x.netcdf_file_name3,
        x.egen_runs,
        x.dem,
        x.source_geomodeller,

        x.drillhole_grp_type1 ,
        x.drillhole_grp_type2 ,
        x.drillhole_grp_type3,

        x.global_bool_drillhole,
        x.local_bool_drillhole,
        x.drillhole_collarid,

        x.distribution,
        x.erraz,
        x.errdip,
        x.errlength,
        x.perturb ), 
        axis=1)

###create loop3d file..working
#LoopProjectFile.CreateBasic("dh2loop_Observation.loop3d")
#lithology = pd.read_csv("lithology.csv")
#drillholeData  = np.zeros(lithology.shape[0] ,LoopProjectFile.drillholeObservationType)   #drillholeLog)   #.drilllholeDescriptionType)
#drillholeData ['collarId'] = lithology['CollarID']
#drillholeData ['from'] = lithology['FromDepth']
#drillholeData ['to'] = lithology['ToDepth']
#drillholeData['fromX'] = lithology['fromX']
#drillholeData['fromX'] = lithology['fromX']
#drillholeData['fromX'] = lithology['fromZ']
#drillholeData['layerId']  = lithology['layerId']
#drillholeData['toX'] = lithology['toX']
#drillholeData['toY'] = lithology['toY']
#drillholeData['toX'] = lithology['toX']
#drillholeData['propertyCode'] = lithology['propertyCode']
#drillholeData['property1'] = lithology['property1']
#drillholeData['property2'] =lithology[' property2']
#drillholeData['unit'] =unit

#LoopProjectFile.CreateBasic("dh2loop_Description.loop3d")
#collar = pd.read_csv("collar.csv")
#drillholeData  = np.zeros(collar.shape[0] ,LoopProjectFile.drillholeDescriptionType)   #drillholeLog)   #.drilllholeDescriptionType)
#drillholeData ['collarId'] = collar['CollarID']
#drillholeData ['holeName'] = collar['HoleId']
#drillholeData ['surfaceX'] = collar['Longitude']
#drillholeData['surfaceY'] = collar['Latitude']
#drillholeData['surfaceZ'] = collar['RL']
#resp = LoopProjectFile.Set('dh2loop_Description.loop3d',
#                               "drillholeLog",
#                               data=drillholeData,
#                               verbose=True)
#if resp["errorFlag"]:
#    print(resp["errorString"])

#resp = LoopProjectFile.Get('dh2loop_Description.loop3d',"drillholeLog")     #dh2loop_Survey.loop3d',"drillholeSurveys")
#if resp['errorFlag']:
#    df = pd.DataFrame()
#else:
#    df = pd.DataFrame.from_records(resp['value'],columns=['collarId','holeName','surfaceX','surfaceY','surfaceZ'])       #'depth','angle1','angle2','unit'])

#print(df)





#LoopProjectFile.CreateBasic("dh2loop_Survey.loop3d")
#survey = pd.read_csv("survey.csv")
#drillholeData  = np.zeros(survey.shape[0] ,LoopProjectFile.drillholeSurveyType)   #drillholeLog)   #.drilllholeDescriptionType)
#drillholeData ['collarId'] = survey['CollarID']
#drillholeData ['depth'] = survey['Depth']
#drillholeData ['angle1'] = survey['Azimuth']
#drillholeData['angle2'] = survey['Inclination']
#drillholeData['unit'] = survey['unit']
#resp = LoopProjectFile.Set('dh2loop_Survey.loop3d',
#                               "drillholeSurveys",
#                               data=drillholeData,
#                               verbose=True)
#if resp["errorFlag"]:
#    print(resp["errorString"])

#resp = LoopProjectFile.Get('dh2loop_Survey.loop3d',"drillholeSurveys")     #dh2loop_Survey.loop3d',"drillholeSurveys")
#if resp['errorFlag']:
#    df = pd.DataFrame()
#else:
#    df = pd.DataFrame.from_records(resp['value'],columns=['collarId','depth','angle1','angle2','unit'])

#print(df)


#LoopProjectFile.CreateBasic("dh2loop_Observation_.loop3d")
#lithology1 = pd.read_csv("lithology.csv")
#print(lithology1)
#drillholeData2  = np.zeros(lithology1.shape[0] ,LoopProjectFile.drillholeObservationType)   #drillholeLog)   #.drilllholeDescriptionType)
#drillholeData2['collarId'] = lithology1['CollarID']
#drillholeData2['from'] =  lithology1['FromDepth']
#drillholeData2['to'] = lithology1['ToDepth']
#drillholeData2['fromX'] = lithology1['fromX']
#drillholeData2['fromY'] = lithology1['fromY']
#drillholeData2['fromZ'] = lithology1['fromZ']
#drillholeData2['layerId']  = lithology1['layerId']
#drillholeData2['toX'] = lithology1['toX']
#drillholeData2['toY'] = lithology1['toY']
#drillholeData2['toZ'] =  lithology1['toZ']
#drillholeData2['propertyCode'] = lithology1['propertyCode']
#drillholeData2['property1'] = lithology1['property1']
#drillholeData2['property2'] =lithology1['property2']
#drillholeData2['unit'] =lithology1['unit']

#resp = LoopProjectFile.Set('dh2loop_Observation_.loop3d',
#                               "drillholeObservations",
#                               data=drillholeData2,
#                               verbose=True)
#if resp["errorFlag"]:
#    print(resp["errorString"])

#resp = LoopProjectFile.Get('dh2loop_Observation_.loop3d',"drillholeObservations")     #dh2loop_Survey.loop3d',"drillholeSurveys")
#if resp['errorFlag']:
#    df = pd.DataFrame()
#else:
#    df = pd.DataFrame.from_records(resp['value'],columns=['collarId','from','to','fromX','fromY','fromZ','layerId','toX','toY','toZ','propertyCode','property1','property2','unit'])

#print(df)











#LoopProjectFile.Set("dh2loop.loop3d", "drillholeLog", data=drillholeData)
#export_to_projectfile('dh2loop',path_to_m2l_files,dh2loop,bbox,0)

#config_perturb['COMMON']['path_to_perturb_models']

#loopFilename, tmp_path, output_path, bbox, proj_crs, overwrite=False):


#Mod_DF = new_coords[mask1].apply(lambda x: interface_perturb(*x),axis=1)

#new_coords[mask1].apply(interface_perturb,axis=1)
#new_coords[mask2].apply(orient_perturb,axis=1)

#(dfs['col'].apply(foo)&dfs['col'].apply(bar))&dfs['col'].apply(baz)

#mask1 = (new_coords['perturb']=='interface')
#mask2 = (new_coords['perturb']=='orient')

#new_coords[mask1].apply(interface_perturb,axis=1) | new_coords[mask2].apply(orient_perturb,axis=1)


#df2 = df.apply(lambda x: np.square(x) if x.name in ['A','B'] else x)

#df2 = new_coords.apply(lambda x: interface_perturb(x) if x.name in ['perturb']== 'interafce' else new_coords.apply(orient_perturb(x)  ))

#df2 = new_coords.apply(lambda x: interface_perturb(x) if x.name in ['perturb']== 'interafce' else x)

#df2 = new_coords.apply(lambda x: interface_perturb(x) if x.name in new_coords.['perturb']== 'interafce' else x)

###
#df = pd.DataFrame({'A': [1, 2], 'B': [10, 20],'C': [10, 20],'D': [10, 20],'E': [10, 20],'F': [10, 20],'G': [10, 20],
#'H': [10, 20],'I': [10, 20],'J': [10, 20],'K': [10, 20],'L': [10, 20],'M': [10, 20],'N': [10, 20],'N1': ['k1','k1'],'o': [10, 20]})
#mask11 = (df['N1']=='k1')
#df1 = mask11.apply(square,axis=1)
#print(df)
#print(df1)




#new_coords.apply(interface_perturb,args=(('path_to_m2l_files','path_to_perturb_models','netcdf_file_name','egen_runs','dem','source_geomodeller','intf_contact_grp_type' ,
#'intf_fault_grp_type','ori_strati_grp_type' ,'ori_fault_grp_type' ,'global_intf_contact_grp_type','global_intf_fault_grp_type','global_ori_strati_grp_type',
#'global_ori_fault_grp_type','local_bool_intf_contact_grp_type','local_bool_intf_fault_grp_type','local_bool_ori_strati_grp_type','local_bool_ori_fault_grp_type',
#'intf_contact_layerid','intf_faultobservations_eventid','ori_strati_layerid','ori_fault_eventid','distribution','loc_distribution','error_gps','kappa','perturb'),),axis=1)


#new_coords.apply(lambda x: interface_perturb(*x), axis=1)




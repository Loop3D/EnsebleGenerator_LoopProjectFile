
import configparser
import pandas as pd
from configparser import SafeConfigParser
from  Perturb import interface_perturb,orient_perturb,square
from ast import literal_eval

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

#csv file
filename = 'Parameter' + ".csv"
new_coords.to_csv('Parameter.csv',index = False)

#new_coords1 = new_coords.replace('NaN',"AB")
new_coords = new_coords.fillna('AB')

#print(new_coords)




for index,x in new_coords.iterrows():
    perturb = str(x.perturb).replace(',', '').replace('\'', '')
    if perturb == 'interface':
        #print('inside')
        Mod_DF = new_coords.apply(lambda x:interface_perturb(x.path_to_m2l_files,
        x.path_to_perturb_models,
        x.netcdf_file_name,
        x.egen_runs,
        x.dem,
        x.source_geomodeller,

        x.intf_contact_grp_type ,
        x.intf_fault_grp_type ,
        x.ori_strati_grp_type ,
        x.ori_fault_grp_type ,

        x.global_intf_contact_grp_type,
        x.global_intf_fault_grp_type,
        x.global_ori_strati_grp_type,
        x.global_ori_fault_grp_type,

        x.local_bool_intf_contact_grp_type,
        x.local_bool_intf_fault_grp_type,
        x.local_bool_ori_strati_grp_type,
        x.local_bool_ori_fault_grp_type,

        x.intf_contact_layerid,
        x.intf_faultobservations_eventid,
        x.ori_strati_layerid,
        x.ori_fault_eventid,


        x.distribution,
        x.loc_distribution,
        x.error_gps,
        x.kappa,
        x.perturb ), 
        axis=1)




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




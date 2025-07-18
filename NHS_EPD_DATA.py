#!/usr/bin/env python
# coding: utf-8

# ## NHS_EPD_DATA
# 
# 
# 

# In[ ]:


import urllib3
import json
import pandas as pd
import re
import warnings
import urllib.parse

warnings.simplefilter("ignore", category=UserWarning)


# In[ ]:


# Initialize urllib3
http = urllib3.PoolManager()


# In[ ]:


base_endpoint = 'https://opendata.nhsbsa.net/api/3/action/'
package_list_method = 'package_list'
package_show_method = 'package_show?id='
action_method = 'datastore_search_sql?'


# In[ ]:


datasets_response = requests.get(base_endpoint +  package_list_method).json()


# In[ ]:


print(datasets_response['result'])


# In[ ]:


dataset_id = "english-prescribing-data-epd"


# In[ ]:


resource_name = 'EPD_202401' # For EPD resources are named EPD_YYYYMM
pco_code = '13T00' # Newcastle Gateshead CCG
bnf_chemical_substance = '0407010H0' # Paracetamol


# In[ ]:


single_month_query = "SELECT * " \
                     f"FROM `{resource_name}` " \
                     f"WHERE pco_code = '{pco_code}' " \
                     f"AND bnf_chemical_substance = '{bnf_chemical_substance}'"


# In[ ]:


single_month_api_call = f"{base_endpoint}" \
                        f"{action_method}" \
                        "resource_id=" \
                        f"{resource_name}" \
                        "&" \
                        "sql=" \
                        f"{urllib.parse.quote(single_month_query)}" # Encode spaces in the url


# In[ ]:


single_month_response = requests.get(single_month_api_call).json()


# In[ ]:


single_month_df  = pd.json_normalize(single_month_response['result']['result']['records'])


# In[ ]:


single_month_df.head()


# In[ ]:


metadata_repsonse  = requests.get(f"{base_endpoint}" \
                                  f"{package_show_method}" \
                                  f"{dataset_id}").json()


# In[ ]:


resources_table  = pd.json_normalize(metadata_repsonse['result']['resources'])


# In[ ]:


resource_name_list = resources_table[resources_table['name'].str.contains('2024')]['name']


# In[ ]:


for_loop_df = pd.DataFrame()



# In[ ]:


for month in resource_name_list:
    
    # Build temporary SQL query
    tmp_query = "SELECT * " \
                f"FROM `{month}` " \
                f"WHERE pco_code = '{pco_code}' " \
                f"AND bnf_chemical_substance = '{bnf_chemical_substance}'"
    
    # Build temporary API call
    tmp_api_call  = f"{base_endpoint}" \
                    f"{action_method}" \
                    "resource_id=" \
                    f"{month}" \
                    "&" \
                    "sql=" \
                    f"{urllib.parse.quote(tmp_query)}" # Encode spaces in the url
    
    # Grab the response JSON as a temporary list
    tmp_response = requests.get(tmp_api_call).json()
    
    # Extract records in the response to a temporary dataframe
    tmp_df = pd.json_normalize(tmp_response['result']['result']['records'])
    
    # Bind the temporary data to the main dataframe
    for_loop_df = pd.concat([for_loop_df, tmp_df], ignore_index=True)


# In[ ]:


# Grab the response JSON as a temporary list
tmp_response = requests.get(tmp_api_call).json()


# In[ ]:


# Extract records in the response to a temporary dataframe
tmp_df = pd.json_normalize(tmp_response['result']['result']['records'])


# In[ ]:


# Bind the temporary data to the main dataframe
for_loop_df = pd.concat([for_loop_df, tmp_df], ignore_index=True)


# In[ ]:


for_loop_df.head()


# In[ ]:


for_loop_df.to_csv('nhs_epd_2024_data.csv')


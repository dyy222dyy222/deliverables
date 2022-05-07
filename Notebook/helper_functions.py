from placekey.api import PlacekeyAPI
from ast import literal_eval

import re
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


def clean_special_char(df,col:str):
    from string import punctuation
    import re
    # punctuation -> space
    remove_punctiuation = str.maketrans(punctuation, ' '*len(punctuation))
    df[col] =  df[col].apply(lambda x: x.translate(remove_punctiuation))
    # several spaces -> one space
    df[col] =  df[col].apply(lambda x: re.sub(' +', ' ', x))
    return df

def get_splited_addr(df,col:str,lt=False):
    import usaddress
    global address
    try:
        address = usaddress.tag(df[col])
    except:
        return None
    if lt:
        return list(address[0])
    else:
        return address
def get_comb_addr(df,splited_col:str):
    test = df[splited_col].to_list()
    values= []
    for i in range(len(test)):
        try:
            values.append(test[i][0])
        except:
            values.append({})
    splited_df = pd.DataFrame(values)
    df.reset_index(inplace=True)
    addr_final = pd.concat([df,splited_df],axis=1)
    del addr_final['index']
    return addr_final


def get_str_replace(MLS_ADDRESS_final,DT_ADDRESS_final):
    #need to change file path
    df = pd.read_csv('/Users/yiweihou/Downloads/Roc360/suffix_abbreviations_converted.csv')
    StreetNamePostType_dict = dict(zip(df.common_name, df.abbr))
    StreetNameDirectional_dict = {'west': 'w', 'south': 's', 'north': 'n','east': 'e'}

    DT_ADDRESS_final.replace({"StreetNamePreDirectional": StreetNameDirectional_dict},inplace=True)
    DT_ADDRESS_final.replace({"StreetNamePostType": StreetNamePostType_dict},inplace=True)

    MLS_ADDRESS_final.replace({"StreetNamePreDirectional": StreetNameDirectional_dict},inplace=True)
    MLS_ADDRESS_final.replace({"StreetNamePostType": StreetNamePostType_dict},inplace=True)
    return MLS_ADDRESS_final,DT_ADDRESS_final

# adding required placekey api field
def get_standard_df(df,column_map: dict, country:str, city:str, region:str):
    # rename column name --> need to be consistant with standatdized api field name
    # see this link: https://docs.placekey.io/#350ed3a9-68db-4c47-9e20-19b430cb9ef1
    df_standard = df.rename(columns=column_map)
    
    # adding other fields
    df_standard['query_id'] = range(1, 1+len(df))
    df_standard['iso_country_code'] = country
    df_standard['city'] = city
    df_standard['region'] = region
    return df_standard

def get_placekey_df(pk_api,stad_df, place_keys:tuple):
    # stad_df is the df containing standatdized api fields
    import json
    # convert query_id to str
    stad_df = stad_df.astype({"query_id": str})
    
    # dump df to json-format with selected key-value pairs (only keep values in specified place_keys)
    data_jsoned = json.loads(stad_df.to_json(orient="records"))
    dict_filter = lambda x, y: dict([ (i,x[i]) for i in x if i in set(y) ])
    result_dict=[dict_filter(i, place_keys) for i in data_jsoned]
    
    #request placekey api (need to test with default vars)
    responses = pk_api.lookup_placekeys(result_dict, verbose=True)
    df_placekeys = pd.read_json(json.dumps(responses), dtype={'query_id':str})
    
    # TODO: need to handle missing / errors

    return df_placekeys
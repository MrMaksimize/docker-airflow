"""_jobs file for public art."""
import os
import pandas as pd
import geopandas as gpd
import logging
from trident.util import general
import requests
from arcgis import GIS
from arcgis.features import FeatureLayerCollection

conf = general.config
prod_dir = conf['prod_data_dir']
prod_file = conf['prod_data_dir'] + '/public_art_locations_datasd_v1.csv'
temp_file = conf['temp_data_dir'] + '/public_art_assets.csv'


def get_public_art():
    """Gets art pieces from NetX, creates prod file."""
    
    url_categories = 'https://sdartsandculture.netx.net/json/category/9'
    categories_ids = []

    logging.info('Getting netx categories list.')
    r_categories = requests.get(url_categories)

    if r_categories.status_code == 200:

        categories_details = r_categories.json()
        logging.info('Get ids for cats if the cat has contents.')
        
        for cat in categories_details:
            if (cat['hasContents']):
                categories_ids.append(cat['categoryid'])
            else:
                if (len(cat['children']) > 0):
                    url_children = f"https://sdartsandculture.netx.net/json/category/{cat['categoryid']}"
                    r_children = requests.get(url_children)

                    if r_children.status_code == 200:
                        children_details = r_children.json()
                        for children in children_details:
                            if (children['hasContents']):
                                categories_ids.append(children['categoryid'])
                    else:
                        return f"child request {cat} failed {r_children.status_code}"
        
        assets_details = []

        logging.info('Getting assets by category id.')

        for i in categories_ids:
            
            url_category = f'https://sdartsandculture.netx.net/json/list/category/id/{i}'
            r_category = requests.get(url_category)

            if r_category.status_code == 200:

                assets = r_category.json()
                asset_len = len(assets)
                logging.info('Found '+str(asset_len)+' assets')
                
                for a in range(asset_len):
                    if (assets[a]['assetId']):
                        url_asset = f"https://sdartsandculture.netx.net/json/asset/{assets[a]['assetId']}"
                        r_asset = requests.get(url_asset)

                        if r_asset.status_code == 200:

                            asset_result = r_asset.json()
                            attribute_list = asset_result['attributeNames']
                            attribute_values = asset_result['attributeValues']
                            accession = attribute_values[0]
                            asset_df = pd.DataFrame(data=attribute_values,index=attribute_list,columns=[accession])
                            assets_details.append(asset_df)

                        else:

                            return f"Attribute request {a} failed {r_asset.status_code}"

            else:

                return f"Asset request {i} failed {r_category.status_code}"


        logging.info('Processing all assets into prod file')

        all_assets = pd.concat(assets_details,axis=1)
        all_assets = all_assets.transpose()

        general.pos_write_csv(
            all_assets, temp_file)

    else:

        return f"Categories request failed {r_categories.status_code}"

    return "Successfully extracted public art"

def process_public_art():
    """ Getting API results and processing """

    df = pd.read_csv(temp_file)

    latitudes = pd.to_numeric(df['Latitude'], errors='coerce')
    longitudes = pd.to_numeric(df['Longitude'], errors='coerce')
    coordinates = df.assign(latitude_float=latitudes,longitude_float=longitudes)
    df_geo = coordinates[coordinates['latitude_float'].notnull()]

    # Dept wanted to remove most columns
    final = df_geo[['Accession Number',
    'Status',
    'Artwork Title',
    'Artist',
    'Location',
    'latitude_float',
    'longitude_float']]
    
    final = final.rename(columns={
        'Accession Number':'accession_number',
        'Status':'status',
        'Artwork Title':'artwork_title',
        'Artist':'artist',
        'Location':'location',
        'latitude_float':'lat',
        'longitude_float':'lng',
    })
    
    final = final.drop_duplicates('accession_number')

    artists_nop = final['artist'].str.replace('\n','')
    artists_notab = artists_nop.str.replace('\t','')
    final['artist'] = artists_notab

    general.pos_write_csv(
        final, prod_file)

    return "Successfully processed public art"

def update_geospatial():
    """ Converting CSV to geospatial formats """

    df = pd.read_csv(prod_file)

    conn = BaseHook.get_connection(conn_id="ARC_ONLINE")

    arc_gis = GIS(conn.host,conn.login,conn.password)
    lyr_id = '62be6801a23c4170851772d6a8184020'
    feature_layer = arc_gis.content.get(lyr_id)
    flayer_collection = FeatureLayerCollection.fromitem(feature_layer)
    logging.info("Overwriting public art feature layer collection")
    overwrite = flayer_collection.manager.overwrite(prod_file)

    return overwrite

#def shp_to_kml():
    #cmd = shp2kml(f"{conf['prod_data_dir']}/public_art_gis_kml")
    #logging.info(cmd)
    #return cmd

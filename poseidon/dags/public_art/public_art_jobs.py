"""_jobs file for public art."""
import os
import pandas as pd
import logging
from poseidon.util import general
import requests

conf = general.config
prod_file = conf['prod_data_dir'] + '/public_art_locations_datasd.csv'


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
                    url_children = 'https://sdartsandculture.netx.net/json/category/{}'.format(str(cat['categoryid']))
                    r_children = requests.get(url_children)

                    if r_children.status_code == 200:
                        children_details = r_children.json()
                        for children in children_details:
                            if (children['hasContents']):
                                categories_ids.append(children['categoryid'])
                    else:
                        return "child request {} failed {}".format(cat, str(r_children.status_code))
        
        assets_details = []

        logging.info('Getting assets by category id.')

        for i in categories_ids:
            
            url_category = 'https://sdartsandculture.netx.net/json/list/category/id/{}'.format(str(i))
            r_category = requests.get(url_category)

            if r_category.status_code == 200:

                assets = r_category.json()
                asset_len = len(assets)
                logging.info('Found '+str(asset_len)+' assets')
                
                for a in range(asset_len):
                    if (assets[a]['assetId']):
                        url_asset = 'https://sdartsandculture.netx.net/json/asset/{}'.format(str(assets[a]['assetId']))
                        r_asset = requests.get(url_asset)

                        if r_asset.status_code == 200:

                            asset_result = r_asset.json()
                            attribute_list = asset_result['attributeNames']
                            attribute_values = asset_result['attributeValues']
                            accession = attribute_values[0]
                            asset_df = pd.DataFrame(data=attribute_values,index=attribute_list,columns=[accession])
                            assets_details.append(asset_df)

                        else:

                            return "Attribute request {} failed {}".format(a,str(r_asset.status_code))

            else:

                return "Asset request {} failed {}".format(i,str(r_category.status_code))


        logging.info('Processing all assets into prod file')

        all_assets = pd.concat(assets_details,axis=1)
        all_assets = all_assets.transpose()
        latitudes = pd.to_numeric(all_assets['Latitude'], errors='coerce')
        longitudes = pd.to_numeric(all_assets['Longitude'], errors='coerce')
        all_assets_coordinates = all_assets.assign(latitude_float=latitudes,longitude_float=longitudes)
        all_assets_geo = all_assets_coordinates[all_assets_coordinates['latitude_float'].notnull()]

        # Dept wanted to remove most columns
        all_assets_cols = all_assets_geo[['Accession Number',
        'Status',
        'Artwork Title',
        'Artist',
        'Location',
        'latitude_float',
        'longitude_float']]
        
        all_assets_rename = all_assets_cols.rename(columns={
            'Accession Number':'accession_number',
            'Status':'status',
            'Artwork Title':'artwork_title',
            'Artist':'artist',
            'Location':'location',
            'latitude_float':'latitude',
            'longitude_float':'longitude',
        })
        
        all_assets_final = all_assets_rename.drop_duplicates('accession_number')

        artists_nop = all_assets_final['artist'].str.replace('\n','')
        artists_notab = artists_nop.str.replace('\t','')
        all_assets_final['artist'] = artists_notab

        general.pos_write_csv(
            all_assets_final, prod_file)

    else:

        return "Categories request failed {}".format(str(r_categories.status_code))

    return "Successfully extracted public art"
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  8 14:02:19 2023

@author: gabri
"""

import ee

# try to initalize an ee sessio n
# if not authenticated then run auth workflow and initialize
# try:
ee.Initialize()
# except:
#     ee.Authenticate()
#     ee.Initialize()

import sqlalchemy    
from sqlalchemy import create_engine   

import eemont
import pandas as pd
import fnmatch
import numpy as np
from datetime import date
import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point,Polygon
import numpy as np
from functools import reduce
import warnings
warnings.filterwarnings('ignore')
import psycopg2
import datetime
from datetime import time, timedelta, date
import time
from datetime import datetime
import ee
import geemap
import os, glob
#import rpy2.robjects as robjects
import sys
import json
import geojson
from dateutil.relativedelta import relativedelta
   
user = 'ferraz'
password = '3ino^Vq3^R1!'
host = 'vps40890.publiccloud.com.br'
port = 5432
database = 'carbon'

global engine
engine = create_engine(
        url="postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )

   
def get_car(codCAR):
    uf = codCAR[0:2].lower()
    
    q = "select st_asgeojson(geom) as geom from car.area_imovel_{0} where cod_imovel  = '{1}'".format(uf, codCAR)
    
    geom = pd.read_sql_query(q,con=engine)
    gdf = gpd.read_file(geom['geom'][0], driver='GeoJSON')
    geo_json = gdf.to_json()
    j = json.loads(geo_json)
    CAR  = ee.FeatureCollection(j)
    return CAR

def get_temperature_data(CAR, start_date, end_date):
        temp = ee.ImageCollection('NOAA/CFSR').select("Temperature_surface").filterBounds(CAR).filterDate(start_date, end_date)

        temp_mask = temp.map(mask_image)

        ts_temp = temp_mask.getTimeSeriesByRegion(reducer = [ee.Reducer.mean()],
                                      geometry = CAR.geometry(),
                                      bands = ['Temperature_surface'],
                                      scale = 100)

        pd_ts_temp= geemap.ee_to_pandas(ts_temp)
        pd_ts_temp['Temperatura'] = pd_ts_temp['Temperature_surface'] - 273.15
        pd_ts_temp['Data']= pd.to_datetime(pd_ts_temp['date'].astype(str), format='%Y/%m/%d').dt.date
        pd_ts_temp_mean = pd_ts_temp.groupby(['Data']).mean().reset_index()
        pd_ts_temp_mean['Data'] = pd.to_datetime(pd_ts_temp_mean.Data)
        pd_ts_temp_mean['MM-DD'] = pd_ts_temp_mean['Data'].dt.strftime('%m;%d')
        return pd_ts_temp_mean
   
def get_precipitation_data(start_date, end_date, CAR, lat_long):
    ppt = ee.ImageCollection("JAXA/GPM_L3/GSMaP/v6/operational").select("hourlyPrecipRate").filterBounds(CAR).filterDate(start_date, end_date)
    point = ee.Geometry.Point(lat_long.getInfo()['coordinates'])#.buffer(20000)

    ts_ppt = ppt.getTimeSeriesByRegion(reducer = [ee.Reducer.sum()],
                                  geometry = point,
                                  bands = 'hourlyPrecipRate',
                                  scale = 100)

    tsPandas = geemap.ee_to_pandas(ts_ppt)
    return tsPandas#, ppt

def get_precipitation_sum(pd_ts_ppt):
    pd_ts_ppt['Data'] = pd.to_datetime(pd_ts_ppt['date'].astype(str)).dt.date
    pd_ts_ppt_sum = pd_ts_ppt.groupby(['Data']).sum()

    pd_ts_ppt_sumcum = pd_ts_ppt_sum.cumsum().reset_index()
    pd_ts_ppt_sumcum['Data'] = pd.to_datetime(pd_ts_ppt_sumcum.Data)
    pd_ts_ppt_sumcum['MM-DD'] = pd_ts_ppt_sumcum['Data'].dt.strftime('%m;%d')

    pd_ts_ppt_sum = pd_ts_ppt_sum.reset_index()
    pd_ts_ppt_sum['Data'] = pd.to_datetime(pd_ts_ppt_sum.Data)
    pd_ts_ppt_sum['MM-DD'] = pd_ts_ppt_sum['Data'].dt.strftime('%m;%d')
    return pd_ts_ppt_sum, pd_ts_ppt_sumcum

    
def get_merx_soy_mask():
        merx_to_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/remap_soja_to_2022')
        merx_sp_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_sp_2022')
        merx_sc_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_sc_2022')
        merx_rs_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_rs_2022')
        merx_ro_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_ro_2022')
        merx_pr_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_pr_2022')
        merx_pi_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_pi_2022')
        merx_pa_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_pa_2022')
        merx_mt_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_mt_2022')
        merx_ms_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_ms_2022')
        merx_mg_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_mg_2022')
        merx_ma_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_ma_2022')
        merx_go_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_go_2022')
        merx_ba_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/soja_ba_2022')

        merx_soja= ee.ImageCollection([merx_ma_2022,merx_go_2022, merx_to_2022 ,merx_ba_2022, merx_mt_2022, merx_sp_2022, merx_sc_2022, merx_rs_2022, merx_ro_2022, merx_pr_2022 , merx_pi_2022, merx_pa_2022, merx_ms_2022, merx_mg_2022 ]).mosaic();
        return merx_soja

merx_soja = get_merx_soy_mask()

def mask_image(image):
    return image.mask(merx_soja)

######################## NDVI E EVI TIME SERIES #######################

# def mask_image(image):
#     return image.mask(merx_soja)#.clip(CAR_t1)

def serie_temporal_ndvi(points,start_date, end_date): 
    S2_ts = (ee.ImageCollection('COPERNICUS/S2_SR')
            #.filterBounds(CAR_t1)
            .filterBounds(points)
            .filterDate(start_date, end_date)
            .maskClouds()
            .scaleAndOffset()
            .spectralIndices(['NDVI', 'EVI'])
            .select(['NDVI', 'EVI']))

    S2_ts_mask = S2_ts#.map(mask_image)

    ts_ndvi_fc = S2_ts_mask.getTimeSeriesByRegion(reducer = [ee.Reducer.mean()],
                                  geometry = points, #CAR_t1,
                                  bands = ['NDVI','EVI'],
                                  scale = 20)
    
    ts_ndvi = geemap.ee_to_pandas(ts_ndvi_fc)
    ts_ndvi0 = geemap.ee_to_pandas(ts_ndvi_fc)

    ts_ndvi = ts_ndvi.loc[ts_ndvi.NDVI > 0]
    ts_evi = ts_ndvi0.loc[ts_ndvi0.EVI> 0]

    return ts_ndvi0, ts_ndvi, ts_evi


    
today = date.today()
#end_date = today - timedelta(days = 5)
#start_date = today - timedelta(weeks = 1)#6 semanas anteriores a data de hoje
end_date_ndvi = today + timedelta(days = 1)
end_date_climate = today+ timedelta(days = 1)

q  = "SELECT * FROM information_schema.tables WHERE table_schema = 'monitoramento'"
# user = 'ferraz'
# password = '3ino^Vq3^R1!'
# host = 'vps40890.publiccloud.com.br'
# port = 5432
# database = 'carbon'
 
# engine = create_engine(
#          url="postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(
#              user, password, host, port, database
#          )
#      )

cod_df = pd.read_sql_query(q,con=engine)

# DELETE TABLE
# delete_list = cod_df['table_name'].to_list()
# delete_list.remove('vazio_sanitario')
# 
# for t in delete_list:
#     q = 'drop table if exists monitoramento.{0} cascade;'.format(t)
#     engine.execute(q)
# 
# =============================================================================

tables = list(set(cod_df['table_name'].str.split('_').str[0]))
tables.remove('vazio')

cod_list = [name.upper()[:2] + '-' + name.upper()[2:9] + '-' + name.upper()[9:] for name in tables]    

soja_fc = ee.FeatureCollection('projects/ee-carbonei/assets/mapeamento/merx_soja_br_2022_grid5x5')

for cod in cod_list:
    st = time.time()
    #cod = 'MT-5102686-235C60F3726D4809B96D1F6A5B4B731E'
    try:
        t = cod.replace('-', '').lower()
        tableNameClimate =  "{0}_climate".format(t)
        tableNameIndex =  "{0}_index".format(t)
        
        start_date_climate = pd.read_sql_query('select data from monitoramento.{0}_climate order by data desc limit 1'.format(t),con=engine)['data'][0]
        start_date_index = pd.read_sql_query('select date from monitoramento.{0}_index order by date desc limit 1'.format(t),con=engine)['date'][0][:10]
        start_date_climate = datetime.strptime(start_date_climate, '%Y-%m-%d') + timedelta(days = 1)
        start_date_index  = datetime.strptime(start_date_index, '%Y-%m-%d') + timedelta(days = 1)
        
    
        # CAR = ee.FeatureCollection('projects/ee-carbonei/assets/area_imovel/cars_all_ufs')\
        #     .filter(ee.Filter.eq('cod_imovel', cod))
        CAR = get_car(cod)
        lat_long = CAR.geometry().centroid() ##utilizado nos dados da propriedade
        lon = lat_long.getInfo()['coordinates'][0] ## utilizado nos dados da propriedade
        lat = lat_long.getInfo()['coordinates'][1] ## utilizado nos dados da propriedade
        a,b = list(CAR.getInfo().items())[-1]
        
        pd_ts_temp_mean = get_temperature_data(CAR, start_date_climate.strftime('%Y-%m-%d'), end_date_climate.strftime('%Y-%m-%d'))
        pd_ts_temp_mean['Data'] = pd_ts_temp_mean['Data'].dt.strftime('%Y-%m-%d')
        pd_ts_ppt = get_precipitation_data(start_date_climate.strftime('%Y-%m-%d'), end_date_climate.strftime('%Y-%m-%d'), CAR, lat_long)
        pd_ts_ppt_sum, pd_ts_ppt_sumcum = get_precipitation_sum(pd_ts_ppt)
        pd_ts_ppt_sum['Data'] = pd_ts_ppt_sum['Data'].dt.strftime('%Y-%m-%d')
        pd_ts_ppt_sumcum['Data'] = pd_ts_ppt_sumcum['Data'].dt.strftime('%Y-%m-%d')
        
        pd_ts_ppt_sumcum.columns = ['Data','sumcum', 'mmdd']
        
        climate_df = pd.merge(pd_ts_temp_mean[['Data', 'Temperatura']],\
                              pd_ts_ppt_sum[['Data', 'hourlyPrecipRate']],on='Data', how='inner')
        climate_df = pd.merge(climate_df ,pd_ts_ppt_sumcum[['Data', 'sumcum']],on='Data', how='inner')
        climate_df.columns = map(str.lower, climate_df.columns)
        
        sumcum_past = pd.read_sql_query('select sumcum from monitoramento.{0}_climate order by sumcum desc limit 1'.format(t),con=engine)['sumcum'][0]
        climate_df['sumcum'] += sumcum_past
        
        talhoes = pd.read_sql_query('select talhao, classe, st_asgeojson(geom) as geom, ST_Area(ST_Transform(geom, 26986))/10000\
    as h from monitoramento.{0}_parcels'.format(t),con=engine)
       
        df_list = []
        
        for tid in range(1,len(talhoes)+1):
            talhao = talhoes.loc[talhoes['talhao'] == tid]
            gdf = gpd.read_file(str(talhao['geom'][tid-1]), driver='GeoJSON')
            geo_json = gdf.to_json()
            j = json.loads(geo_json)
            #j['features'][0]['properties']
            # for k in range(len(j["features"])):
            #     del j["features"][k]['properties']
        
            talhaoFc = ee.FeatureCollection(j)
            #randomPoints = ee.FeatureCollection.randomPoints(talhao, 10)
            ts_ndvi0, ts_ndvi, ts_evi = serie_temporal_ndvi(talhaoFc, start_date_index.strftime('%Y-%m-%d'),\
                                                            end_date_ndvi.strftime('%Y-%m-%d'))
            ts_ndvi['talhao'] = tid
            df_list.append(ts_ndvi)
            
        index_df = pd.concat(df_list)
        index_df.drop('reducer', axis=1, inplace = True)
        
        cols = ['date', 'NDVI', 'EVI', 'talhao']
        index_df = index_df[cols]
        index_df.columns = map(str.lower, index_df .columns)
        
        conn = psycopg2.connect("host='{0}' port='{1}' dbname='{2}' user='{3}' password='{4}'".format(
            host, port, database, user, password))
        cur = conn.cursor()
        
     
        
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableNameClimate,))
        b = cur.fetchone()[0]
        
        if not b:
            print('Farm is not registered. Please use the code to register it.')
            conn.close()
            
        else:
            climate_df["query"] = "('" + climate_df.data.astype(str) +"', " +  climate_df.temperatura.astype(str) +\
                ", " + climate_df.hourlypreciprate.astype(str) + ", " + climate_df.sumcum.astype(str) +\
                    ")"
            
            c_query  = climate_df["query"].tolist()
            c_query = ", ".join(c_query)
            
            c_query ="INSERT INTO monitoramento.{0} (data, temperatura, hourlypreciprate, sumcum) VALUES "\
                .format(tableNameClimate) + c_query
            
            cur.execute(c_query)
            conn.commit()
            
            if len(index_df) > 0:
                
                index_df["query"] = "('" + index_df.date.astype(str) +"', " +   index_df.ndvi.astype(str) +\
                    ", " +  index_df.evi.astype(str) + ", " +  index_df.talhao.astype(str) + ")"
                
                i_query  = index_df["query"].tolist()
                i_query = ", ".join(i_query)
                
                i_query ="INSERT INTO monitoramento.{0} (date, ndvi, evi, talhao) VALUES ".format(tableNameIndex) + i_query
                
                #ct_query = "CREATE TABLE public.conab_base (Safra varchar, Safras varchar, Data varchar, Tocantins float, Maranhao float, Piaui float, Bahia float, Mato_Grosso float, Mato_Grosso_do_Sul float, Goias float, Minas_Gerais float, Sao_Paulo float, Parana float, Santa_Catarina float, Rio_Grande_do_Sul float, TODOS float);"
                
                
                cur.execute(i_query)
                conn.commit()
                conn.close()
                print(cod + ' updated successfully.')
                print("--- 1 CAR atualizacao_semanal %s seconds ---" % (time.time() - st))
            else:
                print('No Sentinel2 data for ' + cod + '. Climate data updated successfully.')
                conn.close()
            
    except Exception as e:
        print(cod + ' update failed due to the following error: ' + str(e) )
        time.sleep(60)



# -*- coding: utf-8 -*-
"""
Created on Tue Mar 28 16:08:07 2023

@author: gabri
"""

import ee
import eemont
import geemap
import os, glob
import rasterio
import rpy2.robjects as robjects
import sys
import pandas as pd
import sqlalchemy
import geopandas as gpd
import json    
from sqlalchemy import create_engine
import time
from joblib import Parallel, delayed
import geojson
import glob
import psycopg2
from datetime import time, timedelta, date
from datetime import datetime
import time
#Initiate GEE

#ee.Authenticate()
ee.Initialize()

st = time.time()


def get_car(codCAR):
    uf = codCAR[0:2].lower()
    
    q = "select st_asgeojson(geom) as geom from car.area_imovel_{0} where cod_imovel  = '{1}'".format(uf, codCAR)
    
    geom = pd.read_sql_query(q,con=engine)
    gdf = gpd.read_file(geom['geom'][0], driver='GeoJSON')
    geo_json = gdf.to_json()
    j = json.loads(geo_json)
    CAR  = ee.FeatureCollection(j)
    return CAR

def get_temperature_data(CAR, start_date, today):
    #start_time = time.time()
    temp = ee.ImageCollection('NOAA/CFSR').select("Temperature_surface").filterBounds(CAR).filterDate(start_date, str(today))

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
    #print("--- TEMPERATURE DATA %s seconds ---" % (time.time() - start_time))
    return pd_ts_temp_mean
   
def get_precipitation_data(start_date, end_date, CAR, lat_long):
    #start_time = time.time()
    ppt = ee.ImageCollection("JAXA/GPM_L3/GSMaP/v6/operational").select("hourlyPrecipRate").filterBounds(CAR).filterDate(start_date, end_date)
    point = ee.Geometry.Point(lat_long.getInfo()['coordinates'])#.buffer(20000)

    ts_ppt = ppt.getTimeSeriesByRegion(reducer = [ee.Reducer.sum()],
                                  geometry = point,
                                  bands = 'hourlyPrecipRate',
                                  scale = 100)

    tsPandas = geemap.ee_to_pandas(ts_ppt)
    #print("--- PPT DATA %s seconds ---" % (time.time() - start_time))
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
        merx_mt_2022 = ee.Image('projects/ee-carbonei/assets/mapeamento/remap_soja_mt_2022')
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

def execute(cod):
    # import ee
    # import eemont, geemap

    # ee.Initialize()
    
    conn = psycopg2.connect("host='{0}' port='{1}' dbname='{2}' user='{3}' password='{4}'".format(
        host, port, database, user, password))
    cur = conn.cursor()

    os.system(r"C:\Users\gabri\anaconda3\python.exe C:\Projetos\monitoramento\pipeline_cadastro\pyOrchestrator_pipeline.py 2022-08-01 2022-08-30 {}".format(cod))
    parcels_file = 'C:\\Projetos\\monitoramento\\pipeline_cadastro\\{}.GeoJSON'.format(cod)
    with open(parcels_file) as f:
        gj = geojson.load(f)
    features = gj['features']
    
    t = cod.replace('-', '').lower()
    tableName =  "{0}_parcels".format(t)
    
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableName,))
    b = cur.fetchone()[0]
    
    if not b:
        ct_query = "CREATE TABLE monitoramento.{0} (talhao integer,  classe varchar,   geom geometry(Polygon, 4326));"\
                .format(tableName)
        cur.execute(ct_query.lower())
        qlist = []
        for feature in features:
            geom = '{\
            "type":"TYPE",\
            "coordinates":COORDINATES,\
            "crs":{"type":"name","properties":{"name":"EPSG:4326"}}}'\
    .replace('TYPE',str(feature['geometry']['type'])).replace('COORDINATES',str(feature['geometry']['coordinates']))
    
            q = "(" + str(feature['properties']['id_talhao']) +", '" +  str(feature['properties']['class']) +\
                "', '" + geom + "')"
            qlist.append(q)
            #q = q.replace('\\', '')
            
        query = ", ".join(qlist)
        
        c_query ="INSERT INTO monitoramento.{0} (talhao, classe, geom) VALUES "\
             .format(tableName) + query
        
        
        cur.execute(c_query)
        # cur.execute(i_query)
        conn.commit()
        conn.close()
        print(cod + ' parcels registered successfully.')
        os.remove(parcels_file)
    else:
        print('Parcels are already registered.')
    
    CAR = get_car(cod)
    lat_long = CAR.geometry().centroid() ##utilizado nos dados da propriedade
    #lon = lat_long.getInfo()['coordinates'][0] ## utilizado nos dados da propriedade
    #lat = lat_long.getInfo()['coordinates'][1] ## utilizado nos dados da propriedade
    a,b = list(CAR.getInfo().items())[-1]

    
    pd_ts_temp_mean = get_temperature_data(CAR, start_date_climate, end_date.strftime('%Y-%m-%d'))
    pd_ts_temp_mean['Data'] = pd_ts_temp_mean['Data'].dt.strftime('%Y-%m-%d')
    pd_ts_ppt = get_precipitation_data(start_date_climate,end_date.strftime('%Y-%m-%d'), CAR, lat_long)
    pd_ts_ppt_sum, pd_ts_ppt_sumcum = get_precipitation_sum(pd_ts_ppt)
    pd_ts_ppt_sum['Data'] = pd_ts_ppt_sum['Data'].dt.strftime('%Y-%m-%d')
    pd_ts_ppt_sumcum['Data'] = pd_ts_ppt_sumcum['Data'].dt.strftime('%Y-%m-%d')
    
    pd_ts_ppt_sumcum.columns = ['Data','sumcum', 'mmdd']
    
    climate_df = pd.merge(pd_ts_temp_mean[['Data', 'Temperatura']],\
                          pd_ts_ppt_sum[['Data', 'hourlyPrecipRate']],on='Data', how='inner')
    climate_df = pd.merge(climate_df ,pd_ts_ppt_sumcum[['Data', 'sumcum']],on='Data', how='inner')
    climate_df.columns = map(str.lower, climate_df.columns)
    
    
    talhoes = pd.read_sql_query('select talhao, classe, st_asgeojson(geom) as geom, ST_Area(ST_Transform(geom, 26986))/10000\
as h from monitoramento.{0}_parcels'.format(t),con=engine)

    df_list = []
    
    for tid in range(1,len(talhoes)+1):
        talhao = talhoes.loc[talhoes['talhao'] == tid]
        gdf = gpd.read_file(str(talhao['geom'][tid-1]), driver='GeoJSON')
        geo_json = gdf.to_json()
        j = json.loads(geo_json)

        talhaoFc = ee.FeatureCollection(j)
        ts_ndvi0, ts_ndvi, ts_evi = serie_temporal_ndvi(talhaoFc, start_date_ndvi,\
                                                        end_date.strftime('%Y-%m-%d'))
        ts_ndvi['talhao'] = tid
        df_list.append(ts_ndvi)
    #print("--- INDEX DATA %s seconds ---" % (time.time() - start_time))
        
    index_df = pd.concat(df_list)
    index_df.drop('reducer', axis=1, inplace = True)
    
    cols = ['date', 'NDVI', 'EVI', 'talhao']
    index_df = index_df[cols]
    index_df.columns = map(str.lower, index_df .columns)
    
    
    conn = psycopg2.connect("host='{0}' port='{1}' dbname='{2}' user='{3}' password='{4}'".format(
        host, port, database, user, password))
    cur = conn.cursor()
    
    tableNameClimate =  "{0}_climate".format(t)
    tableNameIndex =  "{0}_index".format(t)
    
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableNameClimate,))
    b = cur.fetchone()[0]
    
    if not b:
        ct_query = "CREATE TABLE monitoramento.{0} (data varchar,  temperatura float,  hourlypreciprate float, sumcum float);\
            CREATE TABLE monitoramento.{1} (date varchar,  ndvi float,  evi float, talhao float);"\
                .format(tableNameClimate, tableNameIndex)
        cur.execute(ct_query.lower())
    
        climate_df["query"] = "('" + climate_df.data.astype(str) +"', " +  climate_df.temperatura.astype(str) +\
            ", " + climate_df.hourlypreciprate.astype(str) + ", " + climate_df.sumcum.astype(str) +\
                ")"
        
        c_query  = climate_df["query"].tolist()
        c_query = ", ".join(c_query)
        
        c_query ="INSERT INTO monitoramento.{0} (data, temperatura, hourlypreciprate, sumcum) VALUES "\
            .format(tableNameClimate) + c_query
        
        index_df["query"] = "('" + index_df.date.astype(str) +"', " +   index_df.ndvi.astype(str) +\
            ", " +  index_df.evi.astype(str) + ", " +  index_df.talhao.astype(str) + ")"
        
        i_query  = index_df["query"].tolist()
        i_query = ", ".join(i_query)
        
        i_query ="INSERT INTO monitoramento.{0} (date, ndvi, evi, talhao) VALUES ".format(tableNameIndex) + i_query
        
        cur.execute(c_query)
        cur.execute(i_query)
        conn.commit()
        conn.close()
        return print(cod + ' registered successfully.')
    else:
        return print('Farm is already registered. PLease use the code to update data for this farm.')
        
codList = [
'MT-5106281-0FBE67EB2CCD4A41BE699FD99E464CD4',\
]

#start_time = time.time()    
    
global user
user = 'ferraz'
global password
password = '3ino^Vq3^R1!'
global host
host = 'vps40890.publiccloud.com.br'
global port
port = 5432
global database
database = 'carbon'

global engine
engine = create_engine(
        url="postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )

global end_date
end_date = date.today()
global start_date_ndvi
start_date_ndvi = '2019-09-15'
global start_date_climate
start_date_climate = '2022-10-01'
    
global soja_fc
soja_fc = ee.FeatureCollection('projects/ee-carbonei/assets/mapeamento/merx_soja_br_2022_grid5x5')


#Parallel(n_jobs=2, prefer="threads")(delayed(execute)(cod) for cod in codList)
for cod in codList:
    execute(cod)

print("--- 1 CAR pipeline_etl_monitoramento %s seconds ---" % (time.time() - st))
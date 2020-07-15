# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import geopandas as gpd 
import pandas as pd
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import numpy as np
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

import folium




df_original=pd.read_csv('/Users/pulkitwadhwa/Downloads/rapports-accident-2012.csv',index_col=0)


#####finding addresses using civiq number
df_civiq=df_original.dropna(subset=['NO_CIVIQ_ACCDN'])
df_civiq['ADDRESS']=df_civiq['NO_CIVIQ_ACCDN'].astype(str)+df_civiq['RUE_ACCDN']+','+df_civiq['MRC'].apply(lambda x: x.split('(')[0])+', Quebec Canada'
locator = Nominatim(user_agent="myGeocoder")
geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
df_civiq['location'] = df_civiq['ADDRESS'].apply(geocode)
from geopy.extra.rate_limiter import RateLimiter
geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
df_civiq['location'] = df_civiq['ADDRESS'].apply(geocode)
df_civiq['point'] = df_civiq['location'].apply(lambda loc: tuple(loc.point) if loc else None)
df_civiq=df_civiq.dropna(subset=['point'])
df_civiq[['latitude', 'longitude', 'altitude']] = pd.DataFrame(df_civiq['point'].tolist(), index=df_civiq.index)
df_civiq = df_civiq.drop(["ADDRESS", "location", "point","altitude"], axis=1)
df_civiq=df_civiq[(-80<= df_civiq['longitude']) & (df_civiq['longitude'] <= -55)]
df_updatedCiviq = df_civiq.drop(["latitude", "longitude"], axis=1)



##fetching the accidents for which civiq number was not useful to find the results
df_accPres = pd.concat([df_original,df_updatedCiviq]).drop_duplicates(keep=False)


#finding address using placemark
df_accPres=df_accPres.dropna(subset=['ACCDN_PRES_DE'])
df_accPres['ADDRESS']= df_accPres['ACCDN_PRES_DE']+','+df_accPres['MRC'].apply(lambda x: x.split('(')[0])+', Quebec, Canada'
df_accPres['location'] = df_accPres['ADDRESS'].apply(geocode)
from geopy.extra.rate_limiter import RateLimiter
geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
df_accPres['location'] = df_accPres['ADDRESS'].apply(geocode)
df_accPres['point'] = df_accPres['location'].apply(lambda loc: tuple(loc.point) if loc else None)
df_accPres=df_accPres.dropna(subset=['point'])
df_accPres[['latitude', 'longitude', 'altitude']] = pd.DataFrame(df_accPres['point'].tolist(), index=df_accPres.index)
df_accPres = df_accPres.drop(["ADDRESS", "location", "point","altitude"], axis=1)
df_accPres=df_accPres[(-80<= df_accPres['longitude']) & (df_accPres['longitude'] <= -55)]
df_updatedAccPres = df_accPres.drop(["latitude", "longitude"], axis=1)



##fetching the accidents for which civiq number was not useful to find the results
mergedDf = pd.concat([df_updatedCiviq,df_updatedAccPres]).drop_duplicates(keep=False)
df_rue = pd.concat([df_original,mergedDf]).drop_duplicates(keep=False)



##finding address using street name
df_rue['ADDRESS']=df_rue['NO_CIVIQ_ACCDN'].astype(str)+df_rue['RUE_ACCDN']+','+df_rue['MRC'].apply(lambda x: x.split('(')[0])+', Quebec Canada'
locator = Nominatim(user_agent="myGeocoder")
geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
df_rue['location'] = df_rue['ADDRESS'].apply(geocode)
from geopy.extra.rate_limiter import RateLimiter
geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
df_rue['location'] = df_rue['ADDRESS'].apply(geocode)
df_rue['point'] = df_rue['location'].apply(lambda loc: tuple(loc.point) if loc else None)
df_rue=df_rue.dropna(subset=['point'])
df_rue[['latitude', 'longitude', 'altitude']] = pd.DataFrame(df_rue['point'].tolist(), index=df_rue.index)
df_rue = df_rue.drop(["ADDRESS", "location", "point","altitude"], axis=1)
df_rue=df_rue[(-80<= df_rue['longitude']) & (df_rue['longitude'] <= -55)]
df_updatedRue = df_rue.drop(["latitude", "longitude"], axis=1)



df_civ_acc = pd.concat([df_civiq,df_accPres]).drop_duplicates(keep=False)

updated_df= pd.concat([df_civ_acc,df_rue]).drop_duplicates(keep=False)

updated_df=updated_df.sort_index(axis = 0) 

updated_df = updated_df.rename(columns={'longitude': 'LOC_LONG', 'latitude': 'LOC_LAT'})


updated_df.to_csv("Quebec_accidents_2012.csv")
map1 = folium.Map(
    location=[46.8139,71.2080],
    tiles='OpenStreetMap',
    zoom_start=12,
)

updated_df.apply(lambda row:folium.CircleMarker(location=[row["latitude"], row["longitude"]]).add_to(map1), axis=1)
map1
map1.save("map1.html")
    

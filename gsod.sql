--########find all the stations in Canada########
wbaninCan = 
--"""              
SELECT usaf,wban, name, lat, lon
FROM `bigquery-public-data.noaa_gsod.stations`
WHERE country = 'CA' 
LIMIT 100
--"""   

--##count all stations in in canada
SELECT count(distinct usaf)
        FROM `bigquery-public-data.noaa_gsod.stations`
        WHERE country = 'CA'

--get data on lytton station, where record temperature was recorded during 2021 heatwave
--LyttonStn = """
SELECT usaf, wban, name, lat, lon, begin
FROM `bigquery-public-data.noaa_gsod.stations`
WHERE country = 'CA' and name = '%LYTTON%'
--        """  

--SUBQUERY - Get the date of lytton on its hottest year (should be in our heatwave)
--hottestday_lttn = """
select date, stn, max
FROM `bigquery-public-data.noaa_gsod.gsod2021`
WHERE max = 
(SELECT  max(max) as maxtemp
FROM `bigquery-public-data.noaa_gsod.gsod2021`
WHERE stn = '718910') AND stn = '718910'
---"""

--SUBQUERY - GET MAX TEMPERATURE IN ALL OF CANADA DURING HEATWAVE
select date, stn, max
FROM `bigquery-public-data.noaa_gsod.gsod2021`
WHERE max = 
(SELECT  max(max) as maxtemp
FROM `bigquery-public-data.noaa_gsod.gsod2021`
WHERE stn = '718910') AND stn = '718910'

--GET HOTTEST DAYS FOR ALL STATION IN CANADA (TEMP, DAY, MAX)
--#here I am using a join, calculation to celsius. the variable rn is getting row n
--#a window function is sorting by the max
--hottestday_can = """
SELECT max, (max-32)*5/9 celsius, mo, da, stn, name, lon, lat
FROM (
  SELECT max, mo, da, stn, name, lon,lat, ROW_NUMBER() OVER(PARTITION BY name ORDER BY max DESC) rn
  FROM `bigquery-public-data.noaa_gsod.gsod2021` a
  JOIN `bigquery-public-data.noaa_gsod.stations` b
  ON a.stn=b.usaf
  AND a.wban=b.wban
  AND max<1000
  AND country='CA'
)
WHERE rn=1
ORDER BY max DESC
---"""

with date_range as 
(select
    date('2021-06-25') as start_date,
    date('2019-06-25') as end_date)


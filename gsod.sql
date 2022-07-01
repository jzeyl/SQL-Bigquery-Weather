--########find all the stations in Canada########(basic  query)
--wbaninCan = """
        SELECT usaf,wban, name, lat, lon
        FROM `bigquery-public-data.noaa_gsod.stations`
        WHERE country = 'CA' 
--        """   

--#get temperatures in lytton, BC and kamloops, BC during June and july 2021 (JOIN, LIKE, BETWEEN)
--lyttontempwave = """
SELECT date, stn,max,(max-32)*5/9 celsius, name, lat, lon
FROM `bigquery-public-data.noaa_gsod.gsod2021` dat
JOIN `bigquery-public-data.noaa_gsod.stations` station
ON dat.stn=station.usaf
AND dat.wban=station.wban
WHERE (date between '2021-06-01' and '2021-07-30') AND (name LIKE '%KAMLOOPS%' OR name LIKE '%LYTTON%')  
ORDER BY date
--"""

---#get historical max temperature per month, historically, for lytton stations (UNION)
---unionmontlytemp = """
select year, mo, max(max-32)*5/9 celsius, stn 
 FROM (
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2011`
               UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2012`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2013`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2014`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2015`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2016`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2017`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2018`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2019`
                UNION ALL
                SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2020` 
                UNION ALL
                SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2021`   
) 
where stn = '718910' OR stn = '717650'
group by year, mo, stn
order by year, mo, stn
---"""

--# if we query for the max  temperature of Lytton, we should get Jun 30, when the record was brooken
--#query:  check lytton's hottest day (nested query)
--#Here i use a subquery to extract more data from the row where the highest temperature was recorded in London for 2021
--hottestday_lttn = """
select date, stn, max,(max-32)*5/9 celsius
FROM `bigquery-public-data.noaa_gsod.gsod2021`
WHERE max = (SELECT  max(max) as maxtemp
FROM `bigquery-public-data.noaa_gsod.gsod2021`
WHERE stn = '718910') AND stn = '718910' 
--"""

--#To show the geographic location of the heatwave ()
--# Count the days for all stations in Canada where the temperature was 35 or greater(window function)
--count_tempwave = """
SELECT date, stn,max,(max-32)*5/9  celsius, name, lat, lon, 
CASE WHEN (max-32)*5/9 > 34 then '35 or above' END heat,
COUNT(CASE WHEN (max-32)*5/9 > 34 then '35 or above' END) OVER(PARTITION BY name) daysover34
FROM `bigquery-public-data.noaa_gsod.gsod2021` dat
JOIN `bigquery-public-data.noaa_gsod.stations` station
ON dat.stn=station.usaf
AND dat.wban=station.wban
WHERE (date between '2021-06-25' and '2021-07-07') AND (country = 'CA') 
GROUP BY date, stn,max, name, lat, lon
ORDER BY date, stn,max, name, lat, lon
--"""

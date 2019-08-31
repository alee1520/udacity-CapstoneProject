"""
 load_fact_and_dim_table.py
 Created on: 08/28/2019
 Created by: Andre Lee
 Purpose: To automate the creation of staging, fact and dimension tables.
          The scipt contains SQL logic used for ETL.  The script is called
          from etl.py          
"""

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

dbuser= config.get("CLUSTER","DB_USER")
ARN = config.get("IAM_ROLE","ARN")
la_data = config.get("S3","LA_DATA_FILE")
nyc_data = config.get("S3","NYC_DATA_FILE")
us_cities = config.get("S3","US_CITIES_FILE")

######################
# create tables
######################

CREATE_TABLE_D_AGE_GROUP = ("""CREATE TABLE IF NOT EXISTS d_age_group
(
    id integer identity(0, 1) PRIMARY KEY unique not null,
    age_group varchar(10) ,
    age integer
);""")

CREATE_TABLE_D_AREA = ("""CREATE TABLE IF NOT EXISTS d_area
(
    id integer identity(0, 1) PRIMARY KEY unique not null,
    area_cd char(3),
    area_name varchar(50) 
);""")

CREATE_TABLE_D_COORDINATES = ("""CREATE TABLE IF NOT EXISTS d_coordinates
(
    id integer identity(0, 1) PRIMARY KEY unique not null,
    latitude varchar,
    longitude varchar,
    lat_lon varchar(60) 
);""")

CREATE_TABLE_D_GENDER_TYPE = ("""CREATE TABLE IF NOT EXISTS d_gender_type
(
    id integer identity(0, 1) PRIMARY KEY unique not null,
    gender character(1) ,
    gender_desc varchar(20) 
);""")

CREATE_TABLE_D_JURISDICTION = ("""CREATE TABLE IF NOT EXISTS d_jurisdiction
(
    id integer identity(0, 1) PRIMARY KEY unique not null,
    juris_cd integer,
    juris_desc varchar(100) 
);""")

CREATE_TABLE_D_RACE_TYPE = ("""CREATE TABLE IF NOT EXISTS d_race_type
(
    id integer identity(0, 1) PRIMARY KEY unique not null,
    race varchar(50) 
);""")

CREATE_TABLE_D_US_CITIES = ("""CREATE TABLE IF NOT EXISTS d_us_cities
(
    id integer identity(0, 1) PRIMARY KEY unique not null,
    city varchar(100) ,
    state char(2),
    zip char(5),
    lat varchar,
    lon varchar
);""")

CREATE_TABLE_F_CRIME_DATA = ("""CREATE TABLE IF NOT EXISTS f_crime_data
(
    id integer identity(0, 1) PRIMARY KEY unique not null,
    crime_location varchar(3) ,
    rpt_date_of_crime date,
    crime_code integer,
    crime_desc varchar(100) ,
    suspect_race_id integer,
    suspect_age_id integer,
    vic_age_id integer,
    vic_race_id integer,
    area_or_boro varchar(50) ,
    premis_desc varchar(100) ,
    location_id integer
);""")

CREATE_TABLE_STAGING_LA_CRIME = ("""CREATE TABLE IF NOT EXISTS staging_la_crime
(
    dr_no varchar,
    date_rptd varchar,
    date_occ varchar,
    time_occ varchar,
    area varchar,
    area_name varchar,
    rpt_dist_no varchar,
    part_1_2 numeric,
    crm_cd varchar,
    crm_cd_desc varchar,
    mocodes varchar,
    vict_age varchar,
    vict_sex varchar,
    vict_descent varchar,
    premis_cd varchar,
    premis_desc varchar,
    weapon_used_cd varchar,
    weapon_desc varchar,
    status varchar,
    status_desc varchar,
    crm_cd_1 varchar,
    crm_cd_2 varchar,
    crm_cd_3 varchar,
    crm_cd_4 varchar,
    location varchar,
    cross_street varchar,
    lat varchar,
    lon varchar
);""")


CREATE_TABLE_STAGING_NYC_CRIME = ("""CREATE TABLE IF NOT EXISTS staging_nyc_crime
(
    cmplnt_num numeric,
    cmplnt_fr_dt varchar,
    cmplnt_fr_tm varchar,
    cmplnt_to_dt varchar,
    cmplnt_to_tm varchar,
    addr_pct_cd integer,
    rpt_dt varchar,
    ky_cd numeric,
    ofns_desc varchar,
    pd_cd integer,
    pd_desc varchar,
    crm_atpt_cptd_cd varchar,
    law_cat_cd varchar,
    boro_nm varchar,
    loc_of_occur_desc varchar,
    prem_typ_desc varchar,
    juris_desc varchar,
    jurisdiction_code integer,
    parks_nm varchar,
    hadevelopt varchar,
    housing_psa varchar,
    x_coord_cd numeric,
    y_coord_cd numeric,
    susp_age_group varchar,
    susp_race varchar,
    susp_sex varchar,
    transit_district numeric,
    latitude varchar,
    longitude varchar,
    lat_lon varchar,
    patrol_boro varchar,
    station_name varchar,
    vic_age_group varchar,
    vic_race varchar,
    vic_sex char(1)
);""")

CREATE_TABLE_STAGING_US_CITIES = ("""CREATE TABLE IF NOT EXISTS staging_us_cities
(
    zip varchar,
    city varchar,
    state char(2),
    latitude varchar,
    longitude varchar,
    timezone integer,
    dst integer,
    geopoint varchar
);""")


######################
# Load staging tables
######################


STAGING_LA_COPY = ("""copy staging_la_crime from {}
    iam_role {}
    CSV IGNOREHEADER as 1 region 'us-east-2';
""").format(la_data,ARN)

STAGING_NYC_COPY = ("""copy staging_nyc_crime from {}
    iam_role {}
    CSV IGNOREHEADER as 1 region 'us-east-2';
""").format(nyc_data,ARN)

STAGING_US_COPY = ("""copy staging_us_cities from {}
    iam_role {}
    DELIMITER ';' IGNOREHEADER as 1 region 'us-east-2';
""").format(us_cities,ARN)

###########################
#	Load Dimension tables
##########################


D_AREA_INSERT =("""INSERT INTO D_AREA
(AREA_CD, AREA_NAME) 
SELECT distinct 'NYC' AS AREA_CD, boro_nm FROM STAGING_NYC_CRIME
where boro_nm is not null
union
select distinct 'LAX' AS AREA_CD, area_name from staging_la_crime;""")

D_LOC_NYC_INSERT =("""insert into d_coordinates
(latitude, longitude, lat_lon)
select distinct
	latitude,
	longitude,
	lat_lon
from	
staging_nyc_crime;""")

D_LOC_LA_INSERT =("""insert into d_coordinates
(latitude, longitude, lat_lon)
select distinct
	lat,
	lon,
	'('|| lat || ',' || lon || ')'
from	
staging_la_crime;""")

D_RACE_TYPE_INSERT = ("""insert into d_race_type
(race)
select distinct 
    case 
        when susp_race = ' ' then 'N/A'
        else susp_race end as susp_race 
from staging_nyc_crime where susp_race is not null""")

D_JURIS_INSERT = ("""insert into public.d_jurisdiction
(juris_cd, juris_desc)
select distinct jurisdiction_code, juris_desc from staging_nyc_crime
order by 1;""")

D_GENDER_M_INSERT =("""insert into d_gender_type
(gender,gender_desc)
values('M','Male');""")

D_GENDER_F_INSERT = ("""insert into d_gender_type
(gender,gender_desc)
values('F','Female');""")

D_GENDER_U_INSERT = ("""insert into d_gender_type
(gender,gender_desc)
values('U','UNKNOWN');""")

D_AGE_GROUP_LA_INSERT =("""insert into d_age_group
(AGE_GROUP, AGE)
select distinct 
'UNKNOWN' AS AGE_GROUP,
cast(vict_age as int)
from staging_la_crime
where vict_age like '%-%' or vict_age = 0
order by 2;""")

D_AGE_GROUP_NYC_INSERT = ("""INSERT INTO  D_AGE_GROUP
(AGE_GROUP, AGE)
select 
case
	when vict_age  between 0 and 17 then '<18'
	when vict_age  between 18 and 24 then '18-24'
	when vict_age  between 25 and 44 then '25-44'
	when vict_age  between 45 and 64 then '45-64'
	when vict_age  between 65 and 1000 then '<65'
end as AGE_GROUP,
vict_age
from (
select distinct 
cast(vict_age as int) as vict_age from staging_la_crime
where vict_age NOT like '%-%'
order by 1) x;""")

D_US_CITIES_INSERT =("""
INSERT INTO D_US_CITIES
(city,state,zip,lat	,lon)
select distinct
	city, state, zip, latitude, longitude
from
	staging_us_cities;
""")


#######################
# Load fact table
#######################
 #LAX data

F_CRIME_DATA_LA = ("""insert into f_crime_data
(
crime_location,rpt_date_of_crime,crime_code,crime_desc,
	suspect_race_id,suspect_age_id,vic_age_id,vic_race_id,
	area_or_boro,premis_desc,location_id
)
select distinct
'LAX' AS crime_location,
to_date(date_occ,'MM/DD/YYYY') as rpt_date_of_crime,
cast(crm_cd as integer) as crime_code,
crm_cd_desc as crime_desc,
6 as suspect_race_id,
1 as suspect_age_id,
case
	when stg.vict_age is null then 1
	else age_grp.id
end as vic_age_id,
CASE
	WHEN vic_rt.race is null then '6'
	else vic_rt.id
END as vic_race_id,
area_name as area_or_boro,
premis_desc,
case
	when loc.id is null then -1
	else loc.id
end as location_id
from 
	staging_la_crime stg
	LEFT join (select distinct id, substring(race,1,1) race from d_race_type) vic_rt
		on vic_rt.race = stg.vict_descent
	LEFT join d_age_group age_grp
		on age_grp.age = cast(stg.vict_age as integer)
	LEFT join d_coordinates loc
		on loc.latitude = stg.lat 
		and loc.longitude = stg.lon
order by 2""")

# NYC

F_CRIME_DATA_NYC =("""insert into f_crime_data
(
crime_location,rpt_date_of_crime,crime_code,crime_desc,
	suspect_race_id,suspect_age_id,vic_age_id,vic_race_id,
	area_or_boro,premis_desc,location_id
)
select distinct
	'NYC' as crime_location,
	case	
		when cmplnt_fr_dt is null then to_date(rpt_dt,'MM/DD/YYYY')
		else to_date(cmplnt_fr_dt,'MM/DD/YYYY')
	end as rpt_date_of_crime,
	pd_cd,
	pd_desc,
    case 
        when stg.susp_race = ' ' then (select id from d_race_type where race='N/A')
    else rt_sup.id 
    end as suspect_race_id,
	case
		when sus_age.id is null then 0
		else sus_age.id
	end as suspect_age_id,
	case
		when vic_age.id is null then 0
	else vic_age.id
	end as vic_age_id,
	rt_vic.id as vic_race,
	boro_nm,
	prem_typ_desc,
	loc.id as location_id
from staging_nyc_crime stg
	left join (select distinct min(id) as id, age_group from d_age_group 
					group by age_group) sus_age
		on sus_age.age_group = stg.susp_age_group
	left join (select distinct min(id) as id, age_group from d_age_group 
					group by age_group)  vic_age
		on vic_age.age_group = stg.vic_age_group
	left join public.d_race_type rt_sup
		on rt_sup.race = stg.susp_race
	left join public.d_race_type rt_vic
		on rt_vic.race = stg.vic_race	
	left join d_coordinates loc
		on loc.lat_lon = stg.lat_lon
order by cmplnt_fr_dt desc;""")

#Truncate tables so the data can reload

TURNC_D_AGE_GROUP = ("""TRUNCATE TABLE D_AGE_GROUP;""")
TURNC_D_AREA = ("""TRUNCATE TABLE D_AREA;""")
TURNC_D_COORDINATES = ("""TRUNCATE TABLE D_COORDINATES;""")
TURNC_D_GENDER_TYPE = ("""TRUNCATE TABLE D_GENDER_TYPE;""")
TURNC_D_JURISDICTION = ("""TRUNCATE TABLE D_JURISDICTION;""")
TURNC_D_PDCODES = ("""TRUNCATE TABLE D_PDCODES;""")
TURNC_D_RACE_TYPE = ("""TRUNCATE TABLE D_RACE_TYPE;""")
TURNC_D_US_CITIES = ("""TRUNCATE TABLE D_US_CITIES;""")
TURNC_F_CRIME_DATA = ("""TRUNCATE TABLE F_CRIME_DATA;""")
TURNC_STAGING_LA_CRIME = ("""TRUNCATE TABLE STAGING_LA_CRIME;""")
TURNC_STAGING_NYC_CRIME = ("""TRUNCATE TABLE STAGING_NYC_CRIME;""")
TURNC_STAGING_US_CITIES = ("""TRUNCATE TABLE STAGING_US_CITIES;""")

#Drop tables
DROP_D_AGE_GROUP = ("""DROP TABLE IF EXISTS D_AGE_GROUP;""")
DROP_D_AREA = ("""DROP TABLE IF EXISTS D_AREA;""")
DROP_D_COORDINATES = ("""DROP TABLE IF EXISTS D_COORDINATES;""")
DROP_D_GENDER_TYPE = ("""DROP TABLE IF EXISTS D_GENDER_TYPE;""")
DROP_D_JURISDICTION = ("""DROP TABLE IF EXISTS D_JURISDICTION;""")
DROP_D_PDCODES = ("""DROP TABLE IF EXISTS D_PDCODES;""")
DROP_D_RACE_TYPE = ("""DROP TABLE IF EXISTS D_RACE_TYPE;""")
DROP_D_US_CITIES = ("""DROP TABLE IF EXISTS D_US_CITIES;""")
DROP_F_CRIME_DATA = ("""DROP TABLE IF EXISTS F_CRIME_DATA;""")
DROP_STAGING_LA_CRIME = ("""DROP TABLE IF EXISTS STAGING_LA_CRIME;""")
DROP_STAGING_NYC_CRIME = ("""DROP TABLE IF EXISTS STAGING_NYC_CRIME;""")
DROP_STAGING_US_CITIES = ("""DROP TABLE IF EXISTS STAGING_US_CITIES;""")



#Query Action List
copy_table_queries = [STAGING_LA_COPY, STAGING_NYC_COPY, STAGING_US_COPY]
insert_dim_queries = [D_AREA_INSERT,D_LOC_NYC_INSERT,D_RACE_TYPE_INSERT,D_JURIS_INSERT,D_GENDER_M_INSERT,D_GENDER_F_INSERT,D_GENDER_U_INSERT,D_AGE_GROUP_LA_INSERT,D_AGE_GROUP_NYC_INSERT, D_US_CITIES_INSERT]
insert_fact_queries = [F_CRIME_DATA_LA,F_CRIME_DATA_NYC]
truncate_tables = [TURNC_D_AGE_GROUP,TURNC_D_AREA,TURNC_D_COORDINATES,TURNC_D_GENDER_TYPE,TURNC_D_JURISDICTION,TURNC_D_PDCODES, TURNC_D_RACE_TYPE, TURNC_D_US_CITIES, TURNC_F_CRIME_DATA, TURNC_STAGING_LA_CRIME, TURNC_STAGING_NYC_CRIME, TURNC_STAGING_US_CITIES  ]
create_table_queries = [CREATE_TABLE_D_AGE_GROUP ,CREATE_TABLE_D_AREA ,CREATE_TABLE_D_COORDINATES ,CREATE_TABLE_D_GENDER_TYPE ,CREATE_TABLE_D_JURISDICTION ,CREATE_TABLE_D_RACE_TYPE, CREATE_TABLE_D_US_CITIES ,CREATE_TABLE_F_CRIME_DATA,CREATE_TABLE_STAGING_LA_CRIME ,CREATE_TABLE_STAGING_NYC_CRIME ,CREATE_TABLE_STAGING_US_CITIES]
drop_tables =[DROP_D_AGE_GROUP,DROP_D_AREA,DROP_D_COORDINATES,DROP_D_GENDER_TYPE,DROP_D_JURISDICTION,DROP_D_PDCODES, DROP_D_RACE_TYPE, DROP_D_US_CITIES, DROP_F_CRIME_DATA, DROP_STAGING_LA_CRIME, DROP_STAGING_NYC_CRIME, DROP_STAGING_US_CITIES]

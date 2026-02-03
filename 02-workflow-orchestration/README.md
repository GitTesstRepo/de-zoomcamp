## Module 2 Homework

### Quiz Questions

1 Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
- ++++ 128.3 MiB
- 134.5 MiB
- 364.7 MiB
- 692.6 MiB

Comment:
    The file size is 134,481,400 bytes (134,481,400 / 1024 / 1024 = 128.3)


2 What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?
- {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
- ++++ green_tripdata_2020-04.csv
- green_tripdata_04_2020.csv
- green_tripdata_2020.csv

Comment:
After rendering this mask {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv and setting a taxi variable as "green",  a year as 2020 and a month as 04 it will be green_tripdata_2020-04.csv.


3 How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
- 13,537.299
- ++++ 24,648,499
- 18,324,219
- 29,430,127

Comment:
    Statistics per each file:
```
    File                         Records
    yellow_tripdata_2020-01.csv	 6,405,008 
    yellow_tripdata_2020-02.csv	 6,299,354 
    yellow_tripdata_2020-03.csv	 3,007,292 
    yellow_tripdata_2020-04.csv	 237,993 
    yellow_tripdata_2020-05.csv	 348,371 
    yellow_tripdata_2020-06.csv	 549,760 
    yellow_tripdata_2020-07.csv	 800,412 
    yellow_tripdata_2020-08.csv	 1,007,284 
    yellow_tripdata_2020-09.csv	 1,341,012 
    yellow_tripdata_2020-10.csv	 1,681,131 
    yellow_tripdata_2020-11.csv	 1,508,985 
    yellow_tripdata_2020-12.csv	 1,461,897 
```


4 How many rows are there for the Green Taxi data for all CSV files in the year 2020?
- 5,327,301
- 936,199
- ++++ 1,734,051
- 1,342,034

Comment: 
```
SELECT COUNT(*)
FROM public.green_tripdata
WHERE filename LIKE 'green_tripdata_2020%';
```


5 How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
- 1,428,092
- 706,911
- ++++ 1,925,152  
- 2,561,031

Comment: 
```
SELECT COUNT(*)
FROM public.yellow_tripdata
WHERE filename = 'yellow_tripdata_2021-03.csv';
```


6 How would you configure the timezone to New York in a Schedule trigger?
- Add a timezone property set to EST in the Schedule trigger configuration
- ++++ Add a timezone property set to America/New_York in the Schedule trigger configuration
- Add a timezone property set to UTC-5 in the Schedule trigger configuration
- Add a location property set to New_York in the Schedule trigger configuration

Comment: According to Kestra documentation i.e. the second column in the Wikipedia table (https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List)
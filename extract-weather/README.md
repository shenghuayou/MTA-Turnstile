# Extracting and Cleaning the Weather data from wunderground

1. Using extract.js we gather the monthly tables as .xls formats
2. Using convert.js we convert the .xls data into .csv formats
3. Using clean.js we remove the first two lines (headers) from all .csv data
4. Using fixdates.js we fix the first column which is the day of the month into the format mm/dd/yy
   and merge all the datasets into one final .csv called 'weather.csv'

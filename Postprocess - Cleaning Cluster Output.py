#! /usr/bin/env python

phdw = 'output/pricehike-dayweek.txt'
phm = 'output/pricehike-monthly.txt'
pho = 'output/pricehike-overall.txt'
sfall = 'output/seasons-fall.txt'
sspring = 'output/seasons-spring.txt'
swinter = 'output/seasons-winter.txt'
ssummer = 'output/seasons-summer.txt'
wdw = 'output/weather-dayweek.txt'
wo = 'output/weather-overall.txt'
listFiles = [phdw,phm,pho,sfall,sspring,swinter,ssummer,wdw,wo]

for files in listFiles:
    print('Processing the file %s ' % files)
    string = ''
    with open(files,'r') as readFile:
        for line in readFile.readlines():
            string += '(' + line[1:-2] + '),'
    newString = '[' + string[:-1] + ']'
    with open(files,'w') as writeFile:
        writeFile.write(newString)

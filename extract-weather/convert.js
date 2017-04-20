var fs = require('fs');
var spawnSync = require('child_process').spawnSync;

for(var year = 2011; year < 2017; year++) {
  for(var month = 1; month < 13; month++) {
    var string = 'xlstocsv.vbs C:/Users/Victor/Desktop/MTA-Turnstile/weather/'+month+'-'+year+'.xls weather-data/'+month+'-'+year+'.csv'
    fs.writeFileSync('./conversion.bat',string);
    var r = spawnSync('cmd.exe', ['/c', 'conversion.bat']);
    console.log(r.stdout.toString());
  }
}

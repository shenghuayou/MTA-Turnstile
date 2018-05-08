var fs = require('fs');
var spawnSync = require('child_process').spawnSync;

var filename = '';

for (var year = 2010; year < 2017; year++) {
  for (var month = 1; month < 13; month++) {
    if (month < 10)
      filename = '../dataset/weather-data/0'+month+'-'+year+'.csv';
    if (month >= 10)
      filename = '../dataset/weather-data/'+month+'-'+year+'.csv';
    var command = "sed -i '1,2d' "+filename;
    fs.writeFileSync('./cleaner.bat',command);
    var r = spawnSync('cmd.exe', ['/c', 'cleaner.bat']);
    console.log('Executing the command => '+command);
  }
}

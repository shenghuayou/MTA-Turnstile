var fs = require('fs');
var csv = require('fast-csv');

function editDate(filename, month, year) {
  fs.createReadStream(filename)
      .pipe(csv())
      .on('data', function(data){
        var editString = ''
        if (month < 10) {
          if (data[0] < 10) {
            editString = '0'+month+'/0'+data[0]+'/'+year
          }
          if (data[0] >= 10) {
            editString = '0'+month+'/'+data[0]+'/'+year
          }
        }
        if (month >= 10) {
          if (data[0] < 10) {
            editString = month+'/0'+data[0]+'/'+year
          }
          if (data[0] >= 10) {
            editString = month+'/'+data[0]+'/'+year
          }
        }
        var string = editString+',"'+data[20]+'"\n'
        fs.appendFileSync('weather.csv', string)
      });
}

var filename = ''

fs.writeFile('weather.csv', '', { flag: 'wx' }, function (err) {
    if (err) throw err;
});

for(var year = 2010; year < 2011; year++) {
  dataArr = [];
  for(var month = 12; month < 13; month++) {
    if (month < 10)
      filename = '../dataset/weather-data/0'+month+'-'+year+'.csv';
    if (month >= 10)
      filename = '../dataset/weather-data/'+month+'-'+year+'.csv';
    editDate(filename, month, year)
  }
}

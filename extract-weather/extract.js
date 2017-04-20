function isIE () {
  var myNav = navigator.userAgent.toLowerCase();
  return (myNav.indexOf('msie') != -1) ? parseInt(myNav.split('msie')[1]) : false;
}

function fnExcelReport() {
 var fileName = '1-2016';
 var tableName = 'obsTable';
  var tab_text="<table border='1px'>";
  tab = document.getElementById(tableName);
  var lines = tab.rows.length;

  if (lines > 0) {
    tab_text = tab_text + tab.rows[0].innerHTML + '</tr>';
  }

  for (j = 1 ; j < lines; j++) {
    tab_text = tab_text + tab.rows[j].innerHTML + "</tr>";
  }

  tab_text=tab_text+"</table>";
  tab_text= tab_text.replace(/<A[^>]*>|<\/A>/g, "");
  tab_text= tab_text.replace(/<img[^>]*>/gi,"");
  tab_text= tab_text.replace(/<input[^>]*>|<\/input>/gi, "");

  if (isIE() && isIE() <= 9)
  {
    txtArea1.document.open("txt/html","replace");
    txtArea1.document.write(tab_text);
    txtArea1.document.close();
    txtArea1.focus();
    sa=txtArea1.document.execCommand("SaveAs",true,fileName+".xls");
    return (sa);
  }
  else
  {
    var tableToExcel = (function() {
      var uri = 'data:application/vnd.ms-excel;base64,'
      , template = '<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel" xmlns="http://www.w3.org/TR/REC-html40"><head><!--[if gte mso 9]><xml><x:ExcelWorkbook><x:ExcelWorksheets><x:ExcelWorksheet><x:Name>{worksheet}</x:Name><x:WorksheetOptions><x:DisplayGridlines/></x:WorksheetOptions></x:ExcelWorksheet></x:ExcelWorksheets></x:ExcelWorkbook></xml><![endif]--></head><body><table>{table}</table></body></html>'
      , base64 = function(s) { return window.btoa(unescape(encodeURIComponent(s))) }
      , format = function(s, c) { return s.replace(/{(\w+)}/g, function(m, p) { return c[p]; }) }
      return function(table, name) {
        if (!table.nodeType) table = document.getElementById(table)
        var ctx = "";
        ctx = {worksheet: name || 'Worksheet', table: table.innerHTML}
       var link = document.createElement("a");
        link.download = fileName+".xls";
        link.href = uri + base64(format(template, ctx));
        link.click();
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        delete link;
      }
    })()
    tableToExcel(tableName,tableName).innerHTML;
  }
}

fnExcelReport()

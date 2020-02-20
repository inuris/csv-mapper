const csv = require('csv-parser');
const fs = require('fs');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;


var data = [];
var data_new = [];
var count=1;
fs.createReadStream('dlv.product.info.image.csv').pipe(csv()).on('data', (row) => {
	if (row.image) {
		data.push(row);
	}
}).on('end', () => {
    fs.createReadStream('dlv.product.info.id_map.csv').pipe(csv()).on('data', (row) => {
        if (data_new.length===1000){
            var csvWriter = createCsvWriter({
                path: 'out-'+ count++ +'.csv',
                header: [ { id: 'id', title: 'id' }, { id: 'image', title: 'image' } ]
            });
            csvWriter.writeRecords(data_new);
            data_new = [];
        }
        for (let i = 0; i < data.length; i++) {
            if (data[i].id == row.id) {
                data_new.push({
                    "id": row.id_new,
                    "image": data[i].image
                });                
                break;
            }
        }
    })
    .on('end', () => {        
    });
});



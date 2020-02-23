const csv = require('csv-parser');
const fs = require('fs');
const _ = require('lodash');

const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const SAME_COLUMN = '__same__';

const FILE_1 = {
    filename: 'data.id.csv', // id, id_new
    data:[]
}

const FILE_2 = {
    filename: 'data.info.csv', // id, image, value....
    requireField : ['image'], // can't be blank
    data:[]
}
const OUTPUT = {
    filename: "output_",
    limit: 1000, // write data to file for every 1000 lines
    header: [ 
        // ==== EDIT HEADER COLUMN HERE ====
        { id: 'id', title: 'id' }, 
        { id: 'image', title: 'image' }
        // =================================
    ]
};

var data = [];

// Check required fields, false if blank
function notBlank(row, fieldCheck){
    if (!fieldCheck)
        return true;
    // The same column can't be blank
    if (!row[SAME_COLUMN])
        return false;
    // check more required fields array
    fieldCheck.forEach(field => {        
        if (!row[field]) {
            return false;
        }
    });
    return true;
}

// Read FILE_1
fs.createReadStream(FILE_1.filename).pipe(csv()).on('data', (row) => {
    if (notBlank(row, FILE_1.requireField))
        FILE_1.data.push(row);
	
}).on('end', () => {
    console.log("Read file 1: "+FILE_1.data.length + " row(s)");
    // Read FILE_2    
    fs.createReadStream(FILE_2.filename).pipe(csv()).on('data', (row) => {        
        if (notBlank(row, FILE_2.requireField))            
            FILE_2.data.push(row);
        
    })
    .on('end', () => {
        console.log("Read file 2: "+FILE_2.data.length + " row(s)");
        // Use _ to join 2 objects with same id
        let data = _.map(FILE_1.data, function(item){
            return _.extend(item, _.find(FILE_2.data, { [SAME_COLUMN]: item[SAME_COLUMN] }));
        });
        console.log("Mapped file: "+ data.length + " row(s)");
        // Write data to file
        let count = 1;
        while(data.length) {
            let dataWrite = data.splice(0,OUTPUT.limit);
            let csvWriter = createCsvWriter({
                path: OUTPUT.filename+ count++ +'.csv',
                header: OUTPUT.header
            });
            csvWriter.writeRecords(dataWrite);
        }
        console.log("Successfully written to new file!");
    })
});



var faker = require('faker');
var util = require("util");
var stream = require('stream');
var rand = require('seed-random');
var ks = require('kitchen-sync');
var dsvOptions = require('dsv-delimiter-configurations');
var Random = {
    seed : function(seed){ return rand(seed) },
    numSeed : (str) => str
                .split('')
                .map((a) => a.charCodeAt(0))
                .reduce((a, b) => a + b)
 };


function DSVGenerator(options){
    this.options = options || {};
    if(!this.options.counts) this.options.counts = {};
    if(!this.options.counts.columns) this.options.counts.columns = 10;
    if(!this.options.counts.rows) this.options.counts.rows = 10;
    if(!this.options.type) this.options.type = 'CSV';
    if(this.options.seed) faker.seed(Random.numSeed(this.options.seed));
}

DSVGenerator.prototype.readableStream = function(){
    var dsv = this;
    var TestStream = function(){
        stream.Readable.call(this);
        var ob = this;
        setTimeout(function(){
            generate(
                dsv.options.type,
                dsv.options.counts.rows,
                dsv.options.counts.columns,
                function(row, raw){
                    ob.emit('row', row, raw);
                    buffer.push(row);
                },
                function(){
                    setTimeout(function(){
                         ob.emit('done');
                        ob.streamFinished = true;
                    }, 1);
                },
                (dsv.options.seed)
            );
        }, 0);
    };
    util.inherits(TestStream, stream.Readable);
    var buffer = [];
    TestStream.prototype._read = function (numBytes){
        var stillReading = true;
        var val;
        while(buffer.length && stillReading){
            val = buffer.shift();
            stillReading = this.push(val);
        }
        if(this.streamFinished && !buffer.length) this.push(null);
    };
    var thisStream = new TestStream();
    return thisStream;
}

DSVGenerator.prototype.writeFile = function(file, cb){
    var writeStream = fs.createWriteStream(file);
    writeStream.on('finish', function(){ cb() });
    writeStream.pipe(this.readableStream());
}


var masterlist = Object.keys(faker.address).map(function(field){
    return 'address.'+field
}).concat(Object.keys(faker.company).map(function(field){
    return 'company.'+field
}).concat(Object.keys(faker.commerce).map(function(field){
    return 'commerce.'+field
})));

var randomColumn = function(random){
    var index = Math.floor(random() * masterlist.length);
    var parts = masterlist[index].split('.');
    return {
        generate : faker[parts[0]][parts[1]],
        name : parts[1]
    };
};

var autoQuote = function(str, quote, quotables){
    return quotables
            .map((chr)=> str.indexOf(chr) !== -1)
            .reduce((a, b)=> a || b ) ?
                quote+str+quote :
                str

}

var generate = function(type, numRows, numCols, rowCallback, finalCallback, seed){
    var cols = [];
    var row = [];
    let randomGenerator = Random.seed(seed);
    for(var lcv=0; lcv < numCols; lcv++) cols.push(randomColumn(randomGenerator));
    var options = dsvOptions[type];
    cols.forEach(function(col){
        row.push(col.name)
    });
    var value;
    var delimiter = options.delimiter || ',';
    var sentinels = [delimiter, '"', "'"];
    var writeRow = (r) => r.join(delimiter);
    rowCallback(writeRow(row)+(options.terminator||"\n"), row);
    row = [];
    for(var lcv=0; lcv < numRows; lcv++){
        cols.forEach(function(col){
            value = col.generate();
            row.push((
                Array.isArray(value)?
                '"'+value.join(delimiter)+'"':
                autoQuote(value, '"', sentinels)
            ));
        });
        if(lcv == numRows-1){
            rowCallback(writeRow(row), row );
            //setTimeout(function(){
                finalCallback();
            //}, 1);
        }else{
            rowCallback(writeRow(row)+(options.terminator||"\n"), row );
            row = [];
        }
    }
};
DSVGenerator.makeDataFile = (options, cb)=>{
    let callback = ks(cb);
    let generator = new DSVGenerator(options);
    if(options.file){
        generator.writeFile(options.file, (err)=>{
            callback(null, options.file);
        });
    }else{
        setTimeout(()=>{
            callback(null, generator.readableStream());
        });
    }
    return callback.return;
}
module.exports = DSVGenerator;

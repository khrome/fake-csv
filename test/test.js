const should = require('chai').should();
const Generator = require('../fake-csv');
const config = require('dsv-delimiter-configurations');

let size = {rows : 100, cols : 10};

const makeSimpleTestForType = (type)=>{
    it('makes fake '+type+' data', (done)=>{
        Generator.makeDataFile({
            counts : size,
            type : type,
            seed : 'some-unique-string'
        }, (err, stream)=>{
            let rows = [];
            stream.on('row', (row)=>{
                rows.push(row);
            });
            stream.on('done', ()=>{
                rows.length.should.equal(size.rows+1);
                rows[0].split(config[type].delimiter).length.should.be.above(size.cols/2);
                rows[0].split(config[type].delimiter).length.should.be.below(size.cols+1);
                done();
            });
        });
    });
}

describe('fake-csv', ()=>{
    describe('outputs 100x10 data', ()=>{
        makeSimpleTestForType('CSV');
        makeSimpleTestForType('DSV');
        makeSimpleTestForType('TSV');
        makeSimpleTestForType('SSV');
    });
});

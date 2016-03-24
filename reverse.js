var GoogleMapsAPI = require('googlemaps');
var parse = require('csv-parse');
var fs = require('fs');
var through2 = require('through2');
var spy = require('through2-spy');
var sink = require('through2-sink');
var filter = require('through2-filter');
var program = require('commander');

program
  .option('-k, --key [KEY]', 'Google API key')
  .option('-o, --offset <n>', 'offset', parseInt, 0)
  .parse(process.argv);

var publicConfig = {
  key:                program.key,
  stagger_time:       1000,
  encode_polylines:   false,
  secure:             true
};
var gmAPI = new GoogleMapsAPI(publicConfig);

var count = program.offset;
var size = program.offset+5000;

var options = {
  delimiter: '|',
  columns: true
};

fs.createReadStream('data/locality_localadmin_mismatches.psv')
  .pipe(parse(options))
  .pipe(filter.obj(function(record) {
    return count >= program.offset && count < program.offset+size;
  }))
  .pipe(spy.obj(function(record) {
    count++;
  }))
  .pipe(filter.obj(function(record) {
    // don't process the lat/lng if it already has been
    try {
      return !fs.statSync('data/' + record.localadmin_id + '.' + record.locality_id + '.json');
    }
    catch (err) {
      return true;
    }

  }))
  .pipe(through2.obj(function(record, enc, next) {
    setTimeout(function() {
      next(null, record);
    }, 110);
  }))
  .pipe(sink.obj(function(record) {
    // console.log('requesting ' + [record.lat, record.lon].join(','));

    var reverseGeocodeParams = {
      "latlng":        [record.lat, record.lon].join(','),
      "language":      "en"
    };

    gmAPI.reverseGeocode(reverseGeocodeParams, function(err, result) {
      if (result && JSON.parse(result).status === 'OK') {
        var filename = 'data/' + record.localadmin_id + '.' + record.locality_id + '.json';
        fs.writeFileSync(filename, JSON.stringify(result, null, 2) + '\n');
      }

    });

  }));

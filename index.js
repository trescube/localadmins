var fs = require('fs');
var parse = require('csv-parse');
var filter = require('through2-filter');
var sink = require('through2-sink');
var spy = require('through2-spy');
var _ = require('lodash');

var options = {
  delimiter: ',',
  columns: true
};

var localadmins = {};

fs.createReadStream('/Users/stephenhess/git/whosonfirst/whosonfirst-data/meta/wof-localadmin-latest.csv')
  .pipe(parse(options))
  .pipe(filter.obj(function(record) {
    return 'US' === record.iso;
  }))
  .pipe(sink.obj(function(record) {
    localadmins[record.id] = {
      name: record.name,
      localities: []
    };
  }))
  .on('finish', function() {
    fs.createReadStream('/Users/stephenhess/git/whosonfirst/whosonfirst-data/meta/wof-locality-latest.csv')
      .pipe(parse(options))
      .pipe(filter.obj(function(record) {
        return 'US' === record.iso;
      }))
      .pipe(filter.obj(function(record) {
        // only include localities that have a known localadmin parent
        return localadmins.hasOwnProperty(record.parent_id);
      }))
      .pipe(sink.obj(function(record) {
        localadmins[record.parent_id].localities.push({
          id: record.id,
          name: record.name,
          center: {
            lat: record.geom_latitude,
            lon: record.geom_longitude
          }
        });

      }))
      .on('finish', function() {
        // find all localadmin's with exactly 1 child locality that doesn't match on name
        console.log('localadmin_id|localadmin_name|locality_id|locality_name|lat|lon');
        Object.keys(localadmins).forEach(function(localadmin_id) {

          if (isSingleLocalityLocalAdminWithMismatchedNames(localadmins[localadmin_id]) ||
                isMultipleLocalityLocalAdmin(localadmins[localadmin_id])) {
            for (var i = 0; i < localadmins[localadmin_id].localities.length; i++) {
              var parts = [
                localadmin_id,
                localadmins[localadmin_id].name,
                localadmins[localadmin_id].localities[i].id,
                localadmins[localadmin_id].localities[i].name,
                localadmins[localadmin_id].localities[i].center.lat,
                localadmins[localadmin_id].localities[i].center.lon
              ];

              console.log(parts.join('|'));

            }

            // console.log('localadmin ' + localadmin_id + ': ' + localadmins[localadmin_id].name + ' vs ' + localadmins[localadmin_id].localities[0].name);
          }
        });

      });

  });

function isSingleLocalityLocalAdminWithMismatchedNames(localadmin) {
  return localadmin.localities.length === 1 &&
      _.toUpper(localadmin.name) !== _.toUpper(localadmin.localities[0].name)
}

function isMultipleLocalityLocalAdmin(localadmin) {
  return localadmin.localities.length > 1;
}

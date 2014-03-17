var redis       = require('redis');

var from_host
  , from_port
  , from_host_index
  , from_port_index

  , to_host
  , to_port
  , to_host_index
  , to_port_index

  , from_client = null
  , to_client   = null;



module.exports.init = init;
function init(from_host_override, from_port_override, to_host_override, to_port_override, cb) {
  from_host = from_host_override;
  from_port = from_port_override;
  to_host   = to_host_override;
  to_port   = to_port_override;

  if((from_host_index = process.argv.indexOf('--from-host')) !== -1) {
    from_host = process.argv[++from_host_index];
  }
  if((port_index = process.argv.indexOf('--from-port')) !== -1) {
    from_port = process.argv[++port_index];
  }
  if((to_host_index = process.argv.indexOf('--to-host')) !== -1) {
    to_host = process.argv[++to_host_index];
  }
  if((to_port_index = process.argv.indexOf('--to-port')) !== -1) {
    to_port = process.argv[++to_port_index];
  }


  if(!!from_host) {
    from_client = redis.createClient(from_port || 6379, from_host);
    to_client   = redis.createClient(to_port   || 6379, to_host);
  }

  if(!!cb) {
    cb(null, from_client);
  }
}

// run a default init.
init();

// Do everything
if(!!from_host && !!to_host !== -1) {
  console.log('Getting keys...');
  get_all_keys(function(error, source_keys) {
    console.log('Received ' + source_keys.length + ' keys.  Grabbing data.')
    if(!!error) {
      console.log('Problem getting keys from source.', error);
      process.exit(-1)
    }
    get_data(source_keys, function(error, source_data) {
      console.log('Received ' + source_data.length + ' objects.  Preparing transfer.')
      if(!!error) {
        console.log('Problem getting data from source.', error);
        process.exit(-2)
      }
      if(source_keys.length !== source_data.length) {
        console.log('Problem getting data from source.  The amount of keys differs from the amount of data objects.');
        process.exit(-3)
      }
      build_mset_object(source_keys, source_data, function(mset_object) {
        console.log('Transferring.')
        write_data_to_destination(mset_object, function(error) {
          if(!!error) {
            console.log('There was an error writing the data to the destination', error);
            process.exit(-4)
          }
          console.log("Complete");
          process.exit(0)
        })
      });
    });
  })
}
else {
  console.log('usage: node pass.ae.rewards required_option')
  console.log('  options:')
  console.log('    --from-host source_redis_host_url        Points the client to the source redis host. ** REQUIRED! **')
  console.log('    --from-port source_redis_host_port       Points the client to the source redis port.')
  console.log('    --to-host   destination_redis_host_url   Points the client to the destination redis host. ** REQUIRED! **')
  console.log('    --to-port   destination_redis_host_port  Points the client to the destination redis port.')
  process.exit(1)
}

function get_all_keys(cb) {
  from_client.keys('*', cb);
}

function get_data(keys, cb) {
  from_client.mget(keys, cb);
}

function build_mset_object(source_keys, source_data, cb) {
  var source_keys_length = source_keys.length
    , mset_object = [];

  for (var i = 0; i < source_keys_length; i++) {
    mset_object.push(source_keys[i]);
    mset_object.push(source_data[i]);
  };

  cb(mset_object);
}

function write_data_to_destination(mset_object, cb) {
  to_client.mset(mset_object, cb);
}

// TODO: Pass in a "--key" parameter, to just sync certain keys
// TODO: Ask if we want to overwrite data

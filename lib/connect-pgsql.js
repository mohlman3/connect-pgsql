var util = require('util');
var pg = require('pg');

module.exports = function(connect) {

  /**
   * Connect's Store.
   */

  var Store = connect.session.Store;

  /**
   * Initialize PgSqlStore with the given `options`.
   *
   * @param {Object} options
   * @api public
   */

  function PgSqlStore(options) {
    if (!options)
      throw new Error('You have to provide `options` object');

    Store.call(this, options);

    this.getClient = options.getClient || this.getClient;
    this.table = options.table || 'connect_session';
    this.sidColumn = options.sidColumn || 'sid';
    this.expiresColumn = options.expiresColumn || 'expires';
    this.sessionColumn = options.sessionColumn || 'session';
    this.host = options.host;
    this.port = options.port || 3306;
    this.user = options.user;
    this.password = options.password;
    this.database = options.database;
  };

  /**
   * Inherit from `Store`.
   */

  util.inherits(PgSqlStore, Store);

    /**
     * Default implementation to get the Postgres client, configured
     * and ready to hit the database.
     * This function can be overriden in options.
     *
     * Parameter cb: Callback taking values: error, client, done
     */
    PgSqlStore.prototype.getClient = function (cb) {
        var connString = 'postgres://' + this.user + ':' + this.password + '@' + this.host + ':' + this.port + '/' + this.database;
        pg.connect(connString, cb);
    }

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid
   * @param {Function} fn
   * @api public
   */

  PgSqlStore.prototype.get = function(sid, fn) {
    var _this = this
      , now = (new Date()).toISOString();

    this.getClient(function(err, client, done) {
      if (err) return fn(err);

      client.query( ' SELECT * FROM ' + _this.table +
                    ' WHERE ' + _this.sidColumn + ' = $1 ' +
                    ' AND ' + _this.expiresColumn + ' > cast($2 as timestamp without time zone);',
                    [ sid, now ],
          function(err, result) {
            done();
            if (!result.rows.length) fn();
            else fn(null, result.rows[0][_this.sessionColumn]);
          }
      );
    });
  };

  /**
   * Commit the given `sess` object associated with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */

  PgSqlStore.prototype.set = function(sid, sess, fn) {
    var _this = this;

    this.getClient(function(err, client, done) {
      var q = 'with new_values (sid, expires, session) as (' +
              '  values ($1, $2::timestamp without time zone, $3::json)' +
              '), ' +
              'upsert as ' +
              '( ' +
              '  update ' + _this.table + ' cs set ' +
              '    ' + _this.sidColumn + ' = nv.sid, ' +
              '    ' + _this.expiresColumn + ' = nv.expires, ' +
              '    ' + _this.sessionColumn + ' = nv.session ' +
              '  from new_values nv ' +
              '  where cs.' + _this.sidColumn + ' = nv.sid ' +
              '  returning cs.* ' +
              ')' +
              'insert into ' + _this.table + ' (' + _this.sidColumn + ', ' + _this.expiresColumn + ', ' + _this.sessionColumn + ') ' +
              'select sid, expires, session ' +
              'from new_values ' +
              'where not exists (select 1 from upsert up where ' + _this.sidColumn + ' = new_values.sid)';

      if (err) return fn(err);

      client.query(q, [ sid, sess.cookie.expires.toISOString(), JSON.stringify(sess) ], function(err, result) {
        done();
        if (err) return fn(err);
        fn();
      });
    });
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid
   * @api public
   */

  PgSqlStore.prototype.destroy = function(sid, fn) {
    var _this = this;

    this.getClient(function(err, client, done) {
      if (err) return fn(err);

      client.query('DELETE FROM ' + _this.table + ' WHERE ' + _this.sidColumn + ' = $2;', [ sid ], function(err) {
        done();
        if (err) return fn(err);
        fn();
      });
    });
  };

  return PgSqlStore;
};

/* eslint-disable no-unused-vars */
// TODO make only public methods public.
// TODO remove array login stuff.
// TODO remove unused private methods and variables.

const {TDatumType, TEncodingType, TPixel} = (isNodeRuntime() && require("../build/thrift/node/mapd_types.js")) || window // eslint-disable-line global-require
const MapDThrift = isNodeRuntime() && require("../build/thrift/node/mapd.thrift.js") // eslint-disable-line global-require
let Thrift = (isNodeRuntime() && require("thrift")) || window.Thrift // eslint-disable-line global-require
const thriftWrapper = Thrift
const parseUrl = isNodeRuntime() && require("url").parse // eslint-disable-line global-require
if (isNodeRuntime()) { // Because browser Thrift and Node Thrift are exposed slightly differently.
  Thrift = Thrift.Thrift
  Thrift.Transport = thriftWrapper.TBufferedTransport
  Thrift.Protocol = thriftWrapper.TJSONProtocol
}
import * as helpers from "./helpers"
import MapDClientV2 from "./mapd-client-v2"
import processQueryResults from "./process-query-results"

const COMPRESSION_LEVEL_DEFAULT = 3
const DEFAULT_QUERY_TIME = 50
const NUM_PINGS_PER_SERVER = 4

function noop () { /* noop */ }

function arrayify (maybeArray) { return Array.isArray(maybeArray) ? maybeArray : [maybeArray] }

function isNodeRuntime () { return typeof window === "undefined" }

function publicizeMethods (theClass, methods) { methods.forEach(method => { theClass[method.name] = method }) }

function MapdCon () {
  publicizeMethods(this, [
    connect,
    createFrontendViewAsync,
    createLinkAsync,
    createTableAsync,
    dbName,
    deleteFrontendViewAsync,
    detectColumnTypesAsync,
    disconnect,
    getFields,
    getResultRowForPixel,
    getServerStatusAsync,
    getTablesAsync,
    host,
    logging,
    password,
    port,
    protocol,
    query,
    renderVega,
    sessionId,
    user
  ])

  let _host = null
  let _user = null
  let _password = null
  let _port = null
  let _dbName = null
  let _client = null
  let _sessionId = null
  let _protocol = null
  const _datumEnum = {}
  let _logging = false
  let _platform = "mapd"
  let _nonce = 0
  const _balanceStrategy = "adaptive"
  let _numConnections = 0
  let _lastRenderCon = 0
  const queryTimes = { }
  const serverQueueTimes = null
  let serverPingTimes = null
  const pingCount = null
  let importerRowDesc = null
  // invoke initialization methods
  invertDatumTypes(_datumEnum) // TODO might just be for v1
  function processResults (options = {}, result, callback) { // TODO having defaulted params should be "
    const processor = processQueryResults(_logging, updateQueryTimes)
    const processResultsObject = processor(options, _datumEnum, result, callback)
    return processResultsObject
  }
  /** @deprecated will default to query */
  const queryAsync = query
  /**
   * Import a delimited table from a file.
   * @param {String} tableName - desired name of the new table
   * @param {String} fileName
   * @param {TCopyParams} copyParams - see {@link TCopyParams}
   * @param {TColumnType[]} headers -- a colleciton of metadata related to the table headers
   */
  const importTableAsync = importTableAsyncWrapper(false)
  /**
   * Import a geo table from a file.
   * @param {String} tableName - desired name of the new table
   * @param {String} fileName
   * @param {TCopyParams} copyParams - see {@link TCopyParams}
   * @param {TColumnType[]} headers -- a colleciton of metadata related to the table headers
   */
  const importTableGeoAsync = importTableAsyncWrapper(true)

  // return this to allow chaining off of instantiation
  return this

  /**
   * Create a connection to the server, generating a client and session id.
   * @param {Function} callback A callback that takes `(err, success)` as its signature.  Returns con singleton on success.
   * @return {MapdCon} Object
   *
   * @example <caption>Connect to a MapD server:</caption>
   * var con = new MapdCon()
   *   .host('localhost')
   *   .port('8080')
   *   .dbName('myDatabase')
   *   .user('foo')
   *   .password('bar')
   *   .connect((err, con) => console.log(con.sessionId()));
   *
   *   // ["om9E9Ujgbhl6wIzWgLENncjWsaXRDYLy"]
   */
  function connect (callback) {
    if (_sessionId) {
      disconnect()
    }

    // TODO: should be its own function
    const allAreArrays = Array.isArray(_host) && Array.isArray(_port) && Array.isArray(_user) && Array.isArray(_password) && Array.isArray(_dbName)
    if (!allAreArrays) {
      return callback("All connection parameters must be arrays.")
    }

    _client = []
    _sessionId = []

    if (!_user[0]) {
      return callback("Please enter a username.")
    } else if (!_password[0]) {
      return callback("Please enter a password.")
    } else if (!_dbName[0]) {
      return callback("Please enter a database.")
    } else if (!_host[0]) {
      return callback("Please enter a host name.")
    } else if (!_port[0]) {
      return callback("Please enter a port.")
    }

    // now check to see if length of all arrays are the same and > 0
    const hostLength = _host.length
    if (hostLength < 1) {
      return callback("Must have at least one server to connect to.")
    }
    if (hostLength !== _port.length || hostLength !== _user.length || hostLength !== _password.length || hostLength !== _dbName.length) {
      return callback("Array connection parameters must be of equal length.")
    }

    if (!_protocol) {
      _protocol = _host.map(() => window.location.protocol.replace(":", ""))
    }

    const transportUrls = getEndpoints()
    for (let h = 0; h < hostLength; h++) {
      let client = null

      if (isNodeRuntime()) {
        const {protocol: parsedProtocol, hostname: parsedHost, port: parsedPort} = parseUrl(transportUrls[h])
        const connection = thriftWrapper.createHttpConnection(
          parsedHost,
          parsedPort,
          {
            transport: thriftWrapper.TBufferedTransport,
            protocol: thriftWrapper.TJSONProtocol,
            path: "/",
            headers: {Connection: "close"},
            https: parsedProtocol === "https:"
          }
        )
        connection.on("error", console.error) // eslint-disable-line no-console
        client = thriftWrapper.createClient(MapDThrift, connection)
      } else {
        const thriftTransport = new Thrift.Transport(transportUrls[h])
        const thriftProtocol = new Thrift.Protocol(thriftTransport)
        client = new MapDClientV2(thriftProtocol)
      }

      client.connect(_user[h], _password[h], _dbName[h], (error, newSessionId) => { // eslint-disable-line no-loop-func
        if (error) {
          callback(error)
          return
        }
        _client.push(client)
        _sessionId.push(newSessionId)
        _numConnections = _client.length
        callback(null, this)
      })
    }

    return this
  }

  function convertFromThriftTypes (fields) {
    const fieldsArray = []
    // silly to change this from map to array
    // - then later it turns back to map
    for (const key in fields) {
      if (fields.hasOwnProperty(key)) {
        fieldsArray.push({
          name: key,
          type: _datumEnum[fields[key].col_type.type],
          is_array: fields[key].col_type.is_array,
          is_dict: fields[key].col_type.encoding === TEncodingType.DICT // eslint-disable-line no-undef
        })
      }
    }
    return fieldsArray
  }

  /**
   * Disconnect from the server then clears the client and session values.
   * @return {MapdCon} Object
   * @param {Function} callback A callback that takes `(err, success)` as its signature.  Returns con singleton on success.
   *
   * @example <caption>Disconnect from the server:</caption>
   *
   * con.sessionId() // ["om9E9Ujgbhl6wIzWgLENncjWsaXRDYLy"]
   * con.disconnect((err, con) => console.log(err, con))
   * con.sessionId() === null;
   */
  function disconnect (callback = noop) {
    if (_sessionId !== null) {
      for (let c = 0; c < _client.length; c++) {
        _client[c].disconnect(_sessionId[c], error => { // eslint-disable-line no-loop-func
          // Success will return NULL

          if (error) {
            return callback(error, this)
          }
          _sessionId = null
          _client = null
          _numConnections = 0
          serverPingTimes = null
          return callback(null, this)
        })
      }
    }
    return this
  }

  function updateQueryTimes (conId, queryId, estimatedQueryTime, execution_time_ms) {
    queryTimes[queryId] = execution_time_ms
  }

  function getFrontendViews (callback) {
    if (_sessionId) {
      _client[0].get_frontend_views(_sessionId[0], callback)
    } else {
      callback(new Error("No Session ID"))
    }
  }

  /**
   * Get the recent dashboards as a list of <code>TFrontendView</code> objects.
   * These objects contain a value for the <code>view_name</code> property,
   * but not for the <code>view_state</code> property.
   * @return {Promise<TFrontendView[]>} An array which has all saved dashboards.
   *
   * @example <caption>Get the list of dashboards from the server:</caption>
   *
   * con.getFrontendViewsAsync().then((results) => console.log(results))
   * // [TFrontendView, TFrontendView]
   */
  function getFrontendViewsAsync () {
    return new Promise((resolve, reject) => {
      getFrontendViews((error, views) => {
        if (error) {
          reject(error)
        } else {
          resolve(views)
        }
      })
    })
  }

  function getFrontendView (viewName, callback) {
    if (_sessionId && viewName) {
      _client[0].get_frontend_view(_sessionId[0], viewName, callback)
    } else {
      callback(new Error("No Session ID"))
    }
  }

  /**
   * Get a dashboard object containing a value for the <code>view_state</code> property.
   * This object contains a value for the <code>view_state</code> property,
   * but not for the <code>view_name</code> property.
   * @param {String} viewName the name of the dashboard
   * @return {Promise.<Object>} An object that contains all data and metadata related to the dashboard
   *
   * @example <caption>Get a specific dashboard from the server:</caption>
   *
   * con.getFrontendViewAsync('dashboard_name').then((result) => console.log(result))
   * // {TFrontendView}
   */
  function getFrontendViewAsync (viewName) {
    return new Promise((resolve, reject) => {
      getFrontendView(viewName, (err, view) => {
        if (err) {
          reject(err)
        } else {
          resolve(view)
        }
      })
    })
  }

  function getServerStatus (callback) {
    _client[0].get_server_status(_sessionId[0], callback)
  }

  /**
   * Get the status of the server as a <code>TServerStatus</code> object.
   * This includes whether the server is read-only,
   * has backend rendering enabled, and the version number.
   * @return {Promise.<Object>}
   *
   * @example <caption>Get the server status:</caption>
   *
   * con.getServerStatusAsync().then((result) => console.log(result))
   * // {
   * //   "read_only": false,
   * //   "version": "3.0.0dev-20170503-40e2de3",
   * //   "rendering_enabled": true,
   * //   "start_time": 1493840131
   * // }
   */

  function getServerStatusAsync () {
    return new Promise((resolve, reject) => {
      getServerStatus((err, result) => {
        if (err) {
          reject(err)
        } else {
          resolve(result)
        }
      })
    })
  }

  /**
   * Add a new dashboard to the server.
   * @param {String} viewName - the name of the new dashboard
   * @param {String} viewState - the base64-encoded state string of the new dashboard
   * @param {String} imageHash - the numeric hash of the dashboard thumbnail
   * @param {String} metaData - Stringified metaData related to the view
   * @return {Promise} Returns empty if success
   *
   * @example <caption>Add a new dashboard to the server:</caption>
   *
   * con.createFrontendViewAsync('newSave', 'viewstateBase64', null, 'metaData').then(res => console.log(res))
   */
  function createFrontendViewAsync (viewName, viewState, imageHash, metaData) {
    if (!_sessionId) {
      return new Promise((resolve, reject) => {
        reject(new Error("You are not connected to a server. Try running the connect method first."))
      })
    }

    return Promise.all(_client.map((client, i) => new Promise((resolve, reject) => {
      client.create_frontend_view(_sessionId[i], viewName, viewState, imageHash, metaData, (error, data) => {
        if (error) {
          reject(error)
        } else {
          resolve(data)
        }
      })
    })))
  }

  function deleteFrontendView (viewName, callback) {
    if (!_sessionId) {
      throw new Error("You are not connected to a server. Try running the connect method first.")
    }
    try { // eslint-disable-line no-restricted-syntax
      _client.forEach((client, i) => {
        // do we want to try each one individually so if we fail we keep going?
        client.delete_frontend_view(_sessionId[i], viewName, callback)
      })
    } catch (err) {
      console.log("ERROR: Could not delete the frontend view. Check your session id.", err)
    }
  }

  /**
   * Delete a dashboard object containing a value for the <code>view_state</code> property.
   * @param {String} viewName - the name of the dashboard
   * @return {Promise.<String>} Name of dashboard successfully deleted
   *
   * @example <caption>Delete a specific dashboard from the server:</caption>
   *
   * con.deleteFrontendViewAsync('dashboard_name').then(res => console.log(res))
   */
  function deleteFrontendViewAsync (viewName) {
    return new Promise((resolve, reject) => {
      deleteFrontendView(viewName, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve(viewName)
        }
      })
    })
  }

  /**
   * Create a short hash to make it easy to share a link to a specific dashboard.
   * @param {String} viewState - the base64-encoded state string of the new dashboard
   * @param {String} metaData - Stringified metaData related to the link
   * @return {Promise.<String[]>} link - A short hash of the dashboard used for URLs
   *
   * @example <caption>Create a link to the current state of a dashboard:</caption>
   *
   * con.createLinkAsync("eyJuYW1lIjoibXlkYXNoYm9hcmQifQ==", 'metaData').then(res => console.log(res));
   * // ["28127951"]
   */
  function createLinkAsync (viewState, metaData) {
    return Promise.all(_client.map((client, i) => new Promise((resolve, reject) => {
      client.create_link(_sessionId[i], viewState, metaData, (error, data) => {
        if (error) {
          reject(error)
        } else {
          const result = data.split(",").reduce((links, link) => {
            if (links.indexOf(link) === -1) { links.push(link) }
            return links
          }, [])
          if (!result || result.length !== 1) {
            reject(new Error("Different links were created on connection"))
          } else {
            resolve(result.join())
          }
        }
      })
    })))
  }

  function getLinkView (link, callback) {
    _client[0].get_link_view(_sessionId[0], link, callback)
  }

  /**
   * Get a fully-formed dashboard object from a generated share link.
   * This object contains the given link for the <code>view_name</code> property,
   * @param {String} link - the short hash of the dashboard, see {@link createLink}
   * @return {Promise.<Object>} Object of the dashboard and metadata
   *
   * @example <caption>Get a dashboard from a link:</caption>
   *
   * con.getLinkViewAsync('28127951').then(res => console.log(res))
   * //  {
   * //    "view_name": "28127951",
   * //    "view_state": "eyJuYW1lIjoibXlkYXNoYm9hcmQifQ==",
   * //    "image_hash": "",
   * //    "update_time": "2017-04-28T21:34:01Z",
   * //    "view_metadata": "metaData"
   * //  }
   */
  function getLinkViewAsync (link) {
    return new Promise((resolve, reject) => {
      getLinkView(link, (err, theLink) => {
        if (err) {
          reject(err)
        } else {
          resolve(theLink)
        }
      })
    })
  }

  function detectColumnTypes (fileName, copyParams, callback) {
    const thriftCopyParams = helpers.convertObjectToThriftCopyParams(copyParams)
    _client[0].detect_column_types(_sessionId[0], fileName, thriftCopyParams, callback)
  }

  /**
   * Asynchronously get the data from an importable file,
   * such as a .csv or plaintext file with a header.
   * @param {String} fileName - the name of the importable file
   * @param {TCopyParams} copyParams - see {@link TCopyParams}
   * @returns {Promise.<TDetectResult>} An object which has copy_params and row_set
   *
   * @example <caption>Get data from table_data.csv:</caption>
   *
   * var copyParams = new TCopyParams();
   * con.detectColumnTypesAsync('table_data.csv', copyParams).then(res => console.log(res))
   * // TDetectResult {row_set: TRowSet, copy_params: TCopyParams}
   *
   */
  function detectColumnTypesAsync (fileName, copyParams) {
    return new Promise((resolve, reject) => {
      detectColumnTypes.bind(this, fileName, copyParams)((err, res) => {
        if (err) {
          reject(err)
        } else {
          importerRowDesc = res.row_set.row_desc
          resolve(res)
        }
      })
    })
  }

  /**
   * Submit a query to the database and process the results.
   * @param {String} sql The query to perform
   * @param {Object} options the options for the query
   * @param {Function} callback that takes `(err, result) => result`
   * @returns {Object} The result of the query
   *
   * @example <caption>create a query</caption>
   *
   * var query = "SELECT count(*) AS n FROM tweets_nov_feb WHERE country='CO'";
   * var options = {};
   *
   * con.query(query, options, function(err, result) {
   *        console.log(result)
   *      });
   *
   */
  function query (sql, options, callback) {
    let columnarResults = true
    let eliminateNullRows = false
    let queryId = null
    let returnTiming = false
    let limit = -1
    if (options) {
      columnarResults = options.hasOwnProperty("columnarResults") ? options.columnarResults : columnarResults
      eliminateNullRows = options.hasOwnProperty("eliminateNullRows") ? options.eliminateNullRows : eliminateNullRows
      queryId = options.hasOwnProperty("queryId") ? options.queryId : queryId
      returnTiming = options.hasOwnProperty("returnTiming") ? options.returnTiming : returnTiming
      limit = options.hasOwnProperty("limit") ? options.limit : limit
    }

    const lastQueryTime = queryId in queryTimes ? queryTimes[queryId] : DEFAULT_QUERY_TIME

    const curNonce = (_nonce++).toString()

    const conId = 0

    const processResultsOptions = {
      returnTiming,
      eliminateNullRows,
      sql,
      queryId,
      conId,
      estimatedQueryTime: lastQueryTime
    }

    try {
      if (callback) {
        _client[conId].sql_execute(_sessionId[conId], sql, columnarResults, curNonce, limit, (error, result) => {
          if (error) {
            callback(error)
          } else {
            processResults(processResultsOptions, result, callback)
          }
        })
        return curNonce
      } else if (!callback) {
        const SQLExecuteResult = _client[conId].sql_execute(
          _sessionId[conId],
          sql,
          columnarResults,
          curNonce,
          limit
        )
        return processResults(processResultsOptions, SQLExecuteResult)
      }
    } catch (err) {
      if (err.name === "NetworkError") {
        removeConnection(conId)
        if (_numConnections === 0) {
          err.msg = "No remaining database connections"
          throw err
        }
        query(sql, options, callback)
      } else if (callback) {
        callback(err)
      } else {
        throw err
      }
    }
  }

  /**
   * Submit a query to validate whether the backend can create a result set based on the SQL statement.
   * @param {String} sql The query to perform
   * @returns {Promise.<Object>} The result of whether the query is valid
   *
   * @example <caption>create a query</caption>
   *
   * var query = "SELECT count(*) AS n FROM tweets_nov_feb WHERE country='CO'";
   *
   * con.validateQuery(query).then(res => console.log(res))
   *
   * // [{
   * //    "name": "n",
   * //    "type": "INT",
   * //    "is_array": false,
   * //    "is_dict": false
   * //  }]
   *
   */
  function validateQuery (sql) {
    return new Promise((resolve, reject) => {
      _client[0].sql_validate(_sessionId[0], sql, (error, res) => {
        if (error) {
          reject(error)
        } else {
          resolve(convertFromThriftTypes(res))
        }
      })
    })
  }

  function removeConnection (conId) {
    if (conId < 0 || conId >= numConnections) {
      const err = {
        msg: "Remove connection id invalid"
      }
      throw err
    }
    _client.splice(conId, 1)
    _sessionId.splice(conId, 1)
    _numConnections--
  }

  function getTables (callback) {
    _client[0].get_tables(_sessionId[0], (error, tables) => {
      if (error) {
        callback(error)
      } else {
        callback(null, tables.map((table) => ({
          name: table,
          label: "obs"
        })))
      }
    })
  }

  /**
   * Get the names of the databases that exist on the current session's connectdion.
   * @return {Promise.<Object[]>} list of table objects containing the label and table names.
   *
   * @example <caption>Get the list of tables from a connection:</caption>
   *
   *  con.getTablesAsync().then(res => console.log(res))
   *
   *  //  [{
   *  //    label: 'obs', // deprecated property
   *  //    name: 'myDatabaseName'
   *  //   },
   *  //  ...]
   */
  function getTablesAsync () {
    return new Promise((resolve, reject) => {
      getTables.bind(this)((error, tables) => {
        if (error) {
          reject(error)
        } else {
          resolve(tables)
        }
      })
    })
  }

  /**
   * Create an array-like object from {@link TDatumType} by
   * flipping the string key and numerical value around.
   * @param {Object} datumEnum - TODO
   * @returns {Undefined} This function does not return anything
   */
  function invertDatumTypes (datumEnum) {
    const datumType = TDatumType // eslint-disable-line no-undef
    for (const key in datumType) {
      if (datumType.hasOwnProperty(key)) {
        datumEnum[datumType[key]] = key
      }
    }
  }

  /**
   * Get a list of field objects for a given table.
   * @param {String} tableName - name of table containing field names
   * @param {Function} callback - (err, results)
   * @return {Array<Object>} fields - the formmatted list of field objects
   *
   * @example <caption>Get the list of fields from a specific table:</caption>
   *
   * con.getFields('flights', (err, res) => console.log(res))
   * // [{
   *   name: 'fieldName',
   *   type: 'BIGINT',
   *   is_array: false,
   *   is_dict: false
   * }, ...]
   */
  function getFields (tableName, callback) {
    _client[0].get_table_details(_sessionId[0], tableName, (error, fields) => {
      if (fields) {
        const rowDict = fields.row_desc.reduce((accum, value) => {
          accum[value.col_name] = value
          return accum
        }, {})
        callback(null, convertFromThriftTypes(rowDict))
      } else {
        callback(new Error("Table (" + tableName + ") not found" + error))
      }
    })
  }


  function createTable (tableName, rowDescObj, tableType, callback) {
    if (!_sessionId) {
      throw new Error("You are not connected to a server. Try running the connect method first.")
    }

    const thriftRowDesc = helpers.mutateThriftRowDesc(rowDescObj, importerRowDesc)

    for (let c = 0; c < _numConnections; c++) {
      _client[c].create_table(
        _sessionId[c],
        tableName,
        thriftRowDesc,
        tableType,
        (err) => {
          if (err) {
            callback(err)
          } else {
            callback()
          }
        }
      )
    }

  }

  /**
   * Create a table and persist it to the backend.
   * @param {String} tableName - desired name of the new table
   * @param {Array<TColumnType>} rowDescObj - fields of the new table
   * @param {Number<TTableType>} tableType - the types of tables a user can import into the db
   * @return {Promise.<undefined>} it will either catch an error or return undefined on success
   *
   * @example <caption>Create a new table:</caption>
   *
   *  con.createTable('mynewtable', [TColumnType, TColumnType, ...], 0).then(res => console.log(res));
   *  // undefined
   */
  function createTableAsync (tableName, rowDescObj, tableType) {
    return new Promise((resolve, reject) => {
      createTable(tableName, rowDescObj, tableType, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  function importTable (tableName, fileName, copyParams, rowDescObj, isShapeFile, callback) {
    if (!_sessionId) {
      throw new Error("You are not connected to a server. Try running the connect method first.")
    }

    const thriftCopyParams = helpers.convertObjectToThriftCopyParams(copyParams)
    const thriftRowDesc = helpers.mutateThriftRowDesc(rowDescObj, importerRowDesc)

    const thriftCallBack = (err, res) => {
      if (err) {
        callback(err)
      } else {
        callback(null, res)
      }
    }

    for (let c = 0; c < _numConnections; c++) {
      if (isShapeFile) {
        _client[c].import_geo_table(
          _sessionId[c],
          tableName,
          fileName,
          thriftCopyParams,
          thriftRowDesc,
          thriftCallBack
        )
      } else {
        _client[c].import_table(
          _sessionId[c],
          tableName,
          fileName,
          thriftCopyParams,
          thriftCallBack
        )
      }
    }
  }

  function importTableAsyncWrapper (isShapeFile) {
    return (tableName, fileName, copyParams, headers) => new Promise((resolve, reject) => {
      importTable(tableName, fileName, copyParams, headers, isShapeFile, (err, link) => {
        if (err) {
          reject(err)
        } else {
          resolve(link)
        }
      })
    })
  }

  /**
   * Use for backend rendering. This method will fetch a PNG image
   * that is a render of the vega json object.
   *
   * @param {Number} widgetid the widget id of the calling widget
   * @param {String} vega the vega json
   * @param {Object} options the options for the render query
   * @param {Number} options.compressionLevel the png compression level.
   *                  range 1 (low compression, faster) to 10 (high compression, slower).
   *                  Default 3.
   * @param {Function} callback takes `(err, success)` as its signature.  Returns con singleton on success.
   *
   * @returns {Image} Base 64 Image
   */
  function renderVega (widgetid, vega, options, callback) /* istanbul ignore next */ {
    let queryId = null
    let compressionLevel = COMPRESSION_LEVEL_DEFAULT
    if (options) {
      queryId = options.hasOwnProperty("queryId") ? options.queryId : queryId
      compressionLevel = options.hasOwnProperty("compressionLevel") ? options.compressionLevel : compressionLevel
    }

    const lastQueryTime = queryId in queryTimes ? queryTimes[queryId] : DEFAULT_QUERY_TIME

    const curNonce = (_nonce++).toString()

    const conId = 0
    _lastRenderCon = conId

    const processResultsOptions = {
      isImage: true,
      query: "render: " + vega,
      queryId,
      conId,
      estimatedQueryTime: lastQueryTime
    }

    try {
      if (!callback) {
        const renderResult = _client[conId].render_vega(
          _sessionId[conId],
          widgetid,
          vega,
          compressionLevel,
          curNonce
        )
        return processResults(processResultsOptions, renderResult)
      }

      _client[conId].render_vega(_sessionId[conId], widgetid, vega, compressionLevel, curNonce, (error, result) => {
        if (error) {
          callback(error)
        } else {
          processResults(processResultsOptions, result, callback)
        }
      })
    } catch (err) {
      throw err
    }

    return curNonce
  }

  /**
   * Used primarily for backend rendered maps, this method will fetch the row
   * for a specific table that was last rendered at a pixel.
   *
   * @param {widgetId} Number - the widget id of the caller
   * @param {TPixel} pixel - the pixel (lower left-hand corner is pixel (0,0))
   * @param {String} tableName - the table containing the geo data
   * @param {Object} tableColNamesMap - object of tableName -> array of col names
   * @param {Array<Function>} callbacks
   * @param {Number} [pixelRadius=2] - the radius around the primary pixel to search
   */

  function getResultRowForPixel (widgetId, pixel, tableColNamesMap, callbacks, pixelRadius = 2) /* istanbul ignore next */ {
    if (!(pixel instanceof TPixel)) { pixel = new TPixel(pixel) }
    const columnFormat = true // BOOL
    const curNonce = (_nonce++).toString()
    try {
      if (!callbacks) {
        return processPixelResults(
          undefined, // eslint-disable-line no-undefined
          _client[_lastRenderCon].get_result_row_for_pixel(
            _sessionId[_lastRenderCon],
            widgetId,
            pixel,
            tableColNamesMap,
            columnFormat,
            pixelRadius,
            curNonce
          ))
      }
      _client[_lastRenderCon].get_result_row_for_pixel(
        _sessionId[_lastRenderCon],
        widgetId,
        pixel,
        tableColNamesMap,
        columnFormat,
        pixelRadius,
        curNonce,
        processPixelResults.bind(this, callbacks)
      )
    } catch (err) {
      throw err
    }
    return curNonce
  }


  /**
   * Formats the pixel results into the same pattern as textual results.
   *
   * @param {Array<Function>} callbacks a collection of callbacks
   * @param {Object} error an error if one was thrown, otherwise null
   * @param {Array|Object} results unformatted results of pixel rowId information
   *
   * @returns {Object} An object with the pixel results formatted for display
   */
  function processPixelResults (callbacks, error, results) {
    callbacks = Array.isArray(callbacks) ? callbacks : [callbacks]
    results = Array.isArray(results) ? results.pixel_rows : [results]
    const numPixels = results.length
    const processResultsOptions = {
      isImage: false,
      eliminateNullRows: false,
      query: "pixel request",
      queryId: -2
    }
    for (let p = 0; p < numPixels; p++) {
      results[p].row_set = processResults(processResultsOptions, results[p])
    }
    if (!callbacks) {
      return results
    }
    callbacks.pop()(error, results)
  }

  /**
   * Get or set the session ID used by the server to serve the correct data.
   * This is typically set by {@link connect} and should not be set manually.
   * @param {Number} newSessionId - The session ID of the current connection
   * @return {Number|MapdCon} - The session ID or the MapdCon itself
   *
   * @example <caption>Get the session id:</caption>
   *
   *  con.sessionId();
   * // sessionID === 3145846410
   *
   * @example <caption>Set the session id:</caption>
   * var con = new MapdCon().connect().sessionId(3415846410);
   * // NOTE: It is generally unsafe to set the session id manually.
   */
  function sessionId (newSessionId) {
    if (!arguments.length) {
      return _sessionId
    }
    _sessionId = newSessionId
    return this
  }

  /**
   * Get or set the connection server hostname.
   * This is is typically the first method called after instantiating a new MapdCon.
   * @param {String} hostname - The hostname address
   * @return {String|MapdCon} - The hostname or the MapdCon itself
   *
   * @example <caption>Set the hostname:</caption>
   * var con = new MapdCon().host('localhost');
   *
   * @example <caption>Get the hostname:</caption>
   * var host = con.host();
   * // host === 'localhost'
   */
  function host (hostname) {
    if (!arguments.length) {
      return _host
    }
    _host = arrayify(hostname)
    return this
  }

  /**
   * Get or set the connection port.
   * @param {String} thePort - The port to connect on
   * @return {String|MapdCon} - The port or the MapdCon itself
   *
   * @example <caption>Set the port:</caption>
   * var con = new MapdCon().port('8080');
   *
   * @example <caption>Get the port:</caption>
   * var port = con.port();
   * // port === '8080'
   */
  function port (thePort) {
    if (!arguments.length) {
      return _port
    }
    _port = arrayify(thePort)
    return this
  }

  /**
   * Get or set the username to authenticate with.
   * @param {String} username - The username to authenticate with
   * @return {String|MapdCon} - The username or the MapdCon itself
   *
   * @example <caption>Set the username:</caption>
   * var con = new MapdCon().user('foo');
   *
   * @example <caption>Get the username:</caption>
   * var username = con.user();
   * // user === 'foo'
   */
  function user (username) {
    if (!arguments.length) {
      return _user
    }
    _user = arrayify(username)
    return this
  }

  /**
   * Get or set the user's password to authenticate with.
   * @param {String} pass - The password to authenticate with
   * @return {String|MapdCon} - The password or the MapdCon itself
   *
   * @example <caption>Set the password:</caption>
   * var con = new MapdCon().password('bar');
   *
   * @example <caption>Get the username:</caption>
   * var password = con.password();
   * // password === 'bar'
   */
  function password (pass) {
    if (!arguments.length) {
      return _password
    }
    _password = arrayify(pass)
    return this
  }

  /**
   * Get or set the name of the database to connect to.
   * @param {String} db - The database to connect to
   * @return {String|MapdCon} - The name of the database or the MapdCon itself
   *
   * @example <caption>Set the database name:</caption>
   * var con = new MapdCon().dbName('myDatabase');
   *
   * @example <caption>Get the database name:</caption>
   * var dbName = con.dbName();
   * // dbName === 'myDatabase'
   */
  function dbName (db) {
    if (!arguments.length) {
      return _dbName
    }
    _dbName = arrayify(db)
    return this
  }

  /**
   * Whether the raw queries strings will be logged to the console.
   * Used primarily for debugging and defaults to <code>false</code>.
   * @param {Boolean} loggingEnabled - Set to true to enable logging
   * @return {Boolean|MapdCon} - The current logging flag or MapdCon itself
   *
   * @example <caption>Set logging to true:</caption>
   * var con = new MapdCon().logging(true);
   *
   * @example <caption>Get the logging flag:</caption>
   * var isLogging = con.logging();
   * // isLogging === true
   */
  function logging (loggingEnabled) {
    if (typeof loggingEnabled === "undefined") {
      return _logging
    } else if (typeof loggingEnabled !== "boolean") {
      return "logging can only be set with boolean values"
    }
    _logging = loggingEnabled
    const isEnabledTxt = loggingEnabled ? "enabled" : "disabled"
    return `SQL logging is now ${isEnabledTxt}`
  }

  /**
   * The name of the platform.
   * @param {String} thePlatform - The platform, default is "mapd"
   * @return {String|MapdCon} - The platform or the MapdCon itself
   *
   * @example <caption>Set the platform name:</caption>
   * var con = new MapdCon().platform('myPlatform');
   *
   * @example <caption>Get the platform name:</caption>
   * var platform = con.platform();
   * // platform === 'myPlatform'
   */
  function platform (thePlatform) {
    if (!arguments.length) {
      return _platform
    }
    _platform = thePlatform
    return this
  }

  /**
   * Get the number of connections that are currently open.
   * @return {Number} - number of open connections
   *
   * @example <caption>Get the number of connections:</caption>
   *
   * var numConnections = con.numConnections();
   * // numConnections === 1
   */
  function numConnections () {
    return _numConnections
  }

  /**
   * The protocol to use for requests.
   * @param {String} theProtocol - http or https
   * @return {String|MapdCon} - protocol or MapdCon itself
   *
   * @example <caption>Set the protocol:</caption>
   * var con = new MapdCon().protocol('http');
   *
   * @example <caption>Get the protocol:</caption>
   * var protocol = con.protocol();
   * // protocol === 'http'
   */
  function protocol (theProtocol) {
    if (!arguments.length) {
      return _protocol
    }
    _protocol = arrayify(theProtocol)
    return this
  }

  /**
   * Generates a list of endpoints from the connection params.
   * @return {Array<String>} - list of endpoints
   *
   * @example <caption>Get the endpoints:</caption>
   * var con = new MapdCon().protocol('http').host('localhost').port('8000');
   * var endpoints = con.getEndpoints();
   * // endpoints === [ 'http://localhost:8000' ]
   */
  function getEndpoints () {
    return _host.map((oldHost, i) => _protocol[i] + "://" + oldHost + ":" + _port[i])
  }
}

// Set a global mapdcon function when mapdcon is brought in via script tag.
if (typeof module === "object" && module.exports) {
  if (!isNodeRuntime()) {
    window.MapdCon = MapdCon
  }
}
module.exports = MapdCon
export default MapdCon

import { fetch } from '../lib/http';
import { getLogger } from '../lib/logger';
const logger = getLogger('pipes-module');

/**
 * @module tinybird-sdk/pipes
 */
export default {
   /**
   * Query Pipe with optional parameters
   * 
   * @param  { string } pipeName Pipe name
   * @param  { object } [params={}] Parameters
   * @param  { string } [format=json] Result format. One of: json, csv, ndjson, parquet.
   * @return { Promise<object> } Resultset rows
   */
  queryPipe: async (pipeName, params={}, format='json') => {
    let queryString = '';
    try {
      Object.keys(params).map(k => {
        queryString += `${k}=${encodeURIComponent(params[k])}&`;
      }); 

      return fetch(`/v0/pipes/${pipeName}.${format}?${queryString}`);
    } catch (error) {
      logger.error('Error while querying pipe /v0/pipes/(.+).(json|csv|ndjson|parquet)');
      logger.debug(`Request: /v0/pipes/${pipeName}.${format}?${queryString}`);
      logger.debug(`Querystring: ${queryString}`);
      logger.debug(error);
    }
  },

  /**
   * Create a new pipe in Tinybird.
   * 
   * @param  { string } name Pipe name
   * @param  { string } sql SQL query for the pipe
   * @return { Promise<object> } API response
   */
  createPipe: async (name, sql) => {
    try {
      const body = new URLSearchParams();
      body.append('name', name);
      body.append('sql', sql);
      const options = {
        method: 'POST',
        body: body
      };

      return fetch('/v0/pipes', options);
    } catch (error) {
      logger.error('Error while creating pipe /v0/pipes');
      logger.debug(`Request: /v0/pipes with name: ${name}`);
      logger.debug(error);
    }
  },

  /**
   * Create an endpoint for a specific pipe node in Tinybird.
   * 
   * @param  { string } pipeName Pipe name
   * @param  { string } nodeName Node name
   * @return { Promise<object> } API response
   */
  createEndpoint: async (pipeName, nodeName) => {
    try {
      const options = {
        method: 'POST'
      };

      return fetch(`/v0/pipes/${pipeName}/nodes/${nodeName}/endpoint`, options);
    } catch (error) {
      logger.error(`Error while creating endpoint /v0/pipes/${pipeName}/nodes/${nodeName}/endpoint`);
      logger.debug(`Request: /v0/pipes/${pipeName}/nodes/${nodeName}/endpoint`);
      logger.debug(error);
    }
  }
};

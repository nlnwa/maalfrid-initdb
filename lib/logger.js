const LOGGER = require('./config').app;
const THREAD = '[main]';

const levels = {
  TRACE: 1,
  DEBUG: 2,
  INFO: 3,
  WARN: 4,
  ERROR: 5,
};

const configLevel = (process.env.LOG_LEVEL || '').toUpperCase();
const runLevel = levels[configLevel] || levels.INFO;

/**
 * Log messages using level
 *
 * @param {number} level
 * @param {string} messages
 */
function log(level, ...messages) {
  if (runLevel > levels[level]) return;

  const args = [new Date().toISOString(), THREAD, level, LOGGER, '-', ...messages];

  switch (levels[level]) {
    case levels.TRACE:
    case levels.DEBUG:
      console.log(...args);
      break;
    case levels.INFO:
      console.info(...args);
      break;
    case levels.WARN:
      console.warn(...args);
      break;
    case levels.ERROR:
      console.error(...args);
      break;
    default:
      log(levels.INFO, ...messages);
      break;
  }
}

/**
 * Create logger method for a level
 *
 * @param {string} level
 * @return {function}
 */
const create = (level) => (...args) => log(level, ...args);

/**
 * Export one logger method per level
 */
module.exports = {
  trace: create('TRACE'),
  debug: create('DEBUG'),
  info: create('INFO'),
  warn: create('WARN'),
  error: create('ERROR'),
};

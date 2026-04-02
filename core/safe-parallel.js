/**
 * Safe parallel execution with concurrency limits and timeout
 *
 * Prevents overload when running multiple async tasks (API calls, Puppeteer, etc.)
 *
 * Usage:
 *   const { safeParallel, safeBatch } = require('../core/safe-parallel');
 *
 *   // Run tasks with max 3 concurrent, 30s timeout each
 *   const results = await safeParallel([
 *     () => fetchArticle(url1),
 *     () => fetchArticle(url2),
 *     () => fetchArticle(url3),
 *     () => fetchArticle(url4),
 *   ], { concurrency: 3, timeout: 30000, label: 'crawl' });
 *
 *   // Batch an array through a mapper with concurrency limit
 *   const articles = await safeBatch(urls, url => fetchArticle(url), { concurrency: 3 });
 */

/**
 * Run async tasks with concurrency limit.
 * @param {Array<Function>} tasks - Array of () => Promise
 * @param {object} [opts]
 * @param {number} [opts.concurrency=3] - Max concurrent tasks
 * @param {number} [opts.timeout=60000] - Per-task timeout in ms (0 = no timeout)
 * @param {string} [opts.label='task'] - Label for logging
 * @param {boolean} [opts.stopOnError=false] - Stop all on first error
 * @returns {Array<{ value: any, error: string|null, index: number, ms: number }>}
 */
async function safeParallel(tasks, opts = {}) {
  const { concurrency = 3, timeout = 60000, label = 'task', stopOnError = false } = opts;
  const results = new Array(tasks.length);
  let nextIndex = 0;
  let stopped = false;

  async function runNext() {
    while (nextIndex < tasks.length && !stopped) {
      const i = nextIndex++;
      const start = Date.now();

      try {
        let result;
        if (timeout > 0) {
          result = await Promise.race([
            tasks[i](),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error(`Timeout after ${timeout}ms`)), timeout)
            )
          ]);
        } else {
          result = await tasks[i]();
        }

        results[i] = { value: result, error: null, index: i, ms: Date.now() - start };
      } catch (err) {
        results[i] = { value: null, error: err.message, index: i, ms: Date.now() - start };
        console.log(`   [${label}] Task ${i + 1}/${tasks.length} failed: ${err.message.substring(0, 100)}`);

        if (stopOnError) {
          stopped = true;
          return;
        }
      }
    }
  }

  // Launch `concurrency` workers
  const workers = [];
  for (let w = 0; w < Math.min(concurrency, tasks.length); w++) {
    workers.push(runNext());
  }
  await Promise.all(workers);

  // Summary
  const succeeded = results.filter(r => r && !r.error).length;
  const failed = results.filter(r => r && r.error).length;
  const totalMs = results.reduce((sum, r) => sum + (r ? r.ms : 0), 0);
  if (tasks.length > 1) {
    console.log(`   [${label}] ${succeeded}/${tasks.length} succeeded (${failed} failed, ${totalMs}ms total)`);
  }

  return results;
}

/**
 * Map an array through an async function with concurrency limit.
 * @param {Array} items - Input array
 * @param {Function} mapper - async (item, index) => result
 * @param {object} [opts] - Same as safeParallel opts
 * @returns {Array<{ value: any, error: string|null, index: number, ms: number }>}
 */
async function safeBatch(items, mapper, opts = {}) {
  const tasks = items.map((item, i) => () => mapper(item, i));
  return safeParallel(tasks, opts);
}

module.exports = { safeParallel, safeBatch };

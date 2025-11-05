// crawler.js
import axios from 'axios';
import dotenv from 'dotenv';
import { upsertRepos, ensureSchema, closePool } from './db.js';
import retry from 'async-retry';

dotenv.config();

const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
if (!GITHUB_TOKEN) {
  console.warn('Warning: GITHUB_TOKEN not set; set it in .env for local testing or rely on Actions GITHUB_TOKEN.');
}

const TARGET_REPOS = Number(process.env.TARGET_REPOS || 100000); // target count
const PAGE_SIZE = 100; // max 100 per GraphQL search page
const GRAPHQL_URL = 'https://api.github.com/graphql';

// GraphQL query: search repositories
const QUERY = `
query($queryString:String!, $first:Int!, $after:String) {
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
  search(query: $queryString, type: REPOSITORY, first: $first, after: $after) {
    repositoryCount
    pageInfo {
      endCursor
      hasNextPage
    }
    edges {
      node {
        ... on Repository {
          id
          databaseId
          name
          url
          stargazerCount
          owner {
            login
          }
        }
      }
    }
  }
}
`;

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

async function callGithub(variables) {
  // retry wrapper for transient network issues / 5xx
  return retry(async (bail, attempt) => {
    try {
      const resp = await axios.post(GRAPHQL_URL, { query: QUERY, variables }, {
        headers: {
          Authorization: GITHUB_TOKEN ? `bearer ${GITHUB_TOKEN}` : undefined,
          'Content-Type': 'application/json',
          'User-Agent': 'github-crawler'
        },
        timeout: 20000,
      });
      if (resp.status >= 500) throw new Error(`Server error ${resp.status}`);
      return resp.data;
    } catch (err) {
      // if unauthorized etc, bail (no point retrying)
      if (err.response && err.response.status === 401) {
        bail(new Error('Unauthorized - check token'));
      }
      console.log(`callGithub attempt ${attempt} failed: ${err.message}`);
      throw err; // will retry
    }
  }, {
    retries: 5,
    minTimeout: 1000,
    factor: 2,
  });
}

async function main() {
  console.log('Starting crawler. Target repos:', TARGET_REPOS);
  await ensureSchema();

  let collected = 0;
  let after = null;
  let page = 0;
  const q = 'stars:>0'; // simple query to find many repos. Adjust for other criteria.

  while (collected < TARGET_REPOS) {
    page++;
    console.log(`Requesting page ${page}, after cursor: ${after ? after.slice(0,6) + '...' : 'null'}`);
    const variables = { queryString: q, first: PAGE_SIZE, after };

    let data;
    try {
      data = await callGithub(variables);
    } catch (err) {
      console.error('Fatal error calling GitHub:', err.message);
      break;
    }

    if (data.errors) {
      console.error('GraphQL returned errors:', JSON.stringify(data.errors));
      // If it's rate-limit or query error, try sleeping a bit then continue
      await sleep(5000);
      continue;
    }

    // check rateLimit and respect it
    const rl = data.data.rateLimit;
    if (rl) {
      console.log(`RateLimit: remaining=${rl.remaining} cost=${rl.cost} resetAt=${rl.resetAt}`);
      if (rl.remaining <= 10) {
        const resetAt = new Date(rl.resetAt).getTime();
        const now = Date.now();
        const wait = Math.max(1000, resetAt - now + 2000);
        console.log(`Approaching rate limit. Sleeping ${Math.round(wait/1000)}s until reset.`);
        await sleep(wait);
        continue; // after sleeping, retry same page
      }
    }

    const search = data.data.search;
    if (!search || !search.edges || search.edges.length === 0) {
      console.log('No more results from search. Breaking.');
      break;
    }

    const repos = search.edges.map(e => {
      const node = e.node;
      return {
        repo_node_id: node.id,
        repo_db_id: node.databaseId ?? null,
        name: node.name,
        owner: node.owner?.login ?? null,
        stars: node.stargazerCount ?? 0,
        url: node.url
      };
    });

    // insert into DB in a batch
    try {
      await upsertRepos(repos);
      collected += repos.length;
      console.log(`Inserted/updated ${repos.length} rows. Total collected: ${collected}`);
    } catch (err) {
      console.error('DB upsert error:', err);
      // if DB fails, wait and retry next iteration
      await sleep(3000);
    }

    // pagination
    after = search.pageInfo.endCursor;
    if (!search.pageInfo.hasNextPage) {
      console.log('No more pages available from GitHub search.');
      break;
    }

    // quick sleep to avoid bursts (and to give DB a small pause)
    await sleep(200); // small throttle

    // safety: break if no cursor or stuck
    if (!after) break;
  }

  console.log('Crawl complete. Collected:', collected);
  await closePool();
}

main().catch(err => {
  console.error('Unhandled error:', err);
  process.exit(1);
});


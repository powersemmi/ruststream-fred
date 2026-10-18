// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

// Each guide of the earlier site taught one topic that now has a component page of its own.
const guides: Record<string, string> = {
  streams: '/components/streams/',
  lists: '/components/lists/',
  pubsub: '/components/pubsub/',
  'dead-letter': '/components/acknowledgement/dead-letter/',
  'auth-tls': '/components/broker/authentication/',
  transactions: '/components/transactions/',
  asyncapi: '/components/asyncapi/',
  testing: '/components/testing/',
};

// Pages that moved within the site.
const pages: Record<string, string> = {
  // The benchmarks explain what the architecture costs.
  benchmarks: '/architecture/benchmarks/',
};

/** Old address to its target, relative to the build's base URL. */
export function redirects(): Record<string, string> {
  return {...guides, ...pages};
}

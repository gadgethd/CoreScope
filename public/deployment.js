/* global AbortSignal */
const CoreScopeDeployment = (() => {
  const supplied = globalThis.window?.CORE_SCOPE_DEPLOYMENT;
  const marker = globalThis.document?.querySelector?.('script[data-ukmesh-app-config]');
  const ukmeshApp = supplied?.ukmeshApp === true || marker?.hasAttribute?.('data-ukmesh-app-config') === true;
  const request = (...args) => fetch(...args);
  const localPath = (value, fallback) => (
    typeof value === 'string' && /^\/(?!\/)[\w/-]*[\w-]$/.test(value) ? value : fallback
  );
  const apiBase = localPath(supplied?.apiBase, ukmeshApp ? '' : '/api');
  const wsPath = localPath(supplied?.wsPath, ukmeshApp ? '' : '/');
  const privacyUrl = localPath(supplied?.privacyUrl, '');
  const configured = !ukmeshApp || Boolean(apiBase && wsPath && privacyUrl);
  let generation = null;
  let regionScoping = false;
  let retired = false;
  let checking;
  let visibilityRevision = 0;

  function setUnavailable(value) {
    document.documentElement.toggleAttribute('data-ukmesh-data-unavailable', value);
  }

  function retire() {
    setUnavailable(true);
    if (retired) return;
    retired = true;
    window.dispatchEvent(new Event('ukmesh:privacy-unavailable'));
  }

  function usableRuntimeConfig(config) {
    // Match UKMesh's v1 shape. Generation zero is valid upstream but does
    // not authorize public measurements in either app.
    return config && typeof config === 'object' && !Array.isArray(config) &&
      config.version === 1 && typeof config.packetArcs === 'boolean' &&
      typeof config.heatmap === 'boolean' && Number.isSafeInteger(config.privacyGeneration) &&
      config.privacyGeneration > 0 && Number.isInteger(config.refreshAfterSeconds) &&
      config.refreshAfterSeconds >= 5 && config.refreshAfterSeconds <= 300;
  }

  function refresh() {
    if (!ukmeshApp || !configured) return Promise.resolve(null);
    if (checking) return checking;
    const visibility = visibilityRevision;
    checking = (async () => {
      let next = null;
      let nextRegionScoping = false;
      try {
        const response = await request(privacyUrl, {
          cache: 'no-store', credentials: 'omit', redirect: 'error',
          signal: AbortSignal.timeout(2500),
        });
        if (response.ok) {
          const config = await response.json();
          if (usableRuntimeConfig(config)) {
            next = config.privacyGeneration;
            nextRegionScoping = config.regionScoping === true;
          }
        }
      } catch { /* keep public data hidden when the authority is unavailable */ }
      if (retired) {
        if (next !== null) window.location.reload();
        return null;
      }
      if (generation !== null && (next !== generation || (regionScoping && !nextRegionScoping))) {
        regionScoping = nextRegionScoping;
        retire();
        if (next !== null) window.location.reload();
        return null;
      }
      if (next !== null) {
        const first = generation === null;
        generation = next;
        regionScoping = nextRegionScoping;
        if (visibility === visibilityRevision && !document.hidden) setUnavailable(false);
        if (first) window.dispatchEvent(new Event('ukmesh:privacy-ready'));
      }
      return generation;
    })().finally(() => { checking = undefined; });
    return checking;
  }

  function apiPath(path) {
    return apiBase + path.slice(4);
  }

  function fetchResource(path, options) {
    if (typeof path !== 'string' || !/^\/api(?:\/|$)/.test(path)) {
      return request(path, options);
    }
    const url = apiPath(path);
    if (!ukmeshApp) return request(url, options);
    return fetchPublicResource(path, url, options);
  }

  async function fetchPublicResource(path, url, options) {
    if (generation === null) await refresh();
    if (generation === null || retired) throw new Error('Public data temporarily unavailable');
    const expected = generation;
    const response = await request(url, { ...options, cache: 'no-store', credentials: 'omit', redirect: 'error' });
    // Health is an operational startup signal, neither data nor authority.
    if (!retired && path.split('?')[0] === '/api/healthz') return response;
    if (retired || response.headers.get('X-CoreScope-Privacy-Generation') !== String(expected)) {
      // Projection errors carry no generation. A normal edge denial is
      // local to the requested route and never supplies measurements.
      if (response.ok || response.status === 503) retire();
      throw new Error('Public data temporarily unavailable');
    }
    return response;
  }

  async function fetchUKMeshJson(path, { signal, timeout = 15000 } = {}) {
    // These reads go to the UKMesh owner through its same-origin edge.
    if (!ukmeshApp || !path.startsWith('/api/')) throw new Error('UKMesh data unavailable');
    const visibility = visibilityRevision;
    const canRead = () => !retired && !document.hidden && visibility === visibilityRevision && !signal?.aborted;
    const expected = await refresh();
    if (!expected || !canRead()) throw new Error('UKMesh data unavailable');
    const response = await request(path, {
      cache: 'no-store', credentials: 'omit', redirect: 'error',
      signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(timeout)]) : AbortSignal.timeout(timeout),
    });
    if (!response.ok) throw new Error('UKMesh data unavailable');
    const payload = await response.json();
    if (!canRead() || await refresh() !== expected || !canRead()) throw new Error('UKMesh data unavailable');
    return payload;
  }

  function socketUrl() {
    if (!ukmeshApp && wsPath === '/') {
      const address = globalThis.location ?? window.location;
      const proto = address.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${proto}//${address.host}`;
    }
    if (ukmeshApp && (generation === null || retired)) return null;
    const endpoint = new URL(wsPath, window.location.origin);
    endpoint.protocol = endpoint.protocol === 'https:' ? 'wss:' : 'ws:';
    if (ukmeshApp) endpoint.searchParams.set('privacyGeneration', String(generation));
    return endpoint.href;
  }

  if (ukmeshApp) {
    setUnavailable(true);
    if (configured) {
      function poll() {
        void refresh().finally(() => window.setTimeout(poll, 2000));
      }
      poll();
      document.addEventListener('visibilitychange', () => {
        visibilityRevision++;
        setUnavailable(true);
        if (!document.hidden) {
          // A check started before the tab became visible cannot reveal it.
          if (checking) void checking.then(refresh);
          else void refresh();
        }
      });
      window.addEventListener('pageshow', (event) => {
        if (event.persisted) retire();
        void refresh();
      });
    }
    if (supplied?.ukmeshPwa === true) {
      const manifest = document.createElement('link');
      manifest.rel = 'manifest';
      manifest.href = '/manifest.json';
      document.head.append(manifest);
      const workers = globalThis.navigator?.serviceWorker;
      if (workers) {
        // Preserve the workspace's root worker and its shared install state.
        void workers.getRegistration('/').then(existing => {
          if (!existing) return workers.register('/sw.js', { type: 'module', scope: '/', updateViaCache: 'none' });
        }).catch(() => { /* Browser policy or offline startup can disable installation. */ });
      }
    }
  }
  return {
    ukmeshApp, apiPath, fetch: fetchResource, fetchUKMeshJson, socketUrl, refresh,
    regionScoping: () => regionScoping,
  };
})();

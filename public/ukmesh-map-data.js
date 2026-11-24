const UKMeshMapData = (() => {
  const regionName = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
  const roles = { 1: 'companion', 2: 'repeater', 3: 'room', 4: 'sensor' };

  function scopedUrl(path, region, values = {}) {
    const query = new URLSearchParams({ network: 'ukmesh', ...values });
    if (region) query.set('region', region);
    return path + '?' + query.toString();
  }

  function coordinates({ lat, lon }) {
    return Number.isFinite(lat) && Number.isFinite(lon) &&
      Math.abs(lat) <= 90 && Math.abs(lon) <= 180 && (lat !== 0 || lon !== 0);
  }

  async function regions(signal) {
    const result = await CoreScopeDeployment.fetchUKMeshJson(scopedUrl('/api/regions/summary'), { signal });
    if (!Array.isArray(result.regions)) throw new Error('Invalid region list');
    return [...new Set(result.regions.map(row => row?.name).filter(name =>
      typeof name === 'string' && regionName.test(name)))].sort();
  }

  async function regionNodes(region, limit, signal) {
    if (!regionName.test(region) || !CoreScopeDeployment.regionScoping()) throw new Error('Regions unavailable');
    const found = new Map();
    const cursors = new Set();
    let snapshot;
    let cursor;
    do {
      const request = {
        fields: 'node_id,name,lat,lon,role,iata,last_seen,is_online,advert_count,elevation_m',
        limit: String(Math.min(1000, limit - found.size)),
      };
      if (snapshot) request.snapshot = snapshot;
      if (cursor) request.cursor = cursor;
      const result = await CoreScopeDeployment.fetchUKMeshJson(scopedUrl('/api/nodes/map', region, request),
        { signal, timeout: 4000 });
      const page = result.page;
      if (!Array.isArray(result.nodes) || !page || typeof page.snapshot !== 'string' ||
          (snapshot && page.snapshot !== snapshot) || (!page.complete &&
            (typeof page.nextCursor !== 'string' || cursors.has(page.nextCursor) || !result.nodes.length))) {
        throw new Error('Invalid region node snapshot');
      }
      snapshot = page.snapshot;
      for (const node of result.nodes) {
        if (typeof node?.node_id !== 'string' || !node.node_id || !coordinates(node)) continue;
        const key = node.node_id.toLowerCase();
        found.set(key, { ...node, public_key: key, role: roles[node.role] ?? 'unknown' });
        if (found.size >= limit) break;
      }
      cursor = page.complete ? null : page.nextCursor;
      if (cursor) cursors.add(cursor);
    } while (cursor && found.size < limit);
    return [...found.values()];
  }

  function scopedFeed(view, feed) {
    const lifetime = view.lifetime.signal;
    const content = feed.querySelector('.panel-content');
    const status = document.createElement('p');
    const entries = document.createElement('div');
    const clock = new Intl.DateTimeFormat(undefined, { timeStyle: 'short' });
    status.setAttribute('role', 'status');
    content.replaceChildren(status, entries);
    let timer;
    let controller;
    let last = '';

    async function refresh() {
      if (lifetime.aborted || document.hidden || feed.classList.contains('hidden') ||
          !CoreScopeDeployment.regionScoping()) return;
      const request = new AbortController();
      controller = request;
      const signal = AbortSignal.any([request.signal, lifetime]);
      try {
        const rows = await CoreScopeDeployment.fetchUKMeshJson(
          scopedUrl('/api/packets/recent', view.region, { limit: '12' }), { signal, timeout: 4000 },
        );
        if (signal.aborted) return;
        if (!Array.isArray(rows)) throw new Error('Invalid recent packet list');
        const unique = new Map();
        for (const row of rows) {
          if (typeof row?.packet_hash !== 'string' || unique.has(row.packet_hash)) continue;
          unique.set(row.packet_hash, {
            summary: typeof row.summary === 'string' ? row.summary : 'Mesh packet',
            time: Date.parse(row.time),
          });
        }
        const signature = JSON.stringify([...unique]);
        if (signature !== last) {
          entries.replaceChildren(...[...unique.values()].map(({ summary, time }) => {
            const entry = document.createElement('div');
            entry.className = 'live-feed-item';
            entry.textContent = summary;
            if (Number.isFinite(time)) {
              const stamp = document.createElement('time');
              stamp.className = 'feed-time';
              stamp.dataset.ts = String(time);
              stamp.dateTime = new Date(time).toISOString();
              stamp.textContent = clock.format(time);
              entry.append(' ', stamp);
            }
            return entry;
          }));
          last = signature;
        }
        status.textContent = unique.size ? '' : 'No packets in this region yet';
      } catch {
        if (!signal.aborted) status.textContent = 'Could not refresh region packets';
      } finally {
        if (!signal.aborted) timer = setTimeout(refresh, 5000);
      }
    }

    function sync() {
      clearTimeout(timer);
      controller?.abort();
      void refresh();
    }
    document.addEventListener('visibilitychange', sync, { signal: lifetime });
    lifetime.addEventListener('abort', sync, { once: true });
    sync();
    return sync;
  }

  return { scopedUrl, coordinates, regions, regionNodes, scopedFeed };
})();

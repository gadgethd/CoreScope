/* global L */
const UKMeshMapHistory = (() => {
  const windowMinutes = 360;
  const bucketMinutes = 15;
  const maxBuckets = 1 + windowMinutes / bucketMinutes;
  const integerCount = count => Number.isSafeInteger(count) && count >= 0;
  const countLabel = (count, noun) => `${count.toLocaleString()} ${noun}${count === 1 ? '' : 's'}`;

  async function readTimeline(region, signal) {
    const url = UKMeshMapData.scopedUrl('/api/activity/timeline', region, {
      minutes: String(windowMinutes), bucket: String(bucketMinutes),
    });
    const result = await CoreScopeDeployment.fetchUKMeshJson(url, { signal });
    if (result?.region !== region || result.windowMinutes !== windowMinutes || result.bucketMinutes !== bucketMinutes ||
        !Array.isArray(result.buckets) || result.buckets.length > maxBuckets) {
      throw new Error('Invalid named region timeline');
    }
    let previous = -Infinity;
    for (const bucket of result.buckets) {
      const time = typeof bucket?.time === 'string' ? Date.parse(bucket.time) : NaN;
      if (!Number.isFinite(time) || time <= previous ||
          !integerCount(bucket.packetCount) || !integerCount(bucket.observerCount) ||
          !Array.isArray(bucket.activeNodeIds) || bucket.activeNodeIds.length > 250 ||
          !bucket.activeNodeIds.every(id => typeof id === 'string' && id.length > 0 && id.length <= 64)) {
        throw new Error('Invalid activity bucket');
      }
      previous = time;
    }
    return result.buckets;
  }

  function start(map, tools, view, visibleNodes) {
    const lifetime = view.lifetime.signal;
    const details = document.createElement('details');
    details.className = 'ukmesh-activity-replay';
    details.setAttribute('aria-label', 'Region activity replay');
    const summary = document.createElement('summary');
    summary.textContent = 'Activity replay';
    const caption = document.createElement('span');
    const notice = document.createElement('p');
    notice.setAttribute('role', 'status');
    const body = document.createElement('div');
    body.hidden = true;
    const slider = document.createElement('input');
    slider.type = 'range';
    slider.min = '0';
    slider.step = '1';
    slider.setAttribute('aria-label', 'Region replay time');
    const resume = document.createElement('button');
    resume.type = 'button';
    resume.className = 'btn-secondary';
    resume.textContent = 'Return to live';
    const explanation = document.createElement('p');
    explanation.textContent = 'Activity uses 15-minute intervals and current node positions.';
    summary.append(' ', caption);
    body.append(slider, resume, explanation);
    details.append(summary, notice, body);
    tools.append(details);

    const chooseTime = new Intl.DateTimeFormat(undefined, { weekday: 'short', hour: '2-digit', minute: '2-digit' });
    const highlights = L.layerGroup().addTo(map);
    const replayTime = () => new URLSearchParams(location.search).get('replay');
    let buckets;
    let controller;

    function select(index, updateUrl) {
      const bucket = buckets[index];
      highlights.clearLayers();
      slider.value = String(index);
      resume.disabled = !bucket;
      caption.textContent = bucket ? chooseTime.format(Date.parse(bucket.time)) : 'Live';
      slider.setAttribute('aria-valuetext', caption.textContent);
      if (bucket) {
        const members = visibleNodes();
        const seen = new Set();
        const theme = getComputedStyle(document.documentElement);
        const color = theme.getPropertyValue('--link-color').trim() || theme.getPropertyValue('--text').trim();
        for (const id of bucket.activeNodeIds) {
          const key = id.toLowerCase();
          if (!Object.hasOwn(members, key) || seen.has(key)) continue;
          const node = members[key];
          if (!UKMeshMapData.coordinates(node)) continue;
          seen.add(key);
          L.circleMarker([node.lat, node.lon], {
            radius: 11, interactive: false, fill: false, weight: 3,
            color, className: 'ukmesh-replay-highlight',
          }).addTo(highlights);
        }
        notice.textContent = [countLabel(bucket.packetCount, 'packet'), countLabel(bucket.observerCount, 'observer'),
          countLabel(seen.size, 'node') + ' highlighted'].join(' · ');
      } else {
        notice.textContent = 'Showing current live state';
      }
      if (updateUrl) {
        const url = new URL(location.href);
        if (bucket) url.searchParams.set('replay', bucket.time);
        else url.searchParams.delete('replay');
        history.replaceState(null, '', url);
      }
    }

    async function load() {
      if (!details.open || controller || lifetime.aborted || document.hidden ||
          !CoreScopeDeployment.regionScoping()) return;
      const request = new AbortController();
      controller = request;
      const signal = AbortSignal.any([lifetime, request.signal]);
      notice.textContent = 'Loading regional history…';
      try {
        const data = await readTimeline(view.region, signal);
        if (signal.aborted) return;
        buckets = data;
        if (!buckets.length) {
          highlights.clearLayers();
          body.hidden = true;
          caption.textContent = 'Live';
          notice.textContent = 'No activity in this region over the past six hours';
          return;
        }
        slider.max = String(buckets.length);
        body.hidden = false;
        const requestedTime = replayTime();
        const restored = buckets.findIndex(bucket => bucket.time === requestedTime);
        select(restored >= 0 ? restored : buckets.length, false);
      } catch {
        if (!signal.aborted) notice.textContent = 'Could not load history for this region';
      } finally {
        if (controller === request) controller = undefined;
      }
    }

    function sync() {
      if (details.open && !document.hidden) {
        void load();
      } else {
        controller?.abort();
        controller = undefined;
      }
    }
    slider.addEventListener('input', () => select(Number(slider.value), true));
    resume.addEventListener('click', () => select(buckets.length, true));
    details.addEventListener('toggle', sync);
    document.addEventListener('visibilitychange', sync, { signal: lifetime });
    lifetime.addEventListener('abort', () => {
      controller?.abort();
      highlights.remove();
    }, { once: true });
    if (replayTime()) details.open = true;
  }

  return { start };
})();

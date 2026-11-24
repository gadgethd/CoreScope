/* global L */
const UKMeshMapLayers = (() => {
  const points = node => [node.lat, node.lon];
  const isNodeId = key => typeof key === 'string' && /^[\da-f]{64}$/i.test(key);
  const { scopedUrl, coordinates } = UKMeshMapData;
  const shade = css => getComputedStyle(document.documentElement).getPropertyValue(css).trim();

  function popupText(title, detail) {
    const popup = document.createElement('div');
    const name = document.createElement('strong');
    const info = document.createElement('p');
    name.textContent = title;
    info.textContent = detail;
    popup.append(name, info);
    return { popup, info };
  }

  function observedLinkColor(loss) {
    if (!Number.isFinite(loss)) return shade('--text-muted');
    if (loss <= 121.5) return shade('--status-green-text');
    if (loss <= 129.5) return shade('--status-yellow-text');
    return shade('--status-red-text');
  }

  function start(map, controls, view, legacy, nodes) {
    const panel = document.createElement('details');
    panel.className = 'ukmesh-map-layers';
    const title = document.createElement('summary');
    title.textContent = 'Map layers';
    panel.append(title);
    controls.append(panel);
    const layers = legacy?.layers;
    const linkRenderer = L.svg();
    view.lifetime.signal.addEventListener('abort', () => linkRenderer?.remove(), { once: true });

    function addToggle(key, name, defaultChecked, draw) {
      const group = L.layerGroup().addTo(map);
      const label = document.createElement('label');
      const toggle = document.createElement('input');
      const status = document.createElement('span');
      toggle.type = 'checkbox';
      toggle.checked = layers ? layers.has(key) : defaultChecked;
      status.setAttribute('role', 'status');
      label.append(toggle, ' ' + name);
      panel.append(label, status);
      let controller;
      async function update() {
        controller?.abort();
        group.clearLayers();
        status.textContent = '';
        if (!toggle.checked || view.lifetime.signal.aborted ||
            (key !== 'terrain' && view.region && !CoreScopeDeployment.regionScoping())) return;
        const request = new AbortController();
        controller = request;
        const signal = AbortSignal.any([request.signal, view.lifetime.signal]);
        try {
          const result = await draw(group, signal);
          if (!signal.aborted) status.textContent = result;
        } catch {
          if (!signal.aborted) status.textContent = name + ' unavailable';
        }
      }
      toggle.addEventListener('change', () => {
        const enabled = new URLSearchParams(location.search).get('layers');
        const next = new Set(enabled === null ? ['feed'] : enabled.split(',').filter(Boolean));
        if (toggle.checked) next.add(key);
        else next.delete(key);
        const url = new URL(location.href);
        url.searchParams.set('layers', [...next].join(','));
        history.replaceState(null, '', url);
        void update();
      });
      view.lifetime.signal.addEventListener('abort', () => {
        controller?.abort();
        group.remove();
      }, { once: true });
      void update();
      return update;
    }

    addToggle('terrain', 'Terrain shading', legacy?.mode === 'plan', async (group, signal) => {
      UKMeshTerrain.layer(signal).addTo(group);
      return '';
    });

    addToggle('observers', 'Observers', false, async (group, signal) => {
      const result = await CoreScopeDeployment.fetchUKMeshJson(scopedUrl('/api/observers/health'), { signal });
      if (signal.aborted) return '';
      if (!Array.isArray(result)) throw new Error('Invalid observer list');
      const visible = result.filter(item => coordinates(item) && typeof item.node_id === 'string'
        && (!view.region || nodes()[item.node_id.toLowerCase()]));
      for (const node of visible) {
        const { popup } = popupText(node.name || 'Observer', `48-hour uptime ${node.uptime_pct}%`);
        const marker = L.circleMarker(points(node), {
          radius: 6, color: shade('--link-color'), fillOpacity: 0.8, weight: 2,
        });
        marker.bindPopup(popup).addTo(group);
      }
      return visible.length ? `${visible.length} observer${visible.length === 1 ? '' : 's'}` : 'No observers in this view';
    });

    addToggle('paths', 'Paths', legacy?.mode === 'diagnose', async (group, signal) => {
      const response = await CoreScopeDeployment.fetchUKMeshJson(scopedUrl('/api/path-beta/multibyte-paths', view.region), { signal });
      if (signal.aborted) return '';
      if (!Array.isArray(response.segments)) throw new Error('Invalid path list');
      let count = 0;
      for (const segment of response.segments) {
        const positions = segment?.positions;
        if (!Array.isArray(positions) || positions.length !== 2 ||
            !positions.every(pos => Array.isArray(pos) && coordinates({ lat: pos[0], lon: pos[1] }))) continue;
        const validCount = Number.isFinite(segment.count) && segment.count > 0 ? segment.count : 1;
        L.polyline(positions, {
          color: '#00c4ff', weight: Math.min(6, 1 + Math.log2(validCount)), opacity: 0.65,
        }).bindPopup(popupText('Observed path', `${validCount} observations`).popup).addTo(group);
        count++;
      }
      return count ? `${count} observed path${count === 1 ? '' : 's'}` : 'No observed paths in this view';
    });

    const updateLinks = addToggle('links', 'Selected repeater links', legacy?.mode === 'plan', async (group, signal) => {
      const key = view.selectedNode;
      const selected = nodes()[key];
      if (!isNodeId(key) || !selected || !coordinates(selected)) return 'Select a repeater';
      const result = await CoreScopeDeployment.fetchUKMeshJson(scopedUrl(`/api/nodes/${key}/links`, view.region), { signal });
      if (signal.aborted) return '';
      if (!Array.isArray(result)) throw new Error('Invalid link list');
      let count = 0;
      for (const link of result) {
        if (!isNodeId(link.peer_id)) continue;
        const peer = nodes()[link.peer_id.toLowerCase()];
        if (!peer || !coordinates(peer)) continue;
        const { popup, info } = popupText(peer.name || 'Repeater', `${link.observed_count} observations`);
        const history = document.createElement('button');
        history.type = 'button';
        history.textContent = 'Load 72-hour link history';
        popup.append(history);
        history.addEventListener('click', async () => {
          history.disabled = true;
          try {
            const pair = encodeURIComponent([key, link.peer_id].join(':'));
            const report = await CoreScopeDeployment.fetchUKMeshJson(scopedUrl(`/api/links/${pair}/history`, null, { hours: '72' }), { signal });
            if (!signal.aborted) {
              const latest = report.points?.at(-1);
              info.textContent = latest ? [
                `${report.points.length} radio samples over 72 hours`,
                Number.isFinite(latest.snr) ? `Last SNR ${latest.snr.toFixed(1)} dB` : '',
                Number.isFinite(latest.path_loss) ? `Last path loss ${latest.path_loss.toFixed(1)} dB` : '',
              ].filter(Boolean).join(' · ') : 'No 72-hour radio samples';
            }
          } catch {
            if (!signal.aborted) info.textContent = 'Link history unavailable';
          } finally {
            history.disabled = false;
          }
        });
        const line = L.polyline([points(selected), points(peer)], {
          color: observedLinkColor(link.itm_path_loss_db) || '#64748b', weight: 3,
          renderer: linkRenderer, className: 'ukmesh-observed-link',
        }).bindPopup(popup);
        line.on('add', () => {
          const target = line.getElement();
          if (!target) return;
          target.setAttribute('tabindex', '0');
          target.setAttribute('role', 'button');
          target.setAttribute('aria-label', `Observed link to ${peer.name || 'repeater'}`);
          target.addEventListener('keydown', event => {
            if (event.key === ' ' || event.key === 'Enter') {
              event.preventDefault();
              line.openPopup();
            }
          });
        });
        line.addTo(group);
        count++;
      }
      return count ? `${count} observed link${count === 1 ? '' : 's'}` : 'No observed links in this view';
    });

    return () => { void updateLinks(); };
  }

  return { start };
})();

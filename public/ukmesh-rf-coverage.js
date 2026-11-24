/* global L */
const UKMeshRfCoverage = (() => {
  const tileSize = 256;
  const maxLatitude = 85.0511287798066;
  const sourceImage = /^tiles\/(standard|precision)\/[0-9]+-[0-9]+\.png$/;

  function tileBounds({ x, y, z }) {
    const scale = 2 ** z;
    const lat = row => Math.atan(Math.sinh(Math.PI * (1 - 2 * row / scale))) * 180 / Math.PI;
    return { West: x / scale * 360 - 180, East: (x + 1) / scale * 360 - 180, North: lat(y), South: lat(y + 1) };
  }

  function validTile(tile, tier) {
    if (typeof tile?.image !== 'string' || sourceImage.exec(tile.image)?.[1] !== tier) return false;
    const bounds = tile.bounds;
    return Boolean(bounds && ['South', 'North', 'West', 'East'].every(key => Number.isFinite(bounds[key]))
      && bounds.South < bounds.North && bounds.West < bounds.East);
  }

  function draw(ctx, bitmap, source, { x, y, z }, visible) {
    const west = Math.max(source.West, visible.West);
    const east = Math.min(source.East, visible.East);
    const north = Math.min(source.North, visible.North);
    const south = Math.max(source.South, visible.South);
    const sx = lon => (lon - source.West) / (source.East - source.West) * bitmap.width;
    const sy = lat => (source.North - lat) / (source.North - source.South) * bitmap.height;
    const dx = lon => (((lon + 180) / 360) * 2 ** z - x) * tileSize;
    const dy = lat => {
      const clamped = Math.max(-maxLatitude, Math.min(maxLatitude, lat)) * Math.PI / 180;
      const mercatorY = (1 - Math.asinh(Math.tan(clamped)) / Math.PI) / 2;
      return (mercatorY * 2 ** z - y) * tileSize;
    };
    ctx.drawImage(bitmap, sx(west), sy(north), sx(east) - sx(west), sy(south) - sy(north),
      dx(west), dy(north), dx(east) - dx(west), dy(south) - dy(north));
  }

  function rasterLayer(tiles, tier, revision, signal) {
    const RfGrid = L.GridLayer.extend({
      createTile(coords, done) {
        const canvas = document.createElement('canvas');
        canvas.className = 'ukmesh-rf-tile';
        canvas.width = canvas.height = tileSize;
        const ctx = canvas.getContext('2d');
        ctx.imageSmoothingEnabled = false;
        const visible = tileBounds(coords);
        const overlap = tiles.filter(({ bounds }) => bounds.West < visible.East && bounds.East > visible.West
          && bounds.South < visible.North && bounds.North > visible.South);
        // Source rectangles can overlap. Paint in manifest order and release
        // the decoded image after each copy instead of retaining a second tile cache.
        (async () => {
          for (const tile of overlap) {
            if (signal.aborted) return;
            try {
              const url = '/rf-coverage/' + tile.image + '?revision=' + encodeURIComponent(revision);
              const response = await fetch(url, { cache: 'force-cache', credentials: 'omit', redirect: 'error', signal });
              if (!response.ok) continue;
              const bitmap = await createImageBitmap(await response.blob());
              try {
                if (!signal.aborted) draw(ctx, bitmap, tile.bounds, coords, visible);
              } finally {
                bitmap.close();
              }
            } catch { /* An individual missing tile must not hide completed tiles. */ }
          }
          if (!signal.aborted) done(null, canvas);
        })();
        return canvas;
      },
    });
    const bounds = [
      tiles.reduce(([lat, lon], tile) => [Math.min(lat, tile.bounds.South), Math.min(lon, tile.bounds.West)], [90, 180]),
      tiles.reduce(([lat, lon], tile) => [Math.max(lat, tile.bounds.North), Math.max(lon, tile.bounds.East)], [-90, -180]),
    ];
    return new RfGrid({
      opacity: 0.72, bounds, noWrap: true, keepBuffer: 1, maxNativeZoom: tier === 'precision' ? 10 : 9,
      updateWhenIdle: true,
    });
  }

  function start(map, tools, layers) {
    const row = document.createElement('div');
    row.className = 'ukmesh-rf-controls';
    const label = document.createElement('label');
    const enabled = document.createElement('input');
    enabled.type = 'checkbox';
    const saved = localStorage.getItem('meshcore-rf-coverage-visible-v1');
    enabled.checked = layers ? layers.has('coverage') : saved === '1';
    label.append(enabled, ' RF coverage');
    const tier = document.createElement('select');
    tier.setAttribute('aria-label', 'RF coverage tier');
    for (const [value, label] of [['standard', 'Standard'], ['precision', 'Precision']]) tier.add(new Option(label, value));
    tier.value = localStorage.getItem('meshcore-rf-coverage-tier-v1') === 'precision' ? 'precision' : 'standard';
    const status = document.createElement('span');
    status.setAttribute('role', 'status');
    row.append(label, tier, status);
    tools.append(row);

    let currentLayer;
    let dataAbort;
    let tilesAbort;
    let timer;
    let activeSignature;
    let closed = false;

    function clearLayer() {
      tilesAbort?.abort();
      currentLayer?.remove();
      currentLayer = undefined;
      activeSignature = undefined;
    }

    async function refresh() {
      if (!enabled.checked || document.hidden || closed) return;
      const selected = tier.value;
      const controller = new AbortController();
      dataAbort = controller;
      const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(4000)]);
      let delay = 3000;
      try {
        const response = await fetch('/rf-coverage/meta.json', { cache: 'no-store', credentials: 'omit', redirect: 'error', signal });
        if (!response.ok) throw new Error('RF coverage unavailable');
        const meta = await response.json();
        if (controller.signal.aborted) return;
        const data = meta.coverage?.[selected]?.tiles;
        const tiles = Array.isArray(data) ? data.filter(tile => validTile(tile, selected)) : [];
        const progress = meta.run?.tiers?.[selected];
        const revision = `${meta.run?.id ?? meta.generated_at ?? 'unknown'}-${progress?.completed_tiles ?? tiles.length}`;
        const signature = JSON.stringify([selected, revision, tiles]);
        if (activeSignature !== signature) {
          clearLayer();
          if (tiles.length) {
            tilesAbort = new AbortController();
            currentLayer = rasterLayer(tiles, selected, revision, tilesAbort.signal).addTo(map);
            activeSignature = signature;
          }
        }
        if (progress?.state === 'failed') {
          status.textContent = 'Coverage update failed';
        } else if (Number.isSafeInteger(progress?.total_tiles) && progress.total_tiles > 0) {
          status.textContent = `${progress.completed_tiles} of ${progress.total_tiles} tiles ready`;
        } else {
          status.textContent = tiles.length ? 'Coverage ready' : 'Coverage unavailable';
        }
        if (meta.complete) delay = 30000;
      } catch {
        if (!controller.signal.aborted) status.textContent = 'Coverage refresh unavailable';
      } finally {
        if (!controller.signal.aborted && !closed) timer = setTimeout(refresh, delay);
      }
    }

    function sync() {
      clearTimeout(timer);
      dataAbort?.abort();
      tier.disabled = !enabled.checked;
      if (!enabled.checked) {
        clearLayer();
        status.textContent = '';
      } else if (!document.hidden) {
        void refresh();
      }
    }

    enabled.addEventListener('change', () => {
      localStorage.setItem('meshcore-rf-coverage-visible-v1', enabled.checked ? '1' : '0');
      sync();
    });
    tier.addEventListener('change', () => {
      localStorage.setItem('meshcore-rf-coverage-tier-v1', tier.value);
      clearLayer();
      sync();
    });
    document.addEventListener('visibilitychange', sync);
    sync();
    return () => {
      closed = true;
      clearTimeout(timer);
      dataAbort?.abort();
      clearLayer();
      document.removeEventListener('visibilitychange', sync);
    };
  }

  return { start };
})();

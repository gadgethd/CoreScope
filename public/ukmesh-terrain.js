/* global L */
const UKMeshTerrain = (() => {
  const size = 256;
  const equatorMetres = 40075016.686;

  function shade(context, metresPerPixel) {
    const terrain = context.getImageData(0, 0, size, size).data;
    const output = context.createImageData(size, size);
    const elevation = (x, y) => {
      const offset = 4 * (Math.min(size - 1, Math.max(0, y)) * size + Math.min(size - 1, Math.max(0, x)));
      return terrain[offset] * 256 + terrain[offset + 1] + terrain[offset + 2] / 256 - 32768;
    };
    for (let y = 0; y < size; y++) {
      for (let x = 0; x < size; x++) {
        const east = (elevation(x + 1, y) - elevation(x - 1, y)) / (2 * metresPerPixel);
        const south = (elevation(x, y + 1) - elevation(x, y - 1)) / (2 * metresPerPixel);
        const brightness = (east + south) * 0.5 / Math.sqrt(1 + east * east + south * south);
        const offset = (y * size + x) * 4;
        const tone = brightness >= 0 ? 255 : 0;
        output.data[offset] = output.data[offset + 1] = output.data[offset + 2] = tone;
        output.data[offset + 3] = Math.abs(brightness) * 255;
      }
    }
    context.putImageData(output, 0, 0);
  }

  function layer(signal) {
    const requests = new WeakMap();
    const ShadeGrid = L.GridLayer.extend({
      createTile(coords, done) {
        const canvas = document.createElement('canvas');
        canvas.className = 'ukmesh-terrain-tile';
        canvas.width = canvas.height = size;
        const request = new AbortController();
        requests.set(canvas, request);
        const tileSignal = AbortSignal.any([signal, request.signal, AbortSignal.timeout(8000)]);
        void (async () => {
          try {
            const url = `/terrain-tiles/${coords.z}/${coords.x}/${coords.y}.png`;
            const response = await fetch(url, { cache: 'force-cache', credentials: 'omit', redirect: 'error', signal: tileSignal });
            if (!response.ok) throw new Error('Terrain tile unavailable');
            const bitmap = await createImageBitmap(await response.blob());
            try {
              if (tileSignal.aborted) return;
              if (bitmap.width !== size || bitmap.height !== size) throw new Error('Invalid terrain tile size');
              const context = canvas.getContext('2d', { willReadFrequently: true });
              context.drawImage(bitmap, 0, 0);
              const mercator = Math.PI * (1 - 2 * (coords.y + 0.5) / 2 ** coords.z);
              shade(context, equatorMetres / (Math.cosh(mercator) * size * 2 ** coords.z));
            } finally {
              bitmap.close();
            }
            if (!tileSignal.aborted) done(null, canvas);
          } catch (error) {
            if (!signal.aborted && !request.signal.aborted) done(error, canvas);
          } finally {
            requests.delete(canvas);
          }
        })();
        return canvas;
      },
    });
    const result = new ShadeGrid({ minZoom: 7, maxNativeZoom: 12, keepBuffer: 1, noWrap: true });
    result.on('tileunload', event => requests.get(event.tile)?.abort());
    return result;
  }

  return { layer };
})();

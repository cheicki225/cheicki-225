const CACHE = 'arb-radar-v1';
self.addEventListener('install', (e) => { self.skipWaiting(); });
self.addEventListener('activate', (e) => { self.clients.claim(); });
self.addEventListener('fetch', (e) => {
  if (e.request.method !== 'GET' || e.request.url.includes('/api/') || e.request.url.includes('/ws/')) return;
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});

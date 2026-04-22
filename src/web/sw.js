// Minimal service worker — just enables "Add to Home Screen" install prompt.
// No aggressive caching to avoid stale data; all API calls go to the network.
var CACHE = 'shinra-v1';

self.addEventListener('install', function(event) {
  self.skipWaiting();
});

self.addEventListener('activate', function(event) {
  event.waitUntil(self.clients.claim());
});

// Network-first for everything: we rely on the browser's HTTP cache.
self.addEventListener('fetch', function(event) {
  // Only handle GET requests
  if (event.request.method !== 'GET') return;
  event.respondWith(
    fetch(event.request).catch(function() {
      return caches.match(event.request);
    })
  );
});

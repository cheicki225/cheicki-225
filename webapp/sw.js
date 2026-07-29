// Service worker minimal — juste nécessaire pour que le navigateur
// considère l'app comme "installable" (critère PWA). Pas de cache
// offline complexe : les données doivent toujours être fraîches.
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', () => self.clients.claim());
self.addEventListener('fetch', () => {}); // laisse passer toutes les requêtes normalement

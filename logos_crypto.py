"""
Logos des cryptos — couverture étendue via CoinGecko
==========================================================
La bibliothèque `cryptocurrency-icons` utilisée jusqu'ici côté frontend ne
couvre qu'environ 300 cryptos "historiques" (BTC, ETH, les gros altcoins).
La majorité des tokens réellement suivis par le bot (CARV, ZBCN, SXT, SWELL,
CLOUD, COOKIE, ZENT, ZEUS, ARX...) n'y sont pas et retombent tous sur la même
icône générique.

Ce module interroge périodiquement l'API publique de CoinGecko (18 000+
cryptos, sans clé d'API) pour construire une table {TICKER: url_du_logo},
servie ensuite au frontend via /api/logos.

⚠️ COLLISIONS DE TICKER : plusieurs cryptos différentes peuvent partager le
même ticker sur CoinGecko. L'endpoint /coins/markets étant trié par
capitalisation décroissante, on garde la PREMIÈRE occurrence de chaque ticker
— donc la plus grosse capitalisation, celle qui correspond presque toujours à
ce qui est listé sur les exchanges majeurs. Ce n'est pas infaillible : sur un
ticker ambigu, le logo affiché pourrait être celui d'une autre crypto.

⚠️ LIMITE DE DÉBIT : l'API publique sans clé tolère environ 5 à 15 appels par
minute. On espace donc les pages, et on ne rafraîchit que toutes les 12h par
défaut (un logo ne change quasiment jamais).

⚠️ Non testable dans mon environnement (pas d'accès réseau sortant vers
CoinGecko). La logique de parsing a été testée avec des réponses simulées au
format documenté. Vérifie les logs après déploiement :
    logos_crypto : N logos en cache
"""

import asyncio
import logging
import platform

import aiohttp

log = logging.getLogger("logos_crypto")

URL_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"

# Nombre de pages de 250 cryptos à récupérer (triées par capitalisation).
# 20 pages = les 5000 plus grosses cryptos. La récupération initiale prend
# environ 3 minutes (pages espacées pour respecter la limite de débit), ce
# qui est sans conséquence puisqu'elle n'a lieu qu'une fois toutes les 12h.
NB_PAGES = 20
DELAI_ENTRE_PAGES_SEC = 8.0  # marge confortable sous la limite de débit

_cache: dict[str, str] = {}  # {TICKER majuscule: url du logo}


def _session() -> aiohttp.ClientSession:
    """Même stratégie DNS que les autres modules REST (voir symbol_discovery.py)."""
    if platform.system() == "Windows":
        try:
            from aiohttp.resolver import AsyncResolver
            resolver = AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
            return aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=resolver))
        except Exception:
            pass
    return aiohttp.ClientSession()


def _parser_page(data) -> dict[str, str]:
    """
    Extrait {TICKER: url_logo} d'une page /coins/markets.
    Format attendu : [{"symbol": "btc", "name": "Bitcoin", "image": "https://..."}]
    Les variantes 'large' de l'URL sont converties en 'small' (~2 Ko au lieu
    de ~15 Ko) : on affiche ces icônes en 26px, la haute résolution est inutile
    et alourdirait le chargement d'une liste de plusieurs centaines de lignes.
    """
    resultat = {}
    if not isinstance(data, list):
        return resultat

    for piece in data:
        if not isinstance(piece, dict):
            continue
        symbole = piece.get("symbol")
        image = piece.get("image")
        if not symbole or not image or not isinstance(image, str):
            continue
        if not image.startswith("https://"):
            continue  # ignore toute URL inattendue plutôt que de l'injecter dans le HTML
        resultat[symbole.upper()] = image.replace("/large/", "/small/")
    return resultat


async def _recuperer_page(session, page: int) -> dict[str, str]:
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": "250",
        "page": str(page),
        "sparkline": "false",
    }
    async with session.get(URL_MARKETS, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
        if resp.status != 200:
            log.warning(f"logos_crypto : page {page} — statut HTTP {resp.status}")
            return {}
        return _parser_page(await resp.json(content_type=None))


async def _rafraichir_une_fois():
    """
    Ne vide JAMAIS le cache avant de le remplir : si CoinGecko est
    momentanément indisponible, on garde les logos déjà connus plutôt que de
    faire disparaître toutes les icônes d'un coup.

    Les pages sont parcourues dans l'ordre (capitalisation décroissante) et
    on ne remplace pas un ticker déjà vu — la première occurrence gagne.
    """
    nouveaux = 0
    async with _session() as session:
        for page in range(1, NB_PAGES + 1):
            try:
                logos = await _recuperer_page(session, page)
            except Exception as e:
                log.warning(f"logos_crypto : échec page {page} ({e})")
                continue

            if not logos:
                break  # plus de résultats (ou blocage) — inutile d'insister

            for ticker, url in logos.items():
                if ticker not in _cache:
                    _cache[ticker] = url
                    nouveaux += 1

            if page < NB_PAGES:
                await asyncio.sleep(DELAI_ENTRE_PAGES_SEC)

    log.info(f"logos_crypto : {len(_cache)} logos en cache ({nouveaux} ajoutés ce cycle)")


async def boucle_rafraichissement(intervalle_sec: float = 12 * 3600):
    """À lancer en tâche de fond au démarrage (voir bot_fusionne_v1.py)."""
    while True:
        try:
            await _rafraichir_une_fois()
        except Exception as e:
            log.error(f"logos_crypto : erreur boucle de rafraîchissement ({e})")
        await asyncio.sleep(intervalle_sec)


def obtenir(ticker: str):
    """Retourne l'URL du logo pour un ticker (ex: 'BTC'), ou None si inconnu."""
    return _cache.get((ticker or "").upper())


def tous() -> dict[str, str]:
    return dict(_cache)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    async def _test():
        print("Récupération des logos depuis CoinGecko (peut prendre ~1 minute)...")
        await _rafraichir_une_fois()
        print(f"\n{len(_cache)} logos récupérés. Échantillon :")
        for ticker in ["BTC", "ETH", "CARV", "ZBCN", "SXT", "SWELL", "ZEUS", "ARX"]:
            print(f"  {ticker:8} -> {_cache.get(ticker, '(absent)')}")

    asyncio.run(_test())

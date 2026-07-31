"""
Prix actuel + variation 24h par crypto (un exchange de référence par symbole)
====================================================================================
Récupère directement le prix et le %-24h déjà calculés par chaque exchange
via leurs endpoints ticker publics — PAS de tracking maison sur 24h (ce qui
demanderait d'attendre 24h après déploiement avant d'avoir la moindre
donnée, et de stocker un historique complet juste pour ça).

Le premier exchange qui répond pour un symbole donné "gagne" — c'est purement
indicatif pour l'affichage (comme afficher le prix sur un exchange au choix),
pas une moyenne pondérée ni une donnée utilisée dans les calculs d'arbitrage
du bot (ceux-là utilisent prix_live via WebSocket, bien plus précis/frais).

Rafraîchi toutes les 3 minutes par défaut — la variation 24h n'a pas besoin
d'une fraîcheur seconde par seconde comme le spread d'arbitrage.

⚠️ Comme pour symbol_discovery.py, je n'ai pas pu tester ces appels REST en
conditions réelles (accès aux exchanges bloqué dans mon sandbox) — j'ai
vérifié les noms de champs exacts via la documentation officielle de chaque
exchange avant d'écrire ce fichier, et testé la logique de parsing avec des
réponses simulées reprenant leur format exact. Teste ce fichier seul avant
de faire confiance à l'affichage :
    python3 prix_24h.py
"""

import asyncio
import logging
import platform
import time

import aiohttp

log = logging.getLogger("prix_24h")

_cache: dict = {}  # {symbole: {prix, variation_24h_pct, exchange_source, timestamp}}


def _session_avec_dns_force() -> aiohttp.ClientSession:
    """Même stratégie DNS que les autres modules REST du bot (voir symbol_discovery.py)."""
    if platform.system() == "Windows":
        try:
            from aiohttp.resolver import AsyncResolver
            resolver = AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
            connector = aiohttp.TCPConnector(resolver=resolver)
            return aiohttp.ClientSession(connector=connector)
        except Exception:
            pass
    return aiohttp.ClientSession()


def _parser_binance(data) -> dict:
    """Champs vérifiés : lastPrice, priceChangePercent (déjà en %, pas une fraction)."""
    if not isinstance(data, list):
        return {}
    resultat = {}
    for t in data:
        if not isinstance(t, dict):
            continue
        symbole = t.get("symbol", "")
        if not symbole.endswith("USDT"):
            continue
        try:
            resultat[symbole] = {"prix": float(t["lastPrice"]), "variation_24h_pct": float(t["priceChangePercent"])}
        except (TypeError, ValueError, KeyError):
            pass
    return resultat


def _parser_bybit(data) -> dict:
    """Champs vérifiés : lastPrice, price24hPcnt (fraction décimale, ex: "-0.0308" -> *100)."""
    if not isinstance(data, dict):
        return {}
    resultat = {}
    for t in data.get("result", {}).get("list", []):
        if not isinstance(t, dict):
            continue
        symbole = t.get("symbol", "")
        if not symbole.endswith("USDT"):
            continue
        try:
            resultat[symbole] = {"prix": float(t["lastPrice"]), "variation_24h_pct": float(t["price24hPcnt"]) * 100}
        except (TypeError, ValueError, KeyError):
            pass
    return resultat


def _parser_okx(data) -> dict:
    """
    Champs vérifiés : last, open24h — PAS de champ %-24h direct dans l'API OKX
    officielle (contrairement à Binance/Bybit), donc calculé à la main :
    (last - open24h) / open24h * 100.
    """
    if not isinstance(data, dict):
        return {}
    resultat = {}
    for t in data.get("data", []):
        if not isinstance(t, dict):
            continue
        inst_id = t.get("instId", "")
        if not inst_id.endswith("-USDT"):
            continue
        try:
            last = float(t["last"])
            open24h = float(t["open24h"])
            if open24h == 0:
                continue
            variation = (last - open24h) / open24h * 100
            resultat[inst_id.replace("-", "")] = {"prix": last, "variation_24h_pct": variation}
        except (TypeError, ValueError, KeyError):
            pass
    return resultat


def _parser_kucoin(data) -> dict:
    """Champs vérifiés : last, changeRate (fraction décimale, ex: "-0.0014" -> *100)."""
    if not isinstance(data, dict):
        return {}
    resultat = {}
    for t in data.get("data", {}).get("ticker", []):
        if not isinstance(t, dict):
            continue
        symbole = t.get("symbol", "")
        if not symbole.endswith("-USDT"):
            continue
        try:
            resultat[symbole.replace("-", "")] = {
                "prix": float(t["last"]), "variation_24h_pct": float(t["changeRate"]) * 100,
            }
        except (TypeError, ValueError, KeyError):
            pass
    return resultat


def _parser_bitget(data) -> dict:
    """Champs vérifiés : lastPr, change24h (fraction décimale, ex: "0.00069" -> *100)."""
    if not isinstance(data, dict):
        return {}
    resultat = {}
    for t in data.get("data", []):
        if not isinstance(t, dict):
            continue
        symbole = t.get("symbol", "")
        if not symbole.endswith("USDT"):
            continue
        try:
            resultat[symbole] = {"prix": float(t["lastPr"]), "variation_24h_pct": float(t["change24h"]) * 100}
        except (TypeError, ValueError, KeyError):
            pass
    return resultat


def _parser_gateio(data) -> dict:
    """Champs vérifiés : last, change_percentage (déjà en %, pas une fraction)."""
    if not isinstance(data, list):
        return {}
    resultat = {}
    for t in data:
        if not isinstance(t, dict):
            continue
        paire = t.get("currency_pair", "")
        if not paire.endswith("_USDT"):
            continue
        try:
            resultat[paire.replace("_", "")] = {
                "prix": float(t["last"]), "variation_24h_pct": float(t["change_percentage"]),
            }
        except (TypeError, ValueError, KeyError):
            pass
    return resultat


async def _recuperer_binance() -> dict:
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.binance.com/api/v3/ticker/24hr") as resp:
            data = await resp.json(content_type=None)
    return _parser_binance(data)


async def _recuperer_bybit() -> dict:
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.bybit.com/v5/market/tickers", params={"category": "spot"}) as resp:
            data = await resp.json(content_type=None)
    return _parser_bybit(data)


async def _recuperer_okx() -> dict:
    async with _session_avec_dns_force() as session:
        async with session.get("https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"}) as resp:
            data = await resp.json()
    return _parser_okx(data)


async def _recuperer_kucoin() -> dict:
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.kucoin.com/api/v1/market/allTickers") as resp:
            data = await resp.json()
    return _parser_kucoin(data)


async def _recuperer_bitget() -> dict:
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.bitget.com/api/v2/spot/market/tickers") as resp:
            data = await resp.json()
    return _parser_bitget(data)


async def _recuperer_gateio() -> dict:
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.gateio.ws/api/v4/spot/tickers") as resp:
            data = await resp.json()
    return _parser_gateio(data)


_RECUPERATEURS = {
    "binance": _recuperer_binance,
    "bybit": _recuperer_bybit,
    "okx": _recuperer_okx,
    "kucoin": _recuperer_kucoin,
    "bitget": _recuperer_bitget,
    "gateio": _recuperer_gateio,
}


async def _rafraichir_une_fois():
    """
    Ne VIDE PAS le cache avant de le remplir — si un exchange échoue
    temporairement, on garde les dernières valeurs connues plutôt que
    d'afficher "—" partout pour un simple accroc réseau passager.
    """
    resultats = await asyncio.gather(*(f() for f in _RECUPERATEURS.values()), return_exceptions=True)
    maintenant = time.time()
    nb_reçus = 0
    for nom, r in zip(_RECUPERATEURS.keys(), resultats):
        if isinstance(r, Exception):
            log.warning(f"⚠️ prix_24h : échec récupération {nom} ({r})")
            continue
        for symbole, info in r.items():
            if symbole not in _cache:  # premier exchange à répondre pour ce symbole "gagne"
                _cache[symbole] = {**info, "exchange_source": nom, "timestamp": maintenant}
            nb_reçus += 1
    log.info(f"prix_24h : cache mis à jour ({len(_cache)} cryptos en cache, {nb_reçus} entrées reçues ce cycle)")


async def boucle_rafraichissement(intervalle_sec: float = 180.0):
    """À lancer en tâche de fond au démarrage du bot (voir bot_fusionne_v1.py)."""
    while True:
        try:
            await _rafraichir_une_fois()
        except Exception as e:
            log.error(f"Erreur boucle rafraîchissement prix_24h : {e}")
        await asyncio.sleep(intervalle_sec)


def obtenir(symbole: str):
    return _cache.get(symbole)


def tous() -> dict:
    return dict(_cache)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    async def _test():
        print("Test de récupération des prix + variation 24h sur les 6 exchanges...")
        await _rafraichir_une_fois()
        exemples = list(_cache.items())[:10]
        for symbole, info in exemples:
            print(f"  {symbole} ({info['exchange_source']}) : {info['prix']}$ ({info['variation_24h_pct']:+.2f}% 24h)")
        print(f"\nTotal : {len(_cache)} cryptos en cache")

    asyncio.run(_test())

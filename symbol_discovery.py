"""
Découverte automatique des paires disponibles
==================================================
Interroge chaque exchange pour obtenir la liste réelle de ses paires
spot disponibles, puis calcule l'INTERSECTION : les cryptos qui existent
sur les 5 exchanges à la fois (uniquement en USDT, pour rester cohérent
avec le reste du bot).

⚠️ Je n'ai pas pu tester ces appels REST en conditions réelles (mon
environnement n'a pas accès aux exchanges), donc teste bien ce fichier
seul avant de l'intégrer au bot principal :
    python3 symbol_discovery.py

Installation :
    pip install aiohttp --break-system-packages
"""

import asyncio
import logging
import aiohttp
from config import TICKERS_A_RISQUE

log = logging.getLogger("symbol_discovery")


def _session_avec_dns_force() -> aiohttp.ClientSession:
    """
    Sur Windows, pycares/aiodns échoue parfois à lire la config DNS système
    (bug connu), d'où le forçage explicite de Google DNS. Sur Linux/Mac
    (ex: déploiement Railway), ce forçage peut lui-même échouer selon la
    version de pycares installée ('Channel' object has no attribute
    'gethostbyname') — on retombe alors sur le résolveur par défaut, qui
    fonctionne normalement très bien sur ces systèmes.
    """
    import platform
    if platform.system() == "Windows":
        try:
            from aiohttp.resolver import AsyncResolver
            resolver = AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
            connector = aiohttp.TCPConnector(resolver=resolver)
            return aiohttp.ClientSession(connector=connector)
        except Exception:
            pass
    return aiohttp.ClientSession()


async def paires_binance() -> dict:
    """Retourne {symbole: volume_24h_usdt} pour les paires USDT actives."""
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.binance.com/api/v3/exchangeInfo") as resp:
            info = await resp.json()
        async with session.get("https://api.binance.com/api/v3/ticker/24hr") as resp:
            tickers = await resp.json()

    valides = {
        s["symbol"] for s in info.get("symbols", [])
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
    }
    volumes = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in tickers if t.get("symbol") in valides}
    return volumes


async def paires_bybit() -> dict:
    async with _session_avec_dns_force() as session:
        async with session.get(
            "https://api.bybit.com/v5/market/instruments-info", params={"category": "spot"}
        ) as resp:
            info = await resp.json()
        async with session.get(
            "https://api.bybit.com/v5/market/tickers", params={"category": "spot"}
        ) as resp:
            tickers = await resp.json()

    valides = {
        s["symbol"] for s in info.get("result", {}).get("list", [])
        if s.get("quoteCoin") == "USDT" and s.get("status") == "Trading"
    }
    volumes = {
        t["symbol"]: float(t.get("turnover24h", 0))
        for t in tickers.get("result", {}).get("list", [])
        if t.get("symbol") in valides
    }
    return volumes


async def paires_okx() -> dict:
    """OKX utilise le format BTC-USDT -> normalisé en BTCUSDT pour cohérence."""
    async with _session_avec_dns_force() as session:
        async with session.get(
            "https://www.okx.com/api/v5/public/instruments", params={"instType": "SPOT"}
        ) as resp:
            info = await resp.json()
        async with session.get(
            "https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"}
        ) as resp:
            tickers = await resp.json()

    valides = {
        s["instId"] for s in info.get("data", [])
        if s.get("instId", "").endswith("-USDT") and s.get("state") == "live"
    }
    volumes = {
        t["instId"].replace("-", ""): float(t.get("volCcy24h", 0))
        for t in tickers.get("data", [])
        if t.get("instId") in valides
    }
    return volumes


async def paires_kucoin() -> dict:
    """KuCoin utilise aussi le format BTC-USDT -> normalisé."""
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.kucoin.com/api/v2/symbols") as resp:
            info = await resp.json()
        async with session.get("https://api.kucoin.com/api/v1/market/allTickers") as resp:
            tickers = await resp.json()

    valides = {
        s["symbol"] for s in info.get("data", [])
        if s.get("quoteCurrency") == "USDT" and s.get("enableTrading")
    }
    volumes = {
        t["symbol"].replace("-", ""): float(t.get("volValue", 0) or 0)
        for t in tickers.get("data", {}).get("ticker", [])
        if t.get("symbol") in valides
    }
    return volumes


async def paires_bitget() -> dict:
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.bitget.com/api/v2/spot/public/symbols") as resp:
            info = await resp.json()
        async with session.get("https://api.bitget.com/api/v2/spot/market/tickers") as resp:
            tickers = await resp.json()

    valides = {
        s["symbol"] for s in info.get("data", [])
        if s.get("quoteCoin") == "USDT" and s.get("status") == "online"
    }
    volumes = {
        t["symbol"]: float(t.get("usdtVolume", 0) or 0)
        for t in tickers.get("data", [])
        if t.get("symbol") in valides
    }
    return volumes


async def paires_gateio() -> dict:
    """
    Gate.io reste en JSON classique côté WebSocket (pas de protobuf comme
    MEXC) — plus simple. Format de symbole natif : BTC_USDT (underscore).
    """
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.gateio.ws/api/v4/spot/currency_pairs") as resp:
            info = await resp.json()
        async with session.get("https://api.gateio.ws/api/v4/spot/tickers") as resp:
            tickers = await resp.json()

    # currency_pairs retourne directement une LISTE (pas de clé "symbols")
    valides = {
        s["id"] for s in info
        if s.get("quote") == "USDT" and s.get("trade_status") == "tradable"
    }
    volumes = {
        t["currency_pair"].replace("_", ""): float(t.get("quote_volume", 0) or 0)
        for t in tickers if t.get("currency_pair") in valides
    }
    return volumes


async def calculer_intersection(exclure: set[str] | None = None) -> list[str]:
    """
    Récupère les paires de chaque exchange en parallèle, calcule l'intersection
    STRICTE (disponible sur les 5 à la fois), retourne une liste triée.
    Conservé pour compatibilité — voir calculer_disponibilite_min() pour un
    critère plus souple (disponible sur au moins N exchanges) et le filtre de volume.
    """
    exclure = exclure or set()

    resultats = await asyncio.gather(
        paires_binance(), paires_bybit(), paires_okx(), paires_kucoin(), paires_bitget(), paires_gateio(),
        return_exceptions=True,
    )

    noms = ["binance", "bybit", "okx", "kucoin", "bitget", "gateio"]
    ensembles = []
    for nom, r in zip(noms, resultats):
        if isinstance(r, Exception):
            log.error(f"Échec récupération paires {nom} : {r}")
        else:
            log.info(f"{nom} : {len(r)} paires USDT disponibles")
            ensembles.append(set(r.keys()))

    if len(ensembles) < 2:
        raise RuntimeError("Pas assez d'exchanges accessibles pour calculer une intersection utile.")

    intersection = set.intersection(*ensembles) - exclure
    return sorted(intersection)


def _base_asset(symbol_usdt: str) -> str:
    """Extrait la partie 'base' d'un symbole normalisé en XXXUSDT (ex: BTCUSDT -> BTC)."""
    return symbol_usdt[:-4] if symbol_usdt.endswith("USDT") else symbol_usdt


# Tickers courts/génériques statistiquement à très haut risque de désigner
# une crypto DIFFÉRENTE selon l'exchange (collision de nom) — voir config.py


async def calculer_disponibilite_min(
    min_exchanges: int = 3, exclure: set[str] | None = None, volume_min_usdt: float = 500_000,
) -> dict[str, set[str]]:
    """
    Version souple : retourne TOUTES les paires disponibles sur AU MOINS
    min_exchanges exchanges (pas forcément les 5 à la fois), ET dont le
    volume 24h dépasse volume_min_usdt sur AU MOINS UN des exchanges où
    elle est disponible (un token peut être peu actif sur un exchange
    mais très liquide sur un autre — on ne le rejette pas juste pour ça,
    seulement si TOUS les exchanges où il est listé ont un volume trop faible).

    Ce filtre élimine à la source les tokens listés partout mais tradés
    nulle part (ex: DGB, XTER, VOOI, ALLO...) qui génèrent des faux signaux
    à cause d'un carnet d'ordres trop fin, avant même de les surveiller.

    Retourne un dict {symbole: {noms des exchanges où il est disponible ET liquide}}.
    """
    exclure = exclure or set()

    resultats = await asyncio.gather(
        paires_binance(), paires_bybit(), paires_okx(), paires_kucoin(), paires_bitget(), paires_gateio(),
        return_exceptions=True,
    )

    noms = ["binance", "bybit", "okx", "kucoin", "bitget", "gateio"]
    par_exchange: dict[str, dict] = {}
    for nom, r in zip(noms, resultats):
        if isinstance(r, Exception):
            log.error(f"Échec récupération paires {nom} : {r}")
        else:
            log.info(f"{nom} : {len(r)} paires USDT disponibles")
            par_exchange[nom] = r

    if len(par_exchange) < 2:
        raise RuntimeError("Pas assez d'exchanges accessibles.")

    # Compte, pour chaque symbole, sur combien d'exchanges il est disponible
    # ET liquide (volume 24h >= volume_min_usdt sur CET exchange précis)
    compteur: dict[str, set[str]] = {}
    nb_rejetes_volume = 0
    for nom, paires_avec_volume in par_exchange.items():
        for symbole, volume in paires_avec_volume.items():
            if symbole in exclure:
                continue
            if _base_asset(symbole) in TICKERS_A_RISQUE:
                continue
            if volume < volume_min_usdt:
                nb_rejetes_volume += 1
                continue
            compteur.setdefault(symbole, set()).add(nom)

    resultat = {s: exs for s, exs in compteur.items() if len(exs) >= min_exchanges}
    log.info(
        f"{len(resultat)} paires disponibles sur au moins {min_exchanges} exchanges "
        f"avec volume >= {volume_min_usdt:,.0f}$ (24h)"
    )
    return resultat


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    async def _test():
        dispo = await calculer_disponibilite_min(min_exchanges=3, volume_min_usdt=500_000)
        print(f"\n✅ {len(dispo)} paires disponibles (min 3 exchanges, volume >= 500k$)  :")
        exemples = list(dispo.items())[:20]
        for symbole, exchanges in exemples:
            print(f"  {symbole} : {', '.join(sorted(exchanges))}")
        if len(dispo) > 20:
            print(f"... et {len(dispo) - 20} de plus")

    asyncio.run(_test())

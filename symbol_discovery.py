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
            texte_brut = await resp.text()
            try:
                info = await resp.json(content_type=None)
            except Exception:
                log.error(f"Binance exchangeInfo — réponse non-JSON (probable blocage régional) : {texte_brut[:200]}")
                return {}
        async with session.get("https://api.binance.com/api/v3/ticker/24hr") as resp:
            texte_brut = await resp.text()
            try:
                tickers = await resp.json(content_type=None)
            except Exception:
                log.error(f"Binance ticker24hr — réponse non-JSON (probable blocage régional) : {texte_brut[:200]}")
                return {}

    if not isinstance(info, dict):
        log.error(f"Binance exchangeInfo — format inattendu (probable blocage régional) : {str(info)[:200]}")
        return {}
    if not isinstance(tickers, list):
        log.error(f"Binance ticker24hr — format inattendu (probable blocage régional) : {str(tickers)[:200]}")
        return {}

    valides = {
        s["symbol"] for s in info.get("symbols", [])
        if isinstance(s, dict) and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
    }
    volumes = {
        t["symbol"]: float(t.get("quoteVolume", 0))
        for t in tickers if isinstance(t, dict) and t.get("symbol") in valides
    }
    return volumes


async def paires_bybit() -> dict:
    async with _session_avec_dns_force() as session:
        async with session.get(
            "https://api.bybit.com/v5/market/instruments-info", params={"category": "spot"}
        ) as resp:
            texte_brut = await resp.text()
            try:
                info = await resp.json(content_type=None)
            except Exception:
                log.error(f"Bybit instruments-info — réponse non-JSON (probable blocage/limite régional) : {texte_brut[:200]}")
                return {}
        async with session.get(
            "https://api.bybit.com/v5/market/tickers", params={"category": "spot"}
        ) as resp:
            texte_brut = await resp.text()
            try:
                tickers = await resp.json(content_type=None)
            except Exception:
                log.error(f"Bybit tickers — réponse non-JSON (probable blocage/limite régional) : {texte_brut[:200]}")
                return {}

    if not isinstance(info, dict) or not isinstance(tickers, dict):
        log.error("Bybit — format de réponse inattendu (probable blocage régional)")
        return {}

    valides = {
        s["symbol"] for s in info.get("result", {}).get("list", [])
        if isinstance(s, dict) and s.get("quoteCoin") == "USDT" and s.get("status") == "Trading"
    }
    volumes = {
        t["symbol"]: float(t.get("turnover24h", 0))
        for t in tickers.get("result", {}).get("list", [])
        if isinstance(t, dict) and t.get("symbol") in valides
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


async def paires_whitebit() -> dict:
    """
    WhiteBit — ajouté le 07/08. Cote en USDT (BTC_USDT), aucune conversion
    nécessaire — plus simple que Bitvavo.
    """
    async with _session_avec_dns_force() as session:
        async with session.get("https://whitebit.com/api/v4/public/markets") as resp:
            marches = await resp.json()
        async with session.get("https://whitebit.com/api/v4/public/ticker") as resp:
            tickers = await resp.json()

    valides = {
        str(m["name"]) for m in marches
        if isinstance(m, dict) and str(m.get("money", "")).upper() == "USDT"
        and m.get("tradesEnabled")
    }
    volumes = {}
    for marche, infos in (tickers.items() if isinstance(tickers, dict) else []):
        if marche not in valides or not isinstance(infos, dict):
            continue
        # ⚠️ WhiteBit nomme ses marchés BTC_USDT (avec underscore), mais le
        # reste du bot (et le connecteur WebSocket) utilise BTCUSDT — sans
        # cette conversion, l'intersection avec les autres plateformes ne
        # trouverait jamais aucune correspondance.
        symbole = marche.replace("_", "")
        brut = infos.get("quoteVolume") or infos.get("quote_volume") or infos.get("deal") or 0
        try:
            volumes[symbole] = float(brut)
        except (TypeError, ValueError):
            volumes[symbole] = 0.0
    return volumes


async def paires_bitvavo() -> dict:
    """
    Bitvavo — ajouté le 07/08 en même temps que le connecteur WebSocket.

    ⚠️ Bitvavo ne cote qu'en EUR (BTC-EUR), jamais en USDT. Ici on ne fait
    que RENOMMER le symbole en BTCUSDT (format commun du bot) pour que
    l'intersection avec les autres plateformes fonctionne — le VRAI prix
    utilisé pour l'arbitrage est converti séparément, en temps réel, dans
    BitvavoWS.run() via taux_change.py. Cette fonction ne sert qu'à la
    DÉCOUVERTE des symboles disponibles, pas au calcul de prix.

    Le "volume" retourné est le volume en EUR, utilisé tel quel comme proxy
    du volume USDT pour le filtre VOLUME_MIN_USDT — EUR et USDT ayant des
    valeurs proches (~5-8% d'écart), c'est une approximation acceptable
    pour un simple seuil de liquidité, pas pour un calcul de prix.
    """
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.bitvavo.com/v2/markets") as resp:
            marches = await resp.json()
        async with session.get("https://api.bitvavo.com/v2/ticker/24h") as resp:
            tickers = await resp.json()

    valides = {
        str(m["market"]) for m in marches
        if isinstance(m, dict) and str(m.get("market", "")).endswith("-EUR")
        and m.get("status") == "trading"
    }
    volumes = {}
    for t in tickers:
        if not isinstance(t, dict):
            continue
        marche = t.get("market")
        if marche not in valides:
            continue
        base = marche.replace("-EUR", "")
        try:
            volumes[f"{base}USDT"] = float(t.get("volumeQuote", 0) or 0)
        except (TypeError, ValueError):
            volumes[f"{base}USDT"] = 0.0
    return volumes


async def paires_coinex() -> dict:
    """
    CoinEx v2 — ajouté le 04/08.
    Les marchés y sont nommés sans séparateur (BTCUSDT), comme Binance.
    """
    async with _session_avec_dns_force() as session:
        async with session.get("https://api.coinex.com/v2/spot/market") as resp:
            info = await resp.json(content_type=None)
        async with session.get("https://api.coinex.com/v2/spot/ticker") as resp:
            tickers = await resp.json(content_type=None)

    valides = {
        m["market"] for m in (info.get("data") or [])
        if isinstance(m, dict) and str(m.get("quote_ccy", "")).upper() == "USDT"
    }
    volumes = {}
    for t in (tickers.get("data") or []):
        if not isinstance(t, dict):
            continue
        marche = t.get("market")
        if marche not in valides:
            continue
        # `value` est le volume 24h exprimé en devise de cotation (USDT)
        try:
            volumes[marche] = float(t.get("value") or 0)
        except (TypeError, ValueError):
            volumes[marche] = 0.0
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
        paires_coinex(),
        paires_bitvavo(),
        paires_whitebit(),
        return_exceptions=True,
    )

    noms = ["binance", "bybit", "okx", "kucoin", "bitget", "gateio", "coinex", "bitvavo", "whitebit"]
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
        paires_coinex(),
        paires_bitvavo(),
        paires_whitebit(),
        return_exceptions=True,
    )

    noms = ["binance", "bybit", "okx", "kucoin", "bitget", "gateio", "coinex", "bitvavo", "whitebit"]
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

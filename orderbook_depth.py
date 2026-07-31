"""
Profondeur du carnet d'ordres
=================================
Le prix affiché par le WebSocket (bid/ask) n'est valable que pour une
quantité minuscule. Ce module récupère le VRAI carnet d'ordres (plusieurs
niveaux de prix) via REST, pour calculer le prix moyen réellement obtenu
si on trade un montant donné — essentiel pour le mode papier, sinon les
résultats simulés seraient artificiellement optimistes.

⚠️ Comme pour les autres modules REST, je n'ai pas pu tester ces appels
en conditions réelles (accès aux exchanges bloqué dans mon sandbox).
Teste ce fichier seul avant de faire confiance au mode papier :
    python3 orderbook_depth.py
"""

import asyncio
import logging
import aiohttp

log = logging.getLogger("orderbook_depth")


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


def _vers_tiret(symbol: str) -> str:
    """BTCUSDT -> BTC-USDT (format attendu par OKX/KuCoin/Bitget selon l'endpoint)."""
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"
    return symbol


async def carnet_binance(symbol: str, limite: int = 20):
    async with _session_avec_dns_force() as session:
        async with session.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": symbol, "limit": limite},
        ) as resp:
            data = await resp.json()
    bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
    return bids, asks


async def carnet_bybit(symbol: str, limite: int = 20):
    async with _session_avec_dns_force() as session:
        async with session.get(
            "https://api.bybit.com/v5/market/orderbook",
            params={"category": "spot", "symbol": symbol, "limit": limite},
        ) as resp:
            data = await resp.json()
    resultat = data.get("result", {})
    bids = [(float(p), float(q)) for p, q in resultat.get("b", [])]
    asks = [(float(p), float(q)) for p, q in resultat.get("a", [])]
    return bids, asks


async def carnet_okx(symbol: str, limite: int = 20):
    async with _session_avec_dns_force() as session:
        async with session.get(
            "https://www.okx.com/api/v5/market/books",
            params={"instId": _vers_tiret(symbol), "sz": str(limite)},
        ) as resp:
            data = await resp.json()
    livre = data.get("data", [{}])[0]
    bids = [(float(p), float(q)) for p, q, *_ in livre.get("bids", [])]
    asks = [(float(p), float(q)) for p, q, *_ in livre.get("asks", [])]
    return bids, asks


async def carnet_kucoin(symbol: str, limite: int = 20):
    async with _session_avec_dns_force() as session:
        async with session.get(
            f"https://api.kucoin.com/api/v1/market/orderbook/level2_{limite}",
            params={"symbol": _vers_tiret(symbol)},
        ) as resp:
            data = await resp.json()
    resultat = data.get("data", {})
    bids = [(float(p), float(q)) for p, q in resultat.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in resultat.get("asks", [])]
    return bids, asks


async def carnet_bitget(symbol: str, limite: int = 20):
    async with _session_avec_dns_force() as session:
        async with session.get(
            "https://api.bitget.com/api/v2/spot/market/orderbook",
            params={"symbol": symbol, "type": "step0", "limit": str(limite)},
        ) as resp:
            data = await resp.json()
    resultat = data.get("data", {})
    bids = [(float(p), float(q)) for p, q in resultat.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in resultat.get("asks", [])]
    return bids, asks


CARNET_PAR_EXCHANGE = {
    "binance": carnet_binance,
    "bybit": carnet_bybit,
    "okx": carnet_okx,
    "kucoin": carnet_kucoin,
    "bitget": carnet_bitget,
}


async def obtenir_carnet(exchange: str, symbol: str, limite: int = 20):
    """Retourne (bids, asks) pour un exchange/symbole donné, ou (None, None) si échec."""
    fonction = CARNET_PAR_EXCHANGE.get(exchange)
    if not fonction:
        return None, None
    try:
        return await fonction(symbol, limite)
    except Exception as e:
        log.warning(f"Échec récupération carnet {exchange}/{symbol} : {e}")
        return None, None


def prix_execution_moyen(niveaux: list, montant_usdt: float) -> tuple:
    """
    Calcule le prix MOYEN PONDÉRÉ réellement obtenu en 'mangeant' les niveaux
    du carnet jusqu'à épuiser montant_usdt.

    niveaux : liste de (prix, quantité), triée du meilleur prix au pire
    Retourne (prix_moyen, montant_reellement_rempli, liquidite_suffisante: bool)
    """
    if not niveaux:
        return (0, 0, False)

    reste_usdt = montant_usdt
    quantite_totale = 0.0
    cout_total = 0.0

    for prix, quantite_dispo in niveaux:
        valeur_niveau = prix * quantite_dispo
        if valeur_niveau >= reste_usdt:
            quantite_prise = reste_usdt / prix
            cout_total += quantite_prise * prix
            quantite_totale += quantite_prise
            reste_usdt = 0
            break
        else:
            cout_total += valeur_niveau
            quantite_totale += quantite_dispo
            reste_usdt -= valeur_niveau

    liquidite_suffisante = reste_usdt <= 0.01  # tolérance d'arrondi
    prix_moyen = cout_total / quantite_totale if quantite_totale > 0 else 0
    montant_rempli = montant_usdt - reste_usdt

    return (prix_moyen, montant_rempli, liquidite_suffisante)


async def estimer_execution_reelle(exchange_achat: str, exchange_vente: str, symbol: str, montant_usdt: float):
    """
    Récupère les vrais carnets des deux exchanges et calcule le prix moyen
    pondéré réellement obtenu — MÊME en cas de remplissage partiel (carnet
    trop fin pour montant_usdt en entier). Le prix retourné n'est JAMAIS
    fictif : prix_execution_moyen() ne calcule que sur les niveaux
    réellement consommés, jamais extrapolé au-delà de la profondeur dispo.

    "liquidite_suffisante" = True seulement si le montant COMPLET demandé
    a pu être rempli des deux côtés. "montant_executable" donne le montant
    réellement tradable (le plus petit des deux côtés) même quand c'est
    inférieur à montant_usdt — c'est à l'appelant de décider s'il accepte
    un trade de taille réduite plutôt que de rejeter entièrement.
    """
    vide = {
        "liquidite_suffisante": False, "montant_executable": 0.0, "montant_demande": montant_usdt,
        "prix_achat_reel": 0.0, "prix_vente_reel": 0.0, "spread_reel_pct": 0.0,
    }

    (bids_achat, asks_achat), (bids_vente, asks_vente) = await asyncio.gather(
        obtenir_carnet(exchange_achat, symbol),
        obtenir_carnet(exchange_vente, symbol),
    )

    if not asks_achat or not bids_vente:
        return vide

    prix_achat_reel, montant_rempli_achat, liquide_achat = prix_execution_moyen(asks_achat, montant_usdt)
    prix_vente_reel, montant_rempli_vente, liquide_vente = prix_execution_moyen(bids_vente, montant_usdt)

    if prix_achat_reel <= 0 or prix_vente_reel <= 0:
        return vide

    # Le montant exécutable des DEUX côtés à la fois = le plus petit des deux
    # (inutile d'acheter pour 40$ si on ne peut en revendre que 15$ en face)
    montant_executable = min(montant_rempli_achat, montant_rempli_vente)
    spread_reel_pct = ((prix_vente_reel - prix_achat_reel) / prix_achat_reel) * 100

    return {
        "liquidite_suffisante": bool(liquide_achat and liquide_vente),
        "montant_executable": round(montant_executable, 4),
        "montant_demande": montant_usdt,
        "prix_achat_reel": prix_achat_reel,
        "prix_vente_reel": prix_vente_reel,
        "spread_reel_pct": spread_reel_pct,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    async def _test():
        print("Test avec BTCUSDT, 100$ entre Binance (achat) et Bybit (vente)...")
        resultat = await estimer_execution_reelle("binance", "bybit", "BTCUSDT", 100)
        print(resultat)

    asyncio.run(_test())

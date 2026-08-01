"""
Frais de retrait réels par plateforme
==========================================
Le calcul de profit du mode papier ne déduisait que les frais de
TRANSACTION. Or boucler un arbitrage suppose de rééquilibrer les fonds
entre plateformes, et ce transfert coûte de l'argent — un coût FIXE en
dollars, donc d'autant plus lourd que le trade est petit.

Sur un trade de 50$, un retrait à 1$ représente 2% du montant : plus que
l'écart d'arbitrage lui-même dans la majorité des cas. C'est la principale
raison pour laquelle un profit simulé peut être largement surestimé.

Ce module récupère les frais de retrait de l'USDT (l'actif qu'on déplace
pour rééquilibrer) et retient le RÉSEAU LE MOINS CHER disponible sur
chaque plateforme.

⚠️ COUVERTURE PARTIELLE — à connaître :
Seules certaines plateformes exposent ces frais sans authentification.
Pour les autres (Binance, Bybit, OKX), les frais réels exigent une clé API
avec signature, que le bot n'a pas nécessairement. On utilise alors une
estimation par défaut, explicitement marquée comme telle : `est_estime`
vaut True. Ne confonds pas une estimation avec une mesure.

⚠️ Non testable dans mon environnement (pas d'accès réseau aux
plateformes). La logique de lecture a été testée avec des réponses
simulées reprenant leur format documenté. Teste ce fichier seul :
    python3 frais_retrait.py
"""

import asyncio
import logging
import platform

import aiohttp

log = logging.getLogger("frais_retrait")

# Estimations utilisées quand la plateforme n'expose pas ses frais
# publiquement. Volontairement PRUDENTES (plutôt trop chères que trop
# bon marché) : mieux vaut sous-estimer un profit que l'inverse.
FRAIS_PAR_DEFAUT_USDT = {
    "binance": 1.0,
    "bybit": 1.0,
    "okx": 1.0,
    "kucoin": 1.0,
    "bitget": 1.0,
    "gateio": 1.0,
}

# {exchange: {"frais": float, "reseau": str, "est_estime": bool}}
_cache: dict[str, dict] = {}


def _session() -> aiohttp.ClientSession:
    if platform.system() == "Windows":
        try:
            from aiohttp.resolver import AsyncResolver
            resolver = AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
            return aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=resolver))
        except Exception:
            pass
    return aiohttp.ClientSession()


def _moins_cher(candidats: list[tuple[float, str]]):
    """Retourne (frais, réseau) du candidat le moins cher, ou None si aucun."""
    valides = [(f, r) for f, r in candidats if f is not None and f >= 0]
    return min(valides, key=lambda x: x[0]) if valides else None


def _parser_kucoin(data) -> tuple | None:
    """
    /api/v3/currencies — public. Chaque devise porte une liste `chains`
    dont chaque entrée contient `withdrawalMinFee` et `chainName`.
    """
    if not isinstance(data, dict):
        return None
    for devise in data.get("data", []):
        if not isinstance(devise, dict) or devise.get("currency") != "USDT":
            continue
        candidats = []
        for chaine in devise.get("chains", []) or []:
            if not isinstance(chaine, dict):
                continue
            if chaine.get("isWithdrawEnabled") is False:
                continue  # réseau fermé : ne pas le proposer comme option
            try:
                candidats.append((float(chaine.get("withdrawalMinFee")), chaine.get("chainName", "?")))
            except (TypeError, ValueError):
                continue
        return _moins_cher(candidats)
    return None


def _parser_bitget(data) -> tuple | None:
    """/api/v2/spot/public/coins — public. `chains[].withdrawFee`."""
    if not isinstance(data, dict):
        return None
    for devise in data.get("data", []):
        if not isinstance(devise, dict) or devise.get("coin") != "USDT":
            continue
        candidats = []
        for chaine in devise.get("chains", []) or []:
            if not isinstance(chaine, dict):
                continue
            if str(chaine.get("withdrawable")).lower() == "false":
                continue
            try:
                candidats.append((float(chaine.get("withdrawFee")), chaine.get("chain", "?")))
            except (TypeError, ValueError):
                continue
        return _moins_cher(candidats)
    return None


def _parser_gateio(data) -> tuple | None:
    """
    /api/v4/wallet/withdraw_status — public. `withdraw_fix_on_chains`
    associe chaque réseau à son frais fixe.
    """
    if not isinstance(data, list):
        return None
    for devise in data:
        if not isinstance(devise, dict) or devise.get("currency") != "USDT":
            continue
        par_chaine = devise.get("withdraw_fix_on_chains") or {}
        candidats = []
        for reseau, frais in par_chaine.items():
            try:
                candidats.append((float(frais), reseau))
            except (TypeError, ValueError):
                continue
        if candidats:
            return _moins_cher(candidats)
        # Repli sur le frais global si le détail par réseau est absent
        try:
            return (float(devise.get("withdraw_fix")), "défaut")
        except (TypeError, ValueError):
            return None
    return None


_SOURCES_PUBLIQUES = {
    "kucoin": ("https://api.kucoin.com/api/v3/currencies", _parser_kucoin),
    "bitget": ("https://api.bitget.com/api/v2/spot/public/coins", _parser_bitget),
    "gateio": ("https://api.gateio.ws/api/v4/wallet/withdraw_status", _parser_gateio),
}


async def _rafraichir_une_fois():
    """
    Interroge les plateformes qui exposent leurs frais publiquement.
    Les autres gardent leur estimation par défaut, marquée `est_estime`.
    Ne vide jamais le cache : une panne réseau ne doit pas faire disparaître
    des frais déjà connus.
    """
    async with _session() as session:
        for exchange, (url, parser) in _SOURCES_PUBLIQUES.items():
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status != 200:
                        log.warning(f"frais_retrait : {exchange} — statut HTTP {resp.status}")
                        continue
                    resultat = parser(await resp.json(content_type=None))
            except Exception as e:
                log.warning(f"frais_retrait : échec {exchange} ({e})")
                continue

            if resultat:
                frais, reseau = resultat
                _cache[exchange] = {"frais": frais, "reseau": reseau, "est_estime": False}
                log.info(f"frais_retrait : {exchange} — {frais}$ via {reseau} (réel)")
            else:
                log.warning(f"frais_retrait : {exchange} — USDT introuvable dans la réponse")

    # Complète avec les estimations pour les plateformes non couvertes
    for exchange, defaut in FRAIS_PAR_DEFAUT_USDT.items():
        if exchange not in _cache:
            _cache[exchange] = {"frais": defaut, "reseau": "estimation", "est_estime": True}

    reels = sum(1 for v in _cache.values() if not v["est_estime"])
    log.info(f"frais_retrait : {len(_cache)} plateformes ({reels} avec frais réels, {len(_cache) - reels} estimés)")


async def boucle_rafraichissement(intervalle_sec: float = 6 * 3600):
    """À lancer en tâche de fond au démarrage (voir bot_fusionne_v1.py)."""
    while True:
        try:
            await _rafraichir_une_fois()
        except Exception as e:
            log.error(f"frais_retrait : erreur de rafraîchissement ({e})")
        await asyncio.sleep(intervalle_sec)


def frais_retrait_usdt(exchange: str) -> float:
    """Frais de retrait USDT (réseau le moins cher) pour une plateforme."""
    info = _cache.get(exchange)
    if info:
        return info["frais"]
    return FRAIS_PAR_DEFAUT_USDT.get(exchange, 1.0)


def detail(exchange: str) -> dict:
    """Détail complet, y compris si la valeur est estimée ou mesurée."""
    return _cache.get(exchange) or {
        "frais": FRAIS_PAR_DEFAUT_USDT.get(exchange, 1.0),
        "reseau": "estimation", "est_estime": True,
    }


def tous() -> dict:
    return dict(_cache)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    async def _test():
        print("Récupération des frais de retrait USDT...\n")
        await _rafraichir_une_fois()
        print(f"{'Plateforme':<10} {'Frais':>8}  {'Réseau':<12} Source")
        for ex in sorted(_cache):
            d = _cache[ex]
            source = "estimé" if d["est_estime"] else "réel"
            print(f"{ex:<10} {d['frais']:>7.3f}$  {d['reseau']:<12} {source}")

    asyncio.run(_test())

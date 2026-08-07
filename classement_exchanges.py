"""
Classement des plateformes pour l'arbitrage
=============================================
Interroge les données publiques d'une dizaine de plateformes et les classe
sur ce qui compte VRAIMENT pour l'arbitrage : la capacité des tokens à
CIRCULER, dans les deux sens.

LE CRITÈRE PRINCIPAL N'EST PAS LE RETRAIT
La mesure du 04/08 sur tes 3 plateformes actuelles a montré l'inverse de
l'intuition : bitget laisse sortir 87% de ses tokens mais n'en accepte que
50% en entrée. Or un arbitrage exige un retrait ouvert à la SOURCE ET un
dépôt ouvert à la DESTINATION. Le goulot, ce sont les dépôts. D'où le
score composite :

    score = (% retrait ouvert) x (% dépôt ouvert) / 100

C'est grossièrement la probabilité qu'un token pris au hasard puisse
circuler avec une plateforme équivalente. Une plateforme à 90% de retrait
mais 20% de dépôt vaut moins qu'une à 60/60.

⚠️ CE QUE CE SCRIPT NE PEUT PAS TE DIRE
  - si la plateforme accepte les résidents de ton pays (question de KYC,
    à vérifier plateforme par plateforme avant d'ouvrir un compte)
  - la liquidité réelle des carnets
  - le montant des frais de retrait
  - la fiabilité de la plateforme
Une plateforme peut afficher 95% d'ouverture et rester inutilisable faute
de volume. Ce classement mesure UNE dimension, la plus bloquante.

⚠️ PARSERS NON VÉRIFIÉS EN CONDITIONS RÉELLES
kucoin, bitget et gateio sont confirmés (mesure du 04/08). Les autres sont
écrits d'après la documentation publique mais n'ont pas pu être testés
contre les vraies API — un format peut avoir changé. Une plateforme qui
échoue apparaît en bas avec la raison, sans faire planter le reste.
Signale-moi les échecs, ils se corrigent vite.

Usage :
    python3 classement_exchanges.py
    python3 classement_exchanges.py --detail
    python3 classement_exchanges.py --top 12
"""

import asyncio
import logging
import platform
import sys
from collections import Counter

import aiohttp

log = logging.getLogger("classement_exchanges")

# Charge le .env local — sans ça, une exécution directe sur ton PC (comme
# `python3 classement_exchanges.py`) ne verrait jamais tes clés API, même
# si elles sont bien dans le fichier .env à côté du script. Les modules
# telegram_menu_bot.py / telegram_notifier.py font déjà ce chargement pour
# leur propre usage — ce script en a besoin lui aussi, indépendamment.
try:
    from dotenv import load_dotenv
    import stockage
    load_dotenv(stockage.chemin_donnees(".env"))
except Exception:
    pass  # pas grave en environnement Railway : les variables sont déjà dans os.environ

TIMEOUT = aiohttp.ClientTimeout(total=30)


def _session():
    """Contournement DNS Windows, comme le reste du projet."""
    if platform.system() == "Windows":
        try:
            from aiohttp.resolver import AsyncResolver
            return aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    resolver=AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
                )
            )
        except Exception:
            pass
    return aiohttp.ClientSession()


VRAIS = ("true", "1", "yes", "allowed", "enabled", "normal", "open")


def _vrai(valeur, vrais=VRAIS) -> bool:
    return str(valeur).strip().lower() in vrais


def _liste(donnees, *cles):
    """
    Extrait une liste imbriquée sans jamais lever d'exception.
    Les API renvoient parfois `null` au lieu d'une liste vide, ou du texte
    en cas d'erreur — un parser ne doit pas planter là-dessus.
    """
    courant = donnees
    for cle in cles:
        if not isinstance(courant, dict):
            return []
        courant = courant.get(cle)
    return courant if isinstance(courant, list) else []


def _reseau(nom) -> str:
    if not nom:
        return "?"
    n = str(nom).strip().upper()
    for c in ("-", "_", " ", "(", ")", ".", "/"):
        n = n.replace(c, "")
    alias = {
        "TRC20": "TRX", "TRON": "TRX",
        "ERC20": "ETH", "ETHEREUM": "ETH", "ETHERC20": "ETH",
        "BEP20": "BSC", "BINANCESMARTCHAIN": "BSC", "BNBSMARTCHAIN": "BSC", "BNBCHAIN": "BSC",
        "SOLANA": "SOL", "SPL": "SOL",
        "POLYGON": "MATIC", "POL": "MATIC",
        "ARBITRUMONE": "ARBITRUM", "ARB": "ARBITRUM",
        "OPTIMISM": "OP",
        "AVALANCHECCHAIN": "AVAXC", "AVAXCCHAIN": "AVAXC",
        # Relevés sur XT le 04/08 : suffixes techniques accolés au nom
        "POLYGONPOS": "MATIC", "MATICPOS": "MATIC",
        "KASKRC20": "KAS", "KRC20": "KAS",
        # Relevés sur BitMart le 04/08
        "BSCBNB": "BSC", "BASEETH": "BASE", "ARBI": "ARBITRUM",
        "ARBITRUMETH": "ARBITRUM", "OPETH": "OP", "MATICPOLYGON": "MATIC",
        # Relevés sur bitmart le 04/08
        "BSCBNB": "BSC", "BASEETH": "BASE", "ARBI": "ARBITRUM",
        "ARBITRUMETH": "ARBITRUM", "OPTIMISMETH": "OP", "MATICPOLYGON": "MATIC",
        "OPTIMISMOP": "OP", "ETHW": "ETHW",
    }
    if n in alias:
        return alias[n]
    for suffixe in ("EVM", "CHAIN", "NETWORK", "MAINNET"):
        if n.endswith(suffixe) and len(n) > len(suffixe) + 1:
            n = n[: -len(suffixe)]
            break
    return alias.get(n, n)


# ============================================================
# PARSERS — chacun renvoie {TOKEN: {"reseaux": {nom: (retrait, depot)}}}
# ============================================================
def p_kucoin(d):
    out = {}
    for c in _liste(d, "data"):
        if not isinstance(c, dict):
            continue
        r = {}
        for ch in _liste(c, "chains"):
            if isinstance(ch, dict):
                r[_reseau(ch.get("chainName") or ch.get("chainId"))] = (
                    _vrai(ch.get("isWithdrawEnabled")), _vrai(ch.get("isDepositEnabled")))
        if r:
            out[str(c.get("currency", "")).upper()] = {"reseaux": r}
    return out


def p_bitget(d):
    out = {}
    for c in _liste(d, "data"):
        if not isinstance(c, dict):
            continue
        r = {}
        for ch in _liste(c, "chains"):
            if isinstance(ch, dict):
                r[_reseau(ch.get("chain"))] = (
                    _vrai(ch.get("withdrawable")), _vrai(ch.get("rechargeable")))
        if r:
            out[str(c.get("coin", "")).upper()] = {"reseaux": r}
    return out


def p_gateio(d):
    out = {}
    if not isinstance(d, list):
        return out
    for c in d:
        if not isinstance(c, dict):
            continue
        base = str(c.get("currency", "")).upper().split("_")[0]
        if not base:
            continue
        entree = out.setdefault(base, {"reseaux": {}})
        entree["reseaux"][_reseau(c.get("chain") or base)] = (
            not _vrai(c.get("withdraw_disabled")), not _vrai(c.get("deposit_disabled")))
    return out


def p_htx(d):
    """api.huobi.pro/v2/reference/currencies"""
    out = {}
    for c in _liste(d, "data"):
        if not isinstance(c, dict):
            continue
        r = {}
        for ch in _liste(c, "chains"):
            if isinstance(ch, dict):
                r[_reseau(ch.get("displayName") or ch.get("chain"))] = (
                    _vrai(ch.get("withdrawStatus")), _vrai(ch.get("depositStatus")))
        if r:
            out[str(c.get("currency", "")).upper()] = {"reseaux": r}
    return out


def p_bitmart(d):
    """
    api-cloud.bitmart.com/account/v1/currencies

    ⚠️ Corrigé le 04/08 : l'endpoint /spot/v1/currencies utilisé auparavant
    n'expose PAS le réseau. Le parser retombait alors sur le nom du token
    comme nom de réseau — d'où le symptôme « BTC (1), ETH (1), USDT (1) »
    dans le détail, chaque réseau n'apparaissant qu'une fois. Les
    pourcentages restaient valables, mais les réseaux étaient inexploitables
    pour la recherche de trajet commun. /account/v1/currencies fournit bien
    le champ `network`.
    """
    out = {}
    for c in _liste(d, "data", "currencies"):
        if not isinstance(c, dict):
            continue
        nom = str(c.get("currency") or c.get("id", "")).upper().split("-")[0]
        if not nom:
            continue
        reseau_brut = c.get("network")
        # Sans champ réseau, on marque "?" plutôt que d'inventer le nom du
        # token : un faux nom de réseau produit de faux « aucun réseau commun »
        entree = out.setdefault(nom, {"reseaux": {}})
        entree["reseaux"][_reseau(reseau_brut) if reseau_brut else "?"] = (
            _vrai(c.get("withdraw_enabled")), _vrai(c.get("deposit_enabled")))
    return out


def p_coinex(d):
    """api.coinex.com/v2/assets/all-deposit-withdraw-config"""
    out = {}
    for c in _liste(d, "data"):
        if not isinstance(c, dict):
            continue
        actif = c.get("asset") if isinstance(c.get("asset"), dict) else {}
        nom = str(actif.get("ccy") or c.get("ccy", "")).upper()
        if not nom:
            continue
        r = {}
        for ch in _liste(c, "chains"):
            if isinstance(ch, dict):
                r[_reseau(ch.get("chain"))] = (
                    _vrai(ch.get("withdraw_enabled")), _vrai(ch.get("deposit_enabled")))
        if not r:
            r = {_reseau(nom): (_vrai(actif.get("withdraw_enabled")),
                                _vrai(actif.get("deposit_enabled")))}
        out[nom] = {"reseaux": r}
    return out


def p_poloniex(d):
    """api.poloniex.com/currencies"""
    out = {}
    if not isinstance(d, list):
        return out
    for bloc in d:
        if not isinstance(bloc, dict):
            continue
        entrees = ([bloc] if "currency" in bloc
                   else [{"currency": k, **v} for k, v in bloc.items() if isinstance(v, dict)])
        for c in entrees:
            nom = str(c.get("currency", "")).upper()
            if not nom:
                continue
            out[nom] = {"reseaux": {_reseau(c.get("blockchain") or nom): (
                _vrai(c.get("walletWithdrawalState") or c.get("withdrawalEnable")),
                _vrai(c.get("walletDepositState") or c.get("depositEnable")))}}
    return out


def p_ascendex(d):
    """ascendex.com/api/pro/v2/assets"""
    out = {}
    for c in _liste(d, "data"):
        if not isinstance(c, dict):
            continue
        nom = str(c.get("assetCode", "")).upper()
        if not nom:
            continue
        r = {}
        for ch in _liste(c, "blockChain"):
            if isinstance(ch, dict):
                r[_reseau(ch.get("chainName"))] = (
                    _vrai(ch.get("withdrawStatus")), _vrai(ch.get("depositStatus")))
        if r:
            out[nom] = {"reseaux": r}
    return out


def p_digifinex(d):
    """openapi.digifinex.com/v3/currencies"""
    out = {}
    for c in _liste(d, "data"):
        if not isinstance(c, dict):
            continue
        nom = str(c.get("currency", "")).upper()
        if not nom:
            continue
        entree = out.setdefault(nom, {"reseaux": {}})
        entree["reseaux"][_reseau(c.get("chain") or nom)] = (
            _vrai(c.get("withdraw_status")), _vrai(c.get("deposit_status")))
    return out


def p_xt(d):
    """sapi.xt.com/v4/public/wallet/support/currency"""
    out = {}
    for c in _liste(d, "result"):
        if not isinstance(c, dict):
            continue
        nom = str(c.get("currency", "")).upper()
        if not nom:
            continue
        r = {}
        for ch in _liste(c, "supportChains"):
            if isinstance(ch, dict):
                r[_reseau(ch.get("chain"))] = (
                    _vrai(ch.get("withdrawEnabled")), _vrai(ch.get("depositEnabled")))
        if r:
            out[nom] = {"reseaux": r}
    return out


def p_whitebit(d):
    """whitebit.com/api/v4/public/assets — dict {TOKEN: {...}}"""
    out = {}
    if not isinstance(d, dict):
        return out
    for nom, c in d.items():
        if not isinstance(c, dict):
            continue
        r = {}
        reseaux = c.get("networks") if isinstance(c.get("networks"), dict) else {}
        deposits = reseaux.get("deposits") or []
        withdraws = reseaux.get("withdraws") or []
        for reseau in set(list(deposits) + list(withdraws)):
            r[_reseau(reseau)] = (reseau in withdraws, reseau in deposits)
        if not r:
            r = {"?": (_vrai(c.get("can_withdraw")), _vrai(c.get("can_deposit")))}
        out[str(nom).upper()] = {"reseaux": r}
    return out


def p_probit(d):
    """api.probit.com/api/exchange/v1/currency"""
    out = {}
    for c in _liste(d, "data"):
        if not isinstance(c, dict):
            continue
        nom = str(c.get("id", "")).upper()
        if not nom:
            continue
        r = {}
        for pf in _liste(c, "platform"):
            if not isinstance(pf, dict):
                continue
            r[_reseau(pf.get("id") or nom)] = (
                not _vrai(pf.get("withdrawal_suspended")),
                not _vrai(pf.get("deposit_suspended")))
        if not r:
            r = {"?": (not _vrai(c.get("withdrawal_suspended")),
                       not _vrai(c.get("deposit_suspended")))}
        out[nom] = {"reseaux": r}
    return out


def p_kraken(d):
    """
    api.kraken.com/0/public/Assets
    `status` vaut enabled / deposit_only / withdrawal_only / disabled.
    """
    out = {}
    resultats = (d or {}).get("result") if isinstance(d, dict) else None
    if not isinstance(resultats, dict):
        return out
    for cle, c in resultats.items():
        if not isinstance(c, dict):
            continue
        nom = str(c.get("altname") or cle).upper()
        statut = str(c.get("status", "enabled")).lower()
        out[nom] = {"reseaux": {"?": (
            statut in ("enabled", "withdrawal_only"),
            statut in ("enabled", "deposit_only"))}}
    return out


def p_bitvavo(d):
    """api.bitvavo.com/v2/assets — depositStatus / withdrawalStatus"""
    out = {}
    if not isinstance(d, list):
        return out
    for c in d:
        if not isinstance(c, dict):
            continue
        nom = str(c.get("symbol", "")).upper()
        if not nom:
            continue
        out[nom] = {"reseaux": {_reseau(c.get("network") or "?"): (
            _vrai(c.get("withdrawalStatus"), ("ok", "true", "enabled")),
            _vrai(c.get("depositStatus"), ("ok", "true", "enabled")))}}
    return out


def p_exmo(d):
    """api.exmo.com/v1.1/payments/providers/crypto/list"""
    out = {}
    if not isinstance(d, dict):
        return out
    for nom, fournisseurs in d.items():
        if not isinstance(fournisseurs, list):
            continue
        retrait = depot = False
        for f in fournisseurs:
            if not isinstance(f, dict):
                continue
            actif = _vrai(f.get("enabled"))
            if not actif:
                continue
            # Le champ `type` varie ("deposit"/"withdraw" mais aussi
            # "deposits"/"withdrawals" selon les versions) : on cherche la
            # sous-chaîne plutôt qu'une égalité stricte. Sans type du tout,
            # on ne suppose rien — c'est ce qui donnait 0% de dépôt partout.
            genre = str(f.get("type", "")).lower()
            if "withdraw" in genre:
                retrait = True
            if "deposit" in genre:
                depot = True
        out[str(nom).upper()] = {"reseaux": {"?": (retrait, depot)}}
    return out


def p_mexc(d):
    """
    api.mexc.com/api/v3/capital/config/getall

    ⚠️ Cet endpoint est documenté comme public mais MEXC peut exiger une
    clé API selon la version — testé sans clé ici. S'il échoue, il basculera
    automatiquement dans "non mesurée" plutôt que de fausser le classement.
    """
    out = {}
    if not isinstance(d, list):
        return out
    for c in d:
        if not isinstance(c, dict):
            continue
        nom = str(c.get("coin", "")).upper()
        if not nom:
            continue
        r = {}
        for ch in _liste(c, "networkList"):
            if isinstance(ch, dict):
                r[_reseau(ch.get("network") or ch.get("netWork"))] = (
                    _vrai(ch.get("withdrawEnable")), _vrai(ch.get("depositEnable")))
        if r:
            out[nom] = {"reseaux": r}
    return out


def p_bingx(d):
    """open-api.bingx.com/openApi/wallets/v1/capital/config/getall"""
    out = {}
    for c in _liste(d, "data"):
        if not isinstance(c, dict):
            continue
        nom = str(c.get("coin", "")).upper()
        if not nom:
            continue
        r = {}
        for ch in _liste(c, "networkList"):
            if isinstance(ch, dict):
                r[_reseau(ch.get("network"))] = (
                    _vrai(ch.get("withdrawEnable")), _vrai(ch.get("depositEnable")))
        if r:
            out[nom] = {"reseaux": r}
    return out


def p_bybit(d):
    """
    api.bybit.com/v5/asset/coin/query-info

    ⚠️ Documenté comme nécessitant une authentification pour la plupart des
    comptes, mais certains renvoient une réponse publique limitée. Laissé
    dans la liste "sans données publiques" par prudence (voir plus bas) —
    ce parser existe si jamais tu obtiens un accès public un jour.
    """
    out = {}
    for c in _liste(d, "result", "rows"):
        if not isinstance(c, dict):
            continue
        nom = str(c.get("coin", "")).upper()
        if not nom:
            continue
        r = {}
        for ch in _liste(c, "chains"):
            if isinstance(ch, dict):
                r[_reseau(ch.get("chain"))] = (
                    _vrai(ch.get("chainWithdraw"), ("1", "true")),
                    _vrai(ch.get("chainDeposit"), ("1", "true")))
        if r:
            out[nom] = {"reseaux": r}
    return out


def p_okx(d):
    """
    www.okx.com/api/v5/asset/currencies

    ⚠️ Documenté comme nécessitant une clé API dans la plupart des cas.
    Gardé en réserve, pas dans SOURCES par défaut (voir SANS_DONNEES_PUBLIQUES).
    """
    out = {}
    for c in _liste(d, "data"):
        if not isinstance(c, dict):
            continue
        nom = str(c.get("ccy", "")).upper()
        if not nom:
            continue
        entree = out.setdefault(nom, {"reseaux": {}})
        entree["reseaux"][_reseau(c.get("chain") or nom)] = (
            _vrai(c.get("canWd"), ("true", "1")), _vrai(c.get("canDep"), ("true", "1")))
    return out


SOURCES = {
    "kucoin":    ("https://api.kucoin.com/api/v3/currencies", p_kucoin),
    "bitget":    ("https://api.bitget.com/api/v2/spot/public/coins", p_bitget),
    "gateio":    ("https://api.gateio.ws/api/v4/spot/currencies", p_gateio),
    "htx":       ("https://api.huobi.pro/v2/reference/currencies", p_htx),
    "bitmart":   ("https://api-cloud.bitmart.com/account/v1/currencies", p_bitmart),
    "coinex":    ("https://api.coinex.com/v2/assets/all-deposit-withdraw-config", p_coinex),
    "poloniex":  ("https://api.poloniex.com/v2/currencies", p_poloniex),
    "ascendex":  ("https://ascendex.com/api/pro/v1/assets", p_ascendex),
    "digifinex": ("https://openapi.digifinex.com/v3/currencies", p_digifinex),
    "xt":        ("https://sapi.xt.com/v4/public/wallet/support/currency", p_xt),
    "whitebit":  ("https://whitebit.com/api/v4/public/assets", p_whitebit),
    "probit":    ("https://api.probit.com/api/exchange/v1/currency", p_probit),
    "kraken":    ("https://api.kraken.com/0/public/Assets", p_kraken),
    "bitvavo":   ("https://api.bitvavo.com/v2/assets", p_bitvavo),
}
# exmo, mexc, bingx retirés de SOURCES le 04/08 après mesure réelle :
#   - mexc  : HTTP 400 -> exige en réalité une requête signée malgré la doc
#             qui présente cet endpoint comme public
#   - bingx : réponse vide -> même chose, signature requise en pratique
#   - exmo  : l'endpoint /crypto/list ne liste QUE les fournisseurs de
#             RETRAIT, jamais de dépôt -> donnait 0% de dépôt pour tous les
#             tokens, ce qui ressemble à un blocage alors que c'est un trou
#             dans les données. Pas d'endpoint public équivalent trouvé pour
#             les dépôts -> mieux vaut l'exclure que produire un demi-résultat.
# Les 3 parsers restent dans ce fichier (inutilisés) si un accès API un
# jour les rend exploitables.

# okx et bybit exigent quasi-systématiquement une signature -> non tentés,
# mais les parsers existent (p_okx, p_bybit) si tu obtiens un jour un accès.

# Site public de chaque plateforme, pour le test d'accessibilité
SITES = {
    "kucoin": "https://www.kucoin.com", "bitget": "https://www.bitget.com",
    "gateio": "https://www.gate.io", "htx": "https://www.htx.com",
    "bitmart": "https://www.bitmart.com", "coinex": "https://www.coinex.com",
    "poloniex": "https://poloniex.com", "ascendex": "https://ascendex.com",
    "digifinex": "https://www.digifinex.com", "xt": "https://www.xt.com",
    "whitebit": "https://whitebit.com", "probit": "https://www.probit.com",
    "kraken": "https://www.kraken.com",
    # Sites des plateformes normalement "sans données publiques" — utiles
    # au test d'accessibilité UNIQUEMENT si une clé les débloque un jour.
    "binance": "https://www.binance.com", "bybit": "https://www.bybit.com",
    "okx": "https://www.okx.com", "mexc": "https://www.mexc.com",
    "bitvavo": "https://bitvavo.com",
}

# Plateformes exigeant une clé API signée pour cette information —
# impossibles à classer ici. Ce n'est pas un jugement sur elles.
SANS_DONNEES_PUBLIQUES = ("binance", "bybit", "okx", "latoken", "mexc", "bingx", "exmo")
# latoken retiré le 04/08 : son endpoint /v2/currency liste bien 5072 tokens
# mais n'expose AUCUN champ de retrait ni de dépôt — le parser renvoyait donc
# 0% partout, ce qui ressemblait à « tout est bloqué » alors que c'est
# « information absente ». Mieux vaut l'annoncer non mesurable.


async def _tester_site(session, nom, url):
    """
    Teste si le site de la plateforme est joignable depuis TA connexion.

    Ce que ça détecte : géo-blocage franc (403/451), site injoignable,
    redirection vers une page de restriction.

    Ce que ça NE détecte PAS : si le KYC acceptera tes documents, si les
    dépôts en monnaie locale sont possibles, ou si le compte sera fermé
    après vérification. Ces réponses ne s'obtiennent qu'à l'inscription.
    """
    entetes = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20),
                               headers=entetes, allow_redirects=True) as resp:
            final = str(resp.url)
            if resp.status == 451:
                return nom, "geobloque", "HTTP 451 (restriction légale)"
            if resp.status == 403:
                # Un 403 sur la page d'accueil vient presque toujours d'une
                # protection anti-bot (Cloudflare) qui refuse un client sans
                # navigateur — PAS d'un blocage géographique. Bitvavo et
                # WhiteBit répondaient d'ailleurs normalement sur leur API.
                # On ne les déclasse donc pas : à vérifier dans un navigateur.
                return nom, "protection", "HTTP 403 (anti-bot probable, teste dans un navigateur)"
            if resp.status >= 400:
                return nom, "erreur", f"HTTP {resp.status}"
            # Certaines plateformes redirigent vers une page de restriction
            for motif in ("restricted", "unavailable", "not-available", "blocked", "geo"):
                if motif in final.lower():
                    return nom, "geobloque", f"redirigé vers {final[:60]}"
            return nom, "accessible", ""
    except asyncio.TimeoutError:
        return nom, "erreur", "délai dépassé"
    except Exception as e:
        return nom, "erreur", type(e).__name__


async def tester_accessibilite():
    async with _session() as session:
        resultats = await asyncio.gather(*(
            _tester_site(session, nom, url) for nom, url in SITES.items()
        ))
    return {nom: (statut, detail) for nom, statut, detail in resultats}


async def _charger_un(session, nom, url, parser):
    entetes = {
        # Certaines API renvoient une réponse tronquée ou mal encodée sans
        # User-Agent (ClientPayloadError observé sur poloniex le 04/08).
        "User-Agent": "Mozilla/5.0 (compatible; ArbitrageRadar/1.0)",
        "Accept": "application/json",
    }
    try:
        async with session.get(url, timeout=TIMEOUT, headers=entetes) as resp:
            if resp.status != 200:
                return nom, None, f"HTTP {resp.status}"
            try:
                data = await resp.json(content_type=None)
            except Exception:
                # Repli : lire en octets puis décoder nous-mêmes. Contourne
                # les erreurs de longueur/compression déclarée à tort.
                import json
                brut = await resp.read()
                data = json.loads(brut.decode("utf-8", errors="replace"))
    except asyncio.TimeoutError:
        return nom, None, "délai dépassé"
    except Exception as e:
        return nom, None, type(e).__name__

    try:
        resultat = parser(data)
    except Exception as e:
        return nom, None, f"format inattendu ({type(e).__name__})"

    if not resultat:
        return nom, None, "aucun token lu (format changé ?)"
    return nom, resultat, None


async def charger_tout():
    async with _session() as session:
        resultats = await asyncio.gather(*(
            _charger_un(session, nom, url, parser)
            for nom, (url, parser) in SOURCES.items()
        ))

    # Plateformes débloquées par clé API en lecture seule (binance, bybit,
    # okx, mexc — voir cles_privees.py). Ne fait RIEN si aucune clé n'est
    # configurée : le classement se comporte alors exactement comme avant.
    try:
        import cles_privees
        obtenus = await cles_privees.recuperer_toutes_les_cles_configurees()
    except Exception as e:
        obtenus = {}
        print(f"[classement_exchanges] requêtes signées indisponibles : {e}")
        log.warning(f"requêtes signées indisponibles : {e}")

    for exchange, tokens in obtenus.items():
        # cles_privees.py renvoie {"retrait_ouvert": bool, "depot_ouvert": bool}
        # (dict), alors que ce script attend des tuples (retrait, depot) —
        # même convention que verif_retraits.py côté bot, différente ici.
        # Adaptation nécessaire pour que analyser() traite les deux sources
        # de façon identique.
        adapte = {
            token: {"reseaux": {
                reseau: (infos["retrait_ouvert"], infos["depot_ouvert"])
                for reseau, infos in donnees["reseaux"].items()
            }}
            for token, donnees in tokens.items()
        }
        resultats.append((exchange, adapte, None))

    return resultats


def analyser(resultats):
    stats, echecs = {}, {}
    for nom, tokens, erreur in resultats:
        if tokens is None:
            echecs[nom] = erreur
            continue

        total = len(tokens)
        retrait = depot = bidirectionnel = bloques = 0
        reseaux = Counter()

        for infos in tokens.values():
            ouverts_r = [n for n, (w, _) in infos["reseaux"].items() if w]
            ouverts_d = [n for n, (_, dep) in infos["reseaux"].items() if dep]
            if ouverts_r:
                retrait += 1
                reseaux.update(ouverts_r)
            if ouverts_d:
                depot += 1
            if ouverts_r and ouverts_d:
                bidirectionnel += 1
            if not ouverts_r and not ouverts_d:
                bloques += 1

        pct_r = retrait / total * 100 if total else 0
        pct_d = depot / total * 100 if total else 0

        # Signalement automatique des résultats invraisemblables. Sans ça,
        # un parser défaillant produit des chiffres qui RESSEMBLENT à des
        # mesures : latoken affichait 0% partout (champs absents, lus comme
        # « tout fermé ») et exmo 100%/0% (type de fournisseur mal lu).
        alertes = []
        if reseaux and set(reseaux) == {"?"}:
            alertes.append("réseaux inconnus")
        if pct_r == 0 and pct_d == 0:
            alertes.append("0% partout — champs probablement absents")
        elif (pct_r == 0) != (pct_d == 0):
            alertes.append("un sens à 0% — parser suspect")
        elif pct_r == 100 and pct_d == 100 and total > 50:
            alertes.append("100% partout — à confirmer")
        stats[nom] = {
            "total": total, "retrait": retrait, "depot": depot,
            "bidirectionnel": bidirectionnel, "bloques": bloques,
            "pct_retrait": pct_r, "pct_depot": pct_d,
            "pct_bidir": bidirectionnel / total * 100 if total else 0,
            "score": pct_r * pct_d / 100,
            "top_reseaux": reseaux.most_common(6),
            "alertes": alertes,
        }
    return stats, echecs


def afficher(stats, echecs, acces=None, debloquees=None, top=12, detail=False):
    acces = acces or {}
    debloquees = debloquees or set()

    # Les plateformes injoignables depuis ta connexion sont reléguées en bas :
    # un excellent score de circulation ne sert à rien si le site est bloqué.
    def cle_tri(kv):
        nom, s = kv
        statut = acces.get(nom, ("inconnu", ""))[0]
        # "protection" = 403 anti-bot, pas un blocage géographique :
        # ces plateformes restent candidates.
        joignable = statut in ("accessible", "inconnu", "protection")
        return (joignable, s["score"])

    classement = sorted(stats.items(), key=cle_tri, reverse=True)

    print()
    print("=" * 86)
    print(f"CLASSEMENT DES PLATEFORMES POUR L'ARBITRAGE (top {top})")
    print("=" * 86)
    print(f"{'#':>2} {'PLATEFORME':<11} {'TOKENS':>7} {'RETRAIT':>9} {'DÉPÔT':>9} {'LES DEUX':>10} {'SCORE':>7}  {'SITE':<12}")
    print("-" * 86)

    symboles_acces = {
        "accessible": "joignable", "geobloque": "GÉOBLOQUÉ", "protection": "à vérifier",
        "bloque": "bloqué", "erreur": "injoignable", "inconnu": "?",
    }
    for rang, (nom, s) in enumerate(classement[:top], 1):
        statut, detail_acces = acces.get(nom, ("inconnu", ""))
        print(
            f"{rang:>2} {nom:<11} {s['total']:>7} "
            f"{s['pct_retrait']:>8.1f}% {s['pct_depot']:>8.1f}% "
            f"{s['pct_bidir']:>9.1f}% {s['score']:>7.1f}  "
            f"{symboles_acces.get(statut, statut):<12}"
            + (f" {detail_acces}" if detail_acces else "")
        )
        if s.get("alertes"):
            print(f"   {'':<11} ⚠️  " + " ; ".join(s["alertes"]))

    if echecs:
        print("-" * 86)
        print("Non mesurées :")
        for nom, raison in echecs.items():
            print(f"   {nom:<11} {raison}")
    else:
        print("-" * 86)
        print("Non mesurées :")
    for nom in SANS_DONNEES_PUBLIQUES:
        if nom not in debloquees:
            print(f"   {nom:<11} clé API signée requise")

    if detail:
        print()
        print("=" * 86)
        print("RÉSEAUX LES PLUS OUVERTS AU RETRAIT")
        print("=" * 86)
        for nom, s in classement[:top]:
            if s["top_reseaux"]:
                print(f"  {nom:<11} " + ", ".join(f"{r} ({n})" for r, n in s["top_reseaux"]))

        communs = None
        for nom, s in classement[:top]:
            ensemble = {r for r, _ in s["top_reseaux"]}
            communs = ensemble if communs is None else (communs & ensemble)
        if communs:
            print()
            print(f"  Réseaux communs à toutes les plateformes classées : {', '.join(sorted(communs))}")

    print()
    print("=" * 86)
    print("COMMENT LIRE CE CLASSEMENT")
    print("=" * 86)
    print("SCORE = (% retrait x % dépôt) / 100.")
    print("Un arbitrage exige un retrait ouvert à la SOURCE et un dépôt ouvert")
    print("à la DESTINATION. Les dépôts étant partout plus fermés que les")
    print("retraits, ce sont eux qui limitent — d'où ce score combiné plutôt")
    print("qu'un classement sur le seul taux de retrait.")
    print()
    print("« LES DEUX » = part des tokens ayant à la fois retrait et dépôt")
    print("ouverts : la plateforme peut alors servir dans les deux sens.")
    print()
    print("SITE = le site répond-il depuis TA connexion. « GÉOBLOQUÉ » signale")
    print("un refus explicite (HTTP 451) ou une redirection de restriction.")
    print("Les plateformes injoignables sont reléguées en bas du classement.")
    print()
    print("⚠️ Un site joignable ne garantit PAS que tu pourras ouvrir un compte :")
    print("le KYC, les dépôts en monnaie locale et l'acceptation finale ne se")
    print("découvrent qu'à l'inscription. Ce classement dit seulement quelles")
    print("plateformes valent la peine d'être essayées, et dans quel ordre.")
    print()
    print("👉 Ce classement montre TOUTES les plateformes mesurées, du meilleur")
    print("au moins bon score. C'est à toi de choisir lesquelles tu retiens —")
    print("le script ne décide de rien à ta place.")
    print()
    print("⚠️ Une ligne marquée ⚠️ signale une donnée peu crédible (parser")
    print("probablement en défaut) — ne t'appuie pas dessus sans vérifier.")
    print()
    print("⚠️ Un HTTP 403 peut venir d'une protection anti-robot (Cloudflare)")
    print("plutôt que d'un vrai blocage géographique : teste dans ton navigateur")
    print("avant de conclure qu'une plateforme t'est inaccessible.")
    print()
    print("⚠️ Il ne dit RIEN non plus de la liquidité, des frais de retrait ni")
    print("de la fiabilité — une plateforme à 95% d'ouverture peut avoir des")
    print("carnets d'ordres trop minces pour être exploitable.")


# ============================================================
# UTILISATION EN CONTINU DANS LE BOT
# ============================================================
# Le script ci-dessus était conçu pour être lancé à la main sur ton PC.
# Ce qui suit permet au BOT LUI-MÊME de refaire ce test périodiquement,
# sans que tu aies à le relancer manuellement — demandé le 04/08.
#
# ⚠️ Ce module NE branche PAS automatiquement une nouvelle plateforme sur
# les WebSockets du bot : il mesure et classe, mais ajouter une plateforme
# au flux de détection reste une étape volontaire (comme pour coinex),
# parce que ça touche aux connecteurs temps réel, pas juste à une mesure.

_dernier_resultat: dict | None = None
_dernier_calcul: float = 0.0


def dernier_resultat() -> dict | None:
    """None tant qu'aucun calcul n'a encore eu lieu."""
    return _dernier_resultat


async def rafraichir(top: int = 30):
    """Relance la mesure complète (données + accessibilité) et la stocke."""
    global _dernier_resultat, _dernier_calcul
    import time

    resultats, acces = await asyncio.gather(charger_tout(), tester_accessibilite())
    stats, echecs = analyser(resultats)

    classement = sorted(
        stats.items(),
        key=lambda kv: (acces.get(kv[0], ("inconnu", ""))[0] in ("accessible", "inconnu"), kv[1]["score"]),
        reverse=True,
    )

    debloquees = {nom for nom, _, err in resultats if err is None and nom in SANS_DONNEES_PUBLIQUES}

    _dernier_resultat = {
        "timestamp": time.time(),
        "classement": [
            {
                "rang": rang, "exchange": nom, "score": round(s["score"], 1),
                "tokens": s["total"], "pct_retrait": round(s["pct_retrait"], 1),
                "pct_depot": round(s["pct_depot"], 1), "pct_bidir": round(s["pct_bidir"], 1),
                "acces": acces.get(nom, ("inconnu", ""))[0],
                "acces_detail": acces.get(nom, ("inconnu", ""))[1],
                "alertes": s.get("alertes", []),
            }
            for rang, (nom, s) in enumerate(classement[:top], 1)
        ],
        "non_mesurees": {
            **echecs,
            **{ex: "clé API signée requise" for ex in SANS_DONNEES_PUBLIQUES if ex not in debloquees},
        },
    }
    _dernier_calcul = _dernier_resultat["timestamp"]

    ok = [n for n, t, _ in resultats if t is not None]
    print(f"[classement_exchanges] {len(ok)}/{len(SOURCES)} plateformes mesurées, top 1 : "
          f"{classement[0][0] if classement else 'aucune'}")
    log.info(
        f"rafraîchi : {len(ok)}/{len(SOURCES)} plateformes mesurées, "
        f"{len(debloquees)} débloquée(s) par clé, top 1 : "
        f"{classement[0][0] if classement else 'aucune'}"
    )


async def boucle_rafraichissement(intervalle_sec: float = 6 * 3600):
    """
    À lancer au démarrage du bot :
        asyncio.create_task(classement_exchanges.boucle_rafraichissement())

    6h par défaut, même fréquence que frais_retrait et verif_retraits — ces
    listes de tokens et leurs statuts de retrait ne changent pas assez vite
    pour justifier un sondage plus fréquent.
    """
    log.info("boucle de rafraîchissement démarrée (premier calcul en cours)")
    while True:
        try:
            await rafraichir()
        except Exception as e:
            print(f"[classement_exchanges] erreur de rafraîchissement : {e}")
            # log.exception (plutôt que log.warning) capture la trace complète
            # de l'erreur — indispensable ici : avant ce correctif, print()
            # restait bloqué dans le tampon de sortie de Railway et
            # n'apparaissait JAMAIS dans les Deploy Logs, rendant le module
            # invisible même quand une exception l'empêchait de terminer.
            log.exception("erreur de rafraîchissement")
        await asyncio.sleep(intervalle_sec)


def resume_telegram(top: int = 21) -> str:
    """
    Résumé court pour Telegram. Retourne un message d'attente si aucun
    calcul n'a encore eu lieu (juste après le démarrage du bot).
    """
    if _dernier_resultat is None:
        return "🌐 Classement des plateformes : premier calcul en cours, réessaie dans quelques minutes."

    lignes = ["🌐 <b>Classement des plateformes (retrait × dépôt)</b>\n"]
    for ligne in _dernier_resultat["classement"][:top]:
        acces = ligne["acces"]
        marque = "✅" if acces == "accessible" else ("⚠️" if acces == "erreur" else "🚫")
        note = " (alerte qualité)" if ligne["alertes"] else ""
        lignes.append(
            f"{ligne['rang']}. {marque} <b>{ligne['exchange']}</b> — score {ligne['score']:.1f} "
            f"({ligne['tokens']} tokens){note}"
        )

    non_mesurees = _dernier_resultat.get("non_mesurees", {})
    if non_mesurees:
        lignes.append(f"\nNon mesurées : {', '.join(sorted(non_mesurees))}")

    import time
    age_min = (time.time() - _dernier_resultat["timestamp"]) / 60
    lignes.append(f"\n<i>Mesuré il y a {age_min:.0f} min</i>")
    return "\n".join(lignes)


async def main():
    top = 30  # large marge : affiche tout, tu choisis toi-même
    if "--top" in sys.argv:
        try:
            top = int(sys.argv[sys.argv.index("--top") + 1])
        except (IndexError, ValueError):
            pass

    print(f"Interrogation de {len(SOURCES)} plateformes (données publiques, sans clé API)…")
    print("Tentative des plateformes à clé API (si configurées dans .env)…")
    print("Test d'accessibilité des sites depuis ta connexion…")
    resultats, acces = await asyncio.gather(charger_tout(), tester_accessibilite())

    debloquees = {nom for nom, _, err in resultats if err is None and nom in SANS_DONNEES_PUBLIQUES}
    ok = [n for n, t, _ in resultats if t is not None]
    print(f"Réponses exploitables : {len(ok)}/{len(SOURCES) + len(debloquees)} — {', '.join(ok) or 'aucune'}")
    if debloquees:
        print(f"Débloquées par clé API : {', '.join(sorted(debloquees))}")
    bloques = [n for n, (st, _) in acces.items() if st in ("geobloque", "bloque")]
    if bloques:
        print(f"Sites bloqués depuis ta connexion : {', '.join(bloques)}")

    stats, echecs = analyser(resultats)
    if not stats:
        print("Aucune plateforme n'a pu être analysée.")
        return
    afficher(stats, echecs, acces=acces, debloquees=debloquees, top=top, detail="--detail" in sys.argv)


if __name__ == "__main__":
    asyncio.run(main())

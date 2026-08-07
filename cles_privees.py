"""
Requêtes API signées — état des retraits pour binance / bybit / okx / mexc
=============================================================================
Ces 4 plateformes n'exposent PAS l'état de leurs retraits sans authentifier
la requête (contrairement à kucoin, bitget, gateio, coinex). Ce module signe
les appels pour débloquer cette information, en LECTURE SEULE uniquement.

⚠️ SÉCURITÉ — LIS CECI AVANT D'UTILISER TES CLÉS
- Ce module n'appelle QUE des endpoints d'INFORMATION (config des devises).
  Aucun ordre, aucun retrait, aucun virement n'est jamais déclenché ici.
- Les clés ne doivent JAMAIS être créées avec la permission "Withdraw"
  (retrait). Même compromises, elles ne doivent permettre de sortir aucun
  fonds — voir api_keys_manager.py, qui applique déjà ce principe.
- Aucune valeur de clé n'est écrite dans ce fichier, dans les logs, ni
  renvoyée par aucune fonction : uniquement lues depuis l'environnement
  (os.getenv) au moment de l'appel, comme TELEGRAM_TOKEN ou API_SECRET.
- Une plateforme sans clé configurée est simplement ignorée ici — le
  comportement existant (mode strict = écarté, mode souple = autorisé)
  ne change pas tant que tu n'ajoutes rien dans Railway.

FORMAT DE NOM DE VARIABLE ACCEPTÉ
api_keys_manager.py (menu Telegram) écrit {EXCHANGE}_API_KEY /
{EXCHANGE}_API_SECRET / {EXCHANGE}_PASSPHRASE. Si tu as rempli ton .env à la
main avec {EXCHANGE}_API / {EXCHANGE}_SECRET (sans le "_KEY"), ce module lit
aussi cette variante — indique-le en commentaire si tu changes de format,
pour que le reste du projet reste cohérent.

Retour de chaque fonction : même format que les parsers de verif_retraits.py
    {TOKEN: {"reseaux": {nom_reseau: {"retrait_ouvert": bool, "depot_ouvert": bool}}}}
ou None si la clé n'est pas configurée / la requête a échoué.
"""

import hashlib
import hmac
import base64
import json
import logging
import os
import time

import aiohttp

log = logging.getLogger("cles_privees")

TIMEOUT = aiohttp.ClientTimeout(total=20)


def _lire_cle(exchange: str) -> tuple[str, str, str] | None:
    """
    Lit (api_key, api_secret, passphrase) depuis l'environnement.
    Accepte les deux formats de nom (_API_KEY et _API). Retourne None si
    la clé et le secret ne sont pas tous les deux présents.
    Ne journalise jamais la valeur — seulement si elle est trouvée ou non.
    """
    prefixe = exchange.upper()
    cle = os.getenv(f"{prefixe}_API_KEY") or os.getenv(f"{prefixe}_API")
    secret = os.getenv(f"{prefixe}_API_SECRET") or os.getenv(f"{prefixe}_SECRET")
    passphrase = os.getenv(f"{prefixe}_PASSPHRASE") or ""

    if not cle or not secret:
        return None
    return cle, secret, passphrase


def cles_disponibles() -> dict[str, bool]:
    """Pour les logs de démarrage : quelles plateformes ont une clé configurée."""
    return {ex: _lire_cle(ex) is not None for ex in ("binance", "bybit", "okx", "mexc")}


def _normaliser_reseau(nom) -> str:
    """Import tardif pour éviter une dépendance circulaire avec verif_retraits."""
    import verif_retraits
    return verif_retraits._normaliser_reseau(nom)


# ============================================================
# BINANCE — HMAC-SHA256 sur la query string
# ============================================================
async def recuperer_binance() -> dict | None:
    cle_secret = _lire_cle("binance")
    if cle_secret is None:
        return None
    api_key, api_secret, _ = cle_secret

    horodatage = int(time.time() * 1000)
    requete = f"timestamp={horodatage}"
    signature = hmac.new(api_secret.encode(), requete.encode(), hashlib.sha256).hexdigest()
    url = f"https://api.binance.com/sapi/v1/capital/config/getall?{requete}&signature={signature}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers={"X-MBX-APIKEY": api_key}, timeout=TIMEOUT
            ) as resp:
                if resp.status != 200:
                    log.warning(f"binance (signé) : statut HTTP {resp.status}")
                    return None
                data = await resp.json(content_type=None)
    except Exception as e:
        log.warning(f"binance (signé) : échec ({e})")
        return None

    out = {}
    for c in data if isinstance(data, list) else []:
        if not isinstance(c, dict):
            continue
        nom = str(c.get("coin", "")).upper()
        if not nom:
            continue
        reseaux = {}
        for ch in c.get("networkList", []) or []:
            if not isinstance(ch, dict):
                continue
            reseaux[_normaliser_reseau(ch.get("network"))] = {
                "retrait_ouvert": bool(ch.get("withdrawEnable")),
                "depot_ouvert": bool(ch.get("depositEnable")),
            }
        if reseaux:
            out[nom] = {"reseaux": reseaux}
    return out


# ============================================================
# BYBIT v5 — en-têtes X-BAPI-*
# ============================================================
async def recuperer_bybit() -> dict | None:
    cle_secret = _lire_cle("bybit")
    if cle_secret is None:
        return None
    api_key, api_secret, _ = cle_secret

    horodatage = str(int(time.time() * 1000))
    fenetre = "5000"
    # Bybit v5 : signature = HMAC(timestamp + api_key + recv_window + query_string)
    chaine_a_signer = horodatage + api_key + fenetre
    signature = hmac.new(api_secret.encode(), chaine_a_signer.encode(), hashlib.sha256).hexdigest()

    entetes = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": horodatage,
        "X-BAPI-RECV-WINDOW": fenetre,
        "X-BAPI-SIGN": signature,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.bybit.com/v5/asset/coin/query-info",
                headers=entetes, timeout=TIMEOUT,
            ) as resp:
                if resp.status != 200:
                    log.warning(f"bybit (signé) : statut HTTP {resp.status}")
                    return None
                data = await resp.json(content_type=None)
    except Exception as e:
        log.warning(f"bybit (signé) : échec ({e})")
        return None

    out = {}
    for c in (data.get("result", {}) or {}).get("rows", []) if isinstance(data, dict) else []:
        if not isinstance(c, dict):
            continue
        nom = str(c.get("coin", "")).upper()
        if not nom:
            continue
        reseaux = {}
        for ch in c.get("chains", []) or []:
            if not isinstance(ch, dict):
                continue
            reseaux[_normaliser_reseau(ch.get("chain"))] = {
                "retrait_ouvert": str(ch.get("chainWithdraw")) in ("1", "true", "True"),
                "depot_ouvert": str(ch.get("chainDeposit")) in ("1", "true", "True"),
            }
        if reseaux:
            out[nom] = {"reseaux": reseaux}
    return out


# ============================================================
# OKX — en-têtes OK-ACCESS-*, signature base64
# ============================================================
async def recuperer_okx() -> dict | None:
    cle_secret = _lire_cle("okx")
    if cle_secret is None:
        return None
    api_key, api_secret, passphrase = cle_secret
    if not passphrase:
        log.warning("okx (signé) : passphrase manquante (OKX_PASSPHRASE) — requête impossible")
        return None

    horodatage = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    chemin = "/api/v5/asset/currencies"
    chaine_a_signer = horodatage + "GET" + chemin
    signature = base64.b64encode(
        hmac.new(api_secret.encode(), chaine_a_signer.encode(), hashlib.sha256).digest()
    ).decode()

    entetes = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": horodatage,
        "OK-ACCESS-PASSPHRASE": passphrase,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://www.okx.com{chemin}", headers=entetes, timeout=TIMEOUT,
            ) as resp:
                if resp.status != 200:
                    log.warning(f"okx (signé) : statut HTTP {resp.status}")
                    return None
                data = await resp.json(content_type=None)
    except Exception as e:
        log.warning(f"okx (signé) : échec ({e})")
        return None

    out = {}
    for c in data.get("data", []) if isinstance(data, dict) else []:
        if not isinstance(c, dict):
            continue
        nom = str(c.get("ccy", "")).upper()
        if not nom:
            continue
        reseau = _normaliser_reseau(c.get("chain") or nom)
        entree = out.setdefault(nom, {"reseaux": {}})
        entree["reseaux"][reseau] = {
            "retrait_ouvert": str(c.get("canWd")).lower() == "true",
            "depot_ouvert": str(c.get("canDep")).lower() == "true",
        }
    return out


# ============================================================
# MEXC — même schéma de signature que Binance
# ============================================================
async def recuperer_mexc() -> dict | None:
    """
    ⚠️ Corrigé le 07/08 après un HTTP 400 réel en production : contrairement
    à Binance (qui accepte l'absence de recvWindow, 5000ms par défaut), MEXC
    exige ce paramètre explicitement dans la requête ET dans la chaîne
    signée — sans lui, l'API répond {"code":700003,"msg":"Timestamp for
    this request is outside of the recvWindow."} même avec un timestamp
    parfaitement à l'heure.
    """
    cle_secret = _lire_cle("mexc")
    if cle_secret is None:
        return None
    api_key, api_secret, _ = cle_secret

    horodatage = int(time.time() * 1000)
    requete = f"recvWindow=5000&timestamp={horodatage}"
    signature = hmac.new(api_secret.encode(), requete.encode(), hashlib.sha256).hexdigest()
    url = f"https://api.mexc.com/api/v3/capital/config/getall?{requete}&signature={signature}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers={"X-MEXC-APIKEY": api_key}, timeout=TIMEOUT
            ) as resp:
                if resp.status != 200:
                    log.warning(f"mexc (signé) : statut HTTP {resp.status}")
                    return None
                data = await resp.json(content_type=None)
    except Exception as e:
        log.warning(f"mexc (signé) : échec ({e})")
        return None

    out = {}
    for c in data if isinstance(data, list) else []:
        if not isinstance(c, dict):
            continue
        nom = str(c.get("coin", "")).upper()
        if not nom:
            continue
        reseaux = {}
        for ch in c.get("networkList", []) or []:
            if not isinstance(ch, dict):
                continue
            reseaux[_normaliser_reseau(ch.get("network") or ch.get("netWork"))] = {
                "retrait_ouvert": bool(ch.get("withdrawEnable")),
                "depot_ouvert": bool(ch.get("depositEnable")),
            }
        if reseaux:
            out[nom] = {"reseaux": reseaux}
    return out


RECUPERATEURS_SIGNES = {
    "binance": recuperer_binance,
    "bybit": recuperer_bybit,
    "okx": recuperer_okx,
    "mexc": recuperer_mexc,
}


async def recuperer_toutes_les_cles_configurees() -> dict:
    """
    Tente les 4 plateformes ; ne renvoie que celles dont une clé est
    configurée ET dont la requête a réussi. Les autres restent gérées
    comme avant (EXCHANGES_SANS_DONNEES_PUBLIQUES dans verif_retraits.py).
    """
    import asyncio
    disponibles = cles_disponibles()
    a_tenter = [ex for ex, ok in disponibles.items() if ok]
    if not a_tenter:
        return {}

    resultats = await asyncio.gather(*(RECUPERATEURS_SIGNES[ex]() for ex in a_tenter))
    obtenus = {ex: r for ex, r in zip(a_tenter, resultats) if r}
    if obtenus:
        log.info(
            f"cles_privees : {len(obtenus)} plateforme(s) débloquée(s) via clé API "
            f"({', '.join(obtenus)})"
        )
    manquantes = set(a_tenter) - set(obtenus)
    if manquantes:
        log.warning(
            f"cles_privees : clé configurée mais requête échouée pour "
            f"{', '.join(manquantes)} — vérifie la clé et ses permissions"
        )
    return obtenus

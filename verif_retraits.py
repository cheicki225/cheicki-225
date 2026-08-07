"""
Vérification de l'état des retraits/dépôts par TOKEN
======================================================
Répond à LA question qui bloque tout le reste : un écart de 13% sur PYBOBO
entre gateio et kucoin est-il exploitable, ou le token est-il simplement
prisonnier de la plateforme ?

Un arbitrage ne peut se boucler que si le token peut PHYSIQUEMENT circuler :
  - retrait OUVERT sur la plateforme d'achat
  - dépôt OUVERT sur la plateforme de vente
  - et un réseau COMMUN aux deux
Si l'une de ces trois conditions manque, l'écart peut rester à 13%
indéfiniment : ce n'est pas une opportunité, c'est un blocage structurel —
et c'est justement pour ça que l'écart ne se referme jamais.

⚠️ COUVERTURE : 3 plateformes sur 6
kucoin, bitget et gateio exposent ces états publiquement, sans clé API.
binance, bybit et okx exigent une clé API signée pour la même information —
elles sont donc marquées "inconnu" plutôt que supposées ouvertes.
Ce n'est pas une limite gênante en pratique : les plus gros écarts observés
viennent précisément de gateio et kucoin.

⚠️ DIFFÉRENCE AVEC frais_retrait.py
frais_retrait.py ne regarde QUE l'USDT (pour le transfert retour) et, pour
gateio, force `retrait_ouvert=True` car son endpoint (withdraw_status)
n'expose pas cet état. Ici on utilise /spot/currencies, qui donne les vrais
drapeaux withdraw_disabled / deposit_disabled, et on interroge N'IMPORTE
QUEL token.

Usage en ligne de commande :
    python3 verif_retraits.py COTI PYBOBO ZIL BTR BLUAI
"""

import asyncio
import logging
import sys

import aiohttp

log = logging.getLogger("verif_retraits")

TIMEOUT = aiohttp.ClientTimeout(total=25)


def _session_avec_dns_force() -> aiohttp.ClientSession:
    """
    Même contournement que symbol_discovery / orderbook_depth / frais_retrait.

    Sur Windows, pycares/aiodns échoue parfois à lire la configuration DNS
    du système (bug connu) — d'où « Could not contact DNS servers » alors
    que la connexion Internet fonctionne. On force donc explicitement les
    DNS Google. Sur Linux/Mac (déploiement Railway), ce forçage peut
    lui-même échouer selon la version de pycares installée ('Channel'
    object has no attribute 'gethostbyname') : on retombe alors sur le
    résolveur par défaut, qui fonctionne très bien sur ces systèmes.

    ⚠️ Ne JAMAIS forcer ce résolveur en dehors de Windows : c'est
    exactement ce qui cassait le bot sur Railway.
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

# Plateformes dont l'état des retraits n'est PAS récupérable sans clé API.
# Marquées "inconnu" — jamais supposées ouvertes, ce serait le pire défaut
# possible pour un outil censé détecter des blocages.
EXCHANGES_SANS_DONNEES_PUBLIQUES_BASE = ("binance", "bybit", "okx")
# Plateformes retirées de cette liste au moment du rafraîchissement, si une
# clé API en lecture seule est configurée ET que la requête signée a réussi
# (voir cles_privees.py). EXCHANGES_SANS_DONNEES_PUBLIQUES reste donc un
# tuple normal, lisible partout comme avant — juste recalculé à chaque
# rafraîchissement plutôt que figé une fois pour toutes.
EXCHANGES_SANS_DONNEES_PUBLIQUES = EXCHANGES_SANS_DONNEES_PUBLIQUES_BASE


def _normaliser_reseau(nom) -> str:
    """
    Aligne les noms de réseaux entre plateformes.

    ⚠️ Corrigé le 04/08 après mesure sur données réelles : les plateformes
    nomment le MÊME réseau de façons très différentes, et sans alignement le
    filtre conclut « aucun réseau commun » alors que le transfert est
    possible. Exemples relevés sur les vraies API :
        Arbitrum -> bitget "ARBITRUMONE", gateio "ARBEVM", kucoin "ARBITRUM"
        Base     -> gateio "BASEEVM", bitget/kucoin "BASE"
    ARBITRUMONE est le réseau n°1 de bitget (738 tokens) : l'oubli écartait
    donc à tort une grande partie de ses trajets possibles.
    """
    if not nom:
        return "?"
    n = str(nom).strip().upper()
    for caractere in ("-", "_", " ", "(", ")", ".", "/"):
        n = n.replace(caractere, "")

    # Alias explicites d'abord (les cas qu'aucune règle générale ne couvre)
    equivalences = {
        "TRC20": "TRX", "TRON": "TRX",
        "ERC20": "ETH", "ETHEREUM": "ETH", "ETHERC20": "ETH",
        "BEP20": "BSC", "BEP2": "BNB", "BSCBEP20": "BSC",
        "BINANCESMARTCHAIN": "BSC", "BNBSMARTCHAIN": "BSC", "BNBCHAIN": "BSC",
        "SOLANA": "SOL", "SPL": "SOL",
        "POLYGON": "MATIC", "POL": "MATIC",
        "ARBITRUMONE": "ARBITRUM", "ARB": "ARBITRUM", "ARBITRUMNOVA": "ARBITRUMNOVA",
        "OPTIMISM": "OP", "OPTIMISMETH": "OP",
        "AVALANCHECCHAIN": "AVAXC", "AVAXCCHAIN": "AVAXC", "CCHAIN": "AVAXC",
        "AVALANCHEXCHAIN": "AVAXX",
    }
    if n in equivalences:
        return equivalences[n]

    # Suffixes purement décoratifs ajoutés par certaines plateformes
    # (gateio suffixe volontiers "EVM" : ARBEVM, BASEEVM, OPEVM...)
    for suffixe in ("EVM", "CHAIN", "NETWORK", "MAINNET"):
        if n.endswith(suffixe) and len(n) > len(suffixe) + 1:
            n = n[: -len(suffixe)]
            break

    # Nouvelle passe d'alias après retrait du suffixe (ARBEVM -> ARB -> ARBITRUM)
    return equivalences.get(n, n)


def _vrai_sauf_si_faux(valeur) -> bool:
    """Absent ou nul = on considère ouvert ; seul un False explicite ferme."""
    return str(valeur).lower() not in ("false", "0", "none")


# ============================================================
# PARSERS — un par plateforme, tous sur des endpoints publics
# ============================================================
def _parser_kucoin(data, token: str) -> dict | None:
    """https://api.kucoin.com/api/v3/currencies"""
    if not isinstance(data, dict):
        return None
    for devise in data.get("data", []) or []:
        if not isinstance(devise, dict) or str(devise.get("currency", "")).upper() != token:
            continue
        reseaux = {}
        for chaine in devise.get("chains", []) or []:
            if not isinstance(chaine, dict):
                continue
            reseaux[_normaliser_reseau(chaine.get("chainName") or chaine.get("chainId"))] = {
                "retrait_ouvert": _vrai_sauf_si_faux(chaine.get("isWithdrawEnabled")),
                "depot_ouvert": _vrai_sauf_si_faux(chaine.get("isDepositEnabled")),
            }
        return {"trouve": True, "reseaux": reseaux}
    return None


def _parser_bitget(data, token: str) -> dict | None:
    """https://api.bitget.com/api/v2/spot/public/coins"""
    if not isinstance(data, dict):
        return None
    for devise in data.get("data", []) or []:
        if not isinstance(devise, dict) or str(devise.get("coin", "")).upper() != token:
            continue
        reseaux = {}
        for chaine in devise.get("chains", []) or []:
            if not isinstance(chaine, dict):
                continue
            reseaux[_normaliser_reseau(chaine.get("chain"))] = {
                "retrait_ouvert": _vrai_sauf_si_faux(chaine.get("withdrawable")),
                "depot_ouvert": _vrai_sauf_si_faux(chaine.get("rechargeable")),
            }
        return {"trouve": True, "reseaux": reseaux}
    return None


def _parser_gateio(data, token: str) -> dict | None:
    """
    https://api.gateio.ws/api/v4/spot/currencies

    Cet endpoint expose withdraw_disabled / deposit_disabled — contrairement
    à /wallet/withdraw_status utilisé par frais_retrait.py, qui ne les donne
    pas (d'où le `retrait_ouvert=True` codé en dur là-bas).
    Format : une entrée par (devise, chaîne), currency = "COTI" ou "COTI_ETH".
    """
    if not isinstance(data, list):
        return None
    reseaux = {}
    trouve = False
    for devise in data:
        if not isinstance(devise, dict):
            continue
        currency = str(devise.get("currency", "")).upper()
        base = currency.split("_")[0]
        if base != token:
            continue
        trouve = True
        chaine = devise.get("chain") or (currency.split("_", 1)[1] if "_" in currency else token)
        reseaux[_normaliser_reseau(chaine)] = {
            # Attention à la double négation : le champ dit "disabled"
            "retrait_ouvert": not _vrai_sauf_si_faux(devise.get("withdraw_disabled")),
            "depot_ouvert": not _vrai_sauf_si_faux(devise.get("deposit_disabled")),
            "trading_ouvert": not _vrai_sauf_si_faux(devise.get("trade_disabled")),
        }
    return {"trouve": True, "reseaux": reseaux} if trouve else None


def _parser_coinex(data, token: str) -> dict | None:
    """
    https://api.coinex.com/v2/assets/all-deposit-withdraw-config

    Ajouté le 04/08 en même temps que l'intégration de coinex : sans ce
    parser, le filtre de retraits aurait classé toutes les paires coinex
    en « inconnu » et le mode strict les aurait toutes écartées.
    """
    if not isinstance(data, dict):
        return None
    for entree in data.get("data") or []:
        if not isinstance(entree, dict):
            continue
        actif = entree.get("asset") if isinstance(entree.get("asset"), dict) else {}
        nom = str(actif.get("ccy") or entree.get("ccy", "")).upper()
        if nom != token:
            continue
        reseaux = {}
        for chaine in entree.get("chains") or []:
            if not isinstance(chaine, dict):
                continue
            reseaux[_normaliser_reseau(chaine.get("chain"))] = {
                "retrait_ouvert": _vrai_sauf_si_faux(chaine.get("withdraw_enabled")),
                "depot_ouvert": _vrai_sauf_si_faux(chaine.get("deposit_enabled")),
            }
        if not reseaux:
            reseaux = {"?": {
                "retrait_ouvert": _vrai_sauf_si_faux(actif.get("withdraw_enabled")),
                "depot_ouvert": _vrai_sauf_si_faux(actif.get("deposit_enabled")),
            }}
        return {"trouve": True, "reseaux": reseaux}
    return None


def _parser_bitvavo(data, token: str) -> dict | None:
    """
    https://api.bitvavo.com/v2/assets

    Format confirmé par les SDK officiels (Python/Node/Go/PHP) : chaque
    devise a un statut GLOBAL (depositStatus/withdrawalStatus), pas un
    statut par réseau comme kucoin/bitget/gateio — Bitvavo n'expose qu'une
    liste de noms de réseaux supportés, sans état individuel par réseau.
    On applique donc le même statut global à tous les réseaux listés.
    """
    if not isinstance(data, list):
        return None
    for c in data:
        if not isinstance(c, dict):
            continue
        if str(c.get("symbol", "")).upper() != token:
            continue
        retrait_ok = str(c.get("withdrawalStatus", "")).upper() == "OK"
        depot_ok = str(c.get("depositStatus", "")).upper() == "OK"
        reseaux_bruts = c.get("networks") or ["?"]
        reseaux = {
            _normaliser_reseau(r): {"retrait_ouvert": retrait_ok, "depot_ouvert": depot_ok}
            for r in reseaux_bruts
        }
        return {"trouve": True, "reseaux": reseaux}
    return None


def _parser_whitebit(data, token: str) -> dict | None:
    """
    https://whitebit.com/api/v4/public/assets

    Dict keyé par symbole ("BTC": {...}), pas de liste — format différent
    des autres parsers. Statut GLOBAL can_withdraw/can_deposit, comme
    Kraken et Bitvavo — pas de détail par réseau individuel.
    """
    if not isinstance(data, dict):
        return None
    infos = data.get(token)
    if not isinstance(infos, dict):
        return None
    return {
        "trouve": True,
        "reseaux": {"?": {
            "retrait_ouvert": bool(infos.get("can_withdraw")),
            "depot_ouvert": bool(infos.get("can_deposit")),
        }},
    }


SOURCES = {
    "kucoin": ("https://api.kucoin.com/api/v3/currencies", _parser_kucoin),
    "bitget": ("https://api.bitget.com/api/v2/spot/public/coins", _parser_bitget),
    "gateio": ("https://api.gateio.ws/api/v4/spot/currencies", _parser_gateio),
    "coinex": ("https://api.coinex.com/v2/assets/all-deposit-withdraw-config", _parser_coinex),
    "bitvavo": ("https://api.bitvavo.com/v2/assets", _parser_bitvavo),
    "whitebit": ("https://whitebit.com/api/v4/public/assets", _parser_whitebit),
}


# ============================================================
# RÉCUPÉRATION
# ============================================================
async def _telecharger_tout() -> dict:
    """Une seule requête par plateforme, réutilisée pour tous les tokens."""
    brut = {}

    async def _un(exchange, url):
        try:
            async with _session_avec_dns_force() as session:
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status != 200:
                        log.warning(f"{exchange} : statut HTTP {resp.status}")
                        return exchange, None
                    return exchange, await resp.json(content_type=None)
        except Exception as e:
            log.warning(f"{exchange} : échec ({e})")
            return exchange, None

    resultats = await asyncio.gather(
        *(_un(ex, url) for ex, (url, _) in SOURCES.items())
    )
    for exchange, data in resultats:
        brut[exchange] = data
    return brut


def _etat_token(brut: dict, token: str) -> dict:
    """État du token sur chaque plateforme couverte (publiques + débloquées par clé)."""
    token = token.upper().replace("USDT", "") if token.upper().endswith("USDT") else token.upper()
    etat = {}
    for exchange, (_, parser) in SOURCES.items():
        data = brut.get(exchange)
        if data is None:
            etat[exchange] = {"statut": "erreur_reseau", "reseaux": {}}
            continue
        try:
            resultat = parser(data, token)
        except Exception as e:
            log.warning(f"{exchange} : erreur de lecture ({e})")
            etat[exchange] = {"statut": "erreur_lecture", "reseaux": {}}
            continue
        if resultat is None:
            etat[exchange] = {"statut": "token_absent", "reseaux": {}}
        else:
            etat[exchange] = {"statut": "ok", "reseaux": resultat["reseaux"]}

    # Plateformes débloquées via clé API — déjà pré-analysées par token
    # (format identique aux autres) dans _donnees_signees, donc pas besoin
    # de "parser" : juste une recherche directe.
    for exchange, tokens in _donnees_signees.items():
        infos = tokens.get(token)
        etat[exchange] = (
            {"statut": "ok", "reseaux": infos["reseaux"]}
            if infos else {"statut": "token_absent", "reseaux": {}}
        )

    return etat


def analyser_paire(etat: dict, ex_achat: str, ex_vente: str) -> dict:
    """
    Le cycle peut-il se boucler de ex_achat vers ex_vente ?
    Il faut un réseau commun, ouvert au RETRAIT côté achat ET au DÉPÔT côté
    vente. Un réseau rapide côté source ne sert à rien si la destination ne
    l'accepte pas.
    """
    for exchange in (ex_achat, ex_vente):
        if exchange in EXCHANGES_SANS_DONNEES_PUBLIQUES:
            return {"verdict": "inconnu", "reseaux_ok": [],
                    "raison": f"{exchange} n'expose pas ces données sans clé API"}

    source = etat.get(ex_achat, {})
    dest = etat.get(ex_vente, {})

    for exchange, e in ((ex_achat, source), (ex_vente, dest)):
        if e.get("statut") == "token_absent":
            return {"verdict": "bloque", "reseaux_ok": [],
                    "raison": f"token introuvable sur {exchange}"}
        if e.get("statut", "").startswith("erreur"):
            return {"verdict": "inconnu", "reseaux_ok": [],
                    "raison": f"données indisponibles pour {exchange}"}

    communs = []
    for reseau, infos_src in source.get("reseaux", {}).items():
        infos_dst = dest.get("reseaux", {}).get(reseau)
        if infos_dst and infos_src.get("retrait_ouvert") and infos_dst.get("depot_ouvert"):
            communs.append(reseau)

    if communs:
        return {"verdict": "ouvert", "reseaux_ok": sorted(communs), "raison": ""}

    retraits = [r for r, i in source.get("reseaux", {}).items() if i.get("retrait_ouvert")]
    if not retraits:
        return {"verdict": "bloque", "reseaux_ok": [],
                "raison": f"AUCUN retrait ouvert sur {ex_achat}"}

    depots = [r for r, i in dest.get("reseaux", {}).items() if i.get("depot_ouvert")]
    if not depots:
        return {"verdict": "bloque", "reseaux_ok": [],
                "raison": f"AUCUN dépôt ouvert sur {ex_vente}"}

    return {"verdict": "bloque", "reseaux_ok": [],
            "raison": f"aucun réseau commun ({ex_achat} : {', '.join(sorted(retraits)[:4])} / "
                      f"{ex_vente} : {', '.join(sorted(depots)[:4])})"}


async def verifier(tokens: list[str], paires: list[tuple[str, str]] | None = None) -> dict:
    """
    Vérifie une liste de tokens. `paires` par défaut : toutes les
    combinaisons entre les plateformes couvertes.
    """
    if paires is None:
        couverts = list(SOURCES)
        paires = [(a, b) for a in couverts for b in couverts if a != b]

    brut = await _telecharger_tout()
    rapport = {}
    for token in tokens:
        etat = _etat_token(brut, token)
        rapport[token.upper()] = {
            "etat": etat,
            "paires": {f"{a}->{b}": analyser_paire(etat, a, b) for a, b in paires},
        }
    return rapport


def formater(rapport: dict) -> str:
    """Rapport lisible en console."""
    lignes = []
    for token, donnees in rapport.items():
        lignes.append(f"\n{'=' * 62}\n{token}\n{'=' * 62}")

        for exchange, e in donnees["etat"].items():
            if e["statut"] == "token_absent":
                lignes.append(f"  {exchange:<8} : token non listé")
                continue
            if e["statut"].startswith("erreur"):
                lignes.append(f"  {exchange:<8} : données indisponibles ({e['statut']})")
                continue
            ouverts = [r for r, i in e["reseaux"].items() if i.get("retrait_ouvert")]
            fermes = [r for r, i in e["reseaux"].items() if not i.get("retrait_ouvert")]
            lignes.append(
                f"  {exchange:<8} : retrait ouvert sur {len(ouverts)}/{len(e['reseaux'])} réseaux"
                + (f" — ouverts : {', '.join(sorted(ouverts)[:5])}" if ouverts else " — AUCUN")
                + (f" | fermés : {', '.join(sorted(fermes)[:5])}" if fermes else "")
            )

        lignes.append("")
        for paire, analyse in donnees["paires"].items():
            marque = {"ouvert": "[OK]     ", "bloque": "[BLOQUE] ", "inconnu": "[?]      "}[analyse["verdict"]]
            detail = (
                f"réseaux utilisables : {', '.join(analyse['reseaux_ok'][:5])}"
                if analyse["reseaux_ok"] else analyse["raison"]
            )
            lignes.append(f"  {marque}{paire:<20} {detail}")

    return "\n".join(lignes)


def etat_par_exchange(symbole: str) -> dict:
    """
    Vue « une crypto, toutes les plateformes » : pour chaque plateforme,
    l'état du retrait et du dépôt, avec les réseaux concernés.

    Complète paire_exploitable(), qui répond « puis-je aller de A vers B ».
    Ici on répond « où ce token peut-il entrer et sortir ».

    Statuts possibles par plateforme :
      "ouvert"   — au moins un réseau ouvert
      "ferme"    — le token est listé mais AUCUN réseau n'est ouvert
      "absent"   — le token n'est pas listé sur cette plateforme
      "inconnu"  — plateforme sans données publiques (clé API requise),
                   ou données pas encore chargées
    """
    token = symbole.upper()
    if token.endswith("USDT"):
        token = token[:-4]

    resultat = {"token": token, "exchanges": {}}

    for exchange in EXCHANGES_SANS_DONNEES_PUBLIQUES:
        resultat["exchanges"][exchange] = {
            "retrait": "inconnu", "depot": "inconnu", "reseaux_retrait": [],
            "reseaux_depot": [], "raison": "clé API requise",
        }

    if not _donnees_chargees:
        for exchange in SOURCES:
            resultat["exchanges"][exchange] = {
                "retrait": "inconnu", "depot": "inconnu", "reseaux_retrait": [],
                "reseaux_depot": [], "raison": "données pas encore chargées",
            }
        return resultat

    try:
        etat = _etat_token(_donnees_brutes, token)
    except Exception as e:
        log.warning(f"etat_par_exchange({token}) : {e}")
        return resultat

    for exchange, infos in etat.items():
        statut = infos.get("statut")
        if statut == "token_absent":
            resultat["exchanges"][exchange] = {
                "retrait": "absent", "depot": "absent", "reseaux_retrait": [],
                "reseaux_depot": [], "raison": "non listé",
            }
            continue
        if statut != "ok":
            resultat["exchanges"][exchange] = {
                "retrait": "inconnu", "depot": "inconnu", "reseaux_retrait": [],
                "reseaux_depot": [], "raison": statut,
            }
            continue

        reseaux = infos.get("reseaux", {})
        ouverts_retrait = sorted(r for r, i in reseaux.items() if i.get("retrait_ouvert"))
        ouverts_depot = sorted(r for r, i in reseaux.items() if i.get("depot_ouvert"))
        resultat["exchanges"][exchange] = {
            "retrait": "ouvert" if ouverts_retrait else "ferme",
            "depot": "ouvert" if ouverts_depot else "ferme",
            "reseaux_retrait": ouverts_retrait,
            "reseaux_depot": ouverts_depot,
            "total_reseaux": len(reseaux),
            "raison": "",
        }

    return resultat


def matrice(symboles: list[str]) -> list[dict]:
    """
    Même chose pour une liste de tokens, avec un résumé exploitable.

    `paires_possibles` est l'information qui compte vraiment : à quoi bon
    savoir qu'un retrait est ouvert quelque part si aucune destination
    n'accepte le dépôt sur un réseau commun ?
    """
    lignes = []
    for symbole in symboles:
        etat = etat_par_exchange(symbole)
        exchanges = etat["exchanges"]

        sorties = [ex for ex, i in exchanges.items() if i["retrait"] == "ouvert"]
        entrees = [ex for ex, i in exchanges.items() if i["depot"] == "ouvert"]

        paires = []
        for source in sorties:
            for dest in entrees:
                if source == dest:
                    continue
                communs = set(exchanges[source]["reseaux_retrait"]) & set(exchanges[dest]["reseaux_depot"])
                if communs:
                    paires.append({
                        "de": source, "vers": dest, "reseaux": sorted(communs),
                    })

        lignes.append({
            **etat,
            "sorties_ouvertes": sorted(sorties),
            "entrees_ouvertes": sorted(entrees),
            "paires_possibles": paires,
            "nb_paires_possibles": len(paires),
            "circulable": bool(paires),
        })
    return lignes


def resume_telegram(rapport: dict) -> str:
    """Version courte, pour Telegram."""
    lignes = ["🔎 <b>État des retraits</b>\n"]
    for token, donnees in rapport.items():
        verdicts = [a["verdict"] for a in donnees["paires"].values()]
        ouverts = verdicts.count("ouvert")
        total = len(verdicts)
        if ouverts == 0:
            emoji, texte = "🔴", "aucune paire exploitable"
        elif ouverts == total:
            emoji, texte = "🟢", "toutes les paires exploitables"
        else:
            emoji, texte = "🟡", f"{ouverts}/{total} paires exploitables"
        lignes.append(f"{emoji} <b>{token}</b> — {texte}")

        for paire, analyse in donnees["paires"].items():
            if analyse["verdict"] == "bloque":
                lignes.append(f"   ❌ {paire} : {analyse['raison']}")
    return "\n".join(lignes)


# ============================================================
# UTILISATION EN DIRECT DANS LE BOT
# ============================================================
# La détection appelle paire_exploitable() à chaque tick de prix, pour
# chaque permutation d'exchanges. Tout est donc servi depuis un cache
# mémoire, alimenté par une seule série de requêtes toutes les 6 heures.
_donnees_brutes: dict = {}
_donnees_signees: dict = {}  # {exchange: {token: {"reseaux": {...}}}}, via cles_privees.py
_cache_verdicts: dict[tuple, dict] = {}
_donnees_chargees = False

# Compteurs, pour savoir ce que le filtre a réellement écarté
_stats_filtre = {"autorise": 0, "bloque": 0, "inconnu": 0}


def donnees_disponibles() -> bool:
    return _donnees_chargees


def statistiques_filtre() -> dict:
    return dict(_stats_filtre)


async def rafraichir():
    """Recharge l'état des retraits : sources publiques + sources débloquées par clé API."""
    global _donnees_brutes, _donnees_signees, _donnees_chargees, EXCHANGES_SANS_DONNEES_PUBLIQUES

    brut = await _telecharger_tout()
    recues = [ex for ex, d in brut.items() if d is not None]
    if not recues and not _donnees_signees:
        log.warning("verif_retraits : aucune plateforme n'a répondu — filtre inopérant")
        return
    _donnees_brutes = brut

    # Tentative des plateformes signées (binance/bybit/okx/mexc). Ne fait
    # RIEN si aucune clé n'est configurée dans Railway — comportement
    # identique à avant dans ce cas.
    try:
        import cles_privees
        obtenus = await cles_privees.recuperer_toutes_les_cles_configurees()
        if obtenus:
            _donnees_signees = obtenus
            EXCHANGES_SANS_DONNEES_PUBLIQUES = tuple(
                ex for ex in EXCHANGES_SANS_DONNEES_PUBLIQUES_BASE if ex not in obtenus
            )
    except Exception as e:
        log.warning(f"verif_retraits : requêtes signées indisponibles ({e})")

    _donnees_chargees = True
    _cache_verdicts.clear()
    total_plateformes = len(SOURCES) + len(_donnees_signees)
    log.info(
        f"verif_retraits : {len(recues) + len(_donnees_signees)}/{total_plateformes} plateformes "
        f"chargées ({', '.join(recues + list(_donnees_signees))})"
        + (f" | débloquées par clé : {', '.join(_donnees_signees)}" if _donnees_signees else "")
    )


async def boucle_rafraichissement(intervalle_sec: float = 6 * 3600):
    # Première tentative immédiate, puis rappel toutes les 6h. Tant que les
    # données ne sont pas chargées, le mode strict bloque TOUT — un silence
    # complet du bot serait alors dû au filtre, pas à l'absence d'écarts.
    # D'où ce réessai rapproché et cet avertissement explicite.
    echecs = 0
    while True:
        try:
            await rafraichir()
        except Exception as e:
            log.error(f"verif_retraits : erreur de rafraîchissement ({e})")

        if not _donnees_chargees:
            echecs += 1
            log.warning(
                f"⚠️ verif_retraits : données toujours indisponibles ({echecs} tentative(s)). "
                f"En mode strict, AUCUNE alerte ne peut passer tant que c'est le cas — "
                f"bascule FILTRE_RETRAITS_MODE sur 'souple' ou FILTRE_RETRAITS_ACTIF sur False "
                f"si le bot reste muet."
            )
            await asyncio.sleep(min(300 * echecs, 1800))  # réessai rapproché, plafonné à 30 min
            continue

        echecs = 0
        await asyncio.sleep(intervalle_sec)


def paire_exploitable(symbole: str, ex_achat: str, ex_vente: str) -> dict:
    """
    Synchrone et mis en cache : appelable dans la boucle de détection.

    Retourne {"verdict": "ouvert"|"bloque"|"inconnu", "reseaux_ok", "raison"}.

    "inconnu" n'est JAMAIS traité comme "ouvert" ici — c'est au code appelant
    de décider quoi en faire (voir FILTRE_RETRAITS_MODE dans config.py).
    Supposer qu'un retrait inconnu est ouvert reviendrait à désactiver
    silencieusement le filtre sur la moitié des plateformes.
    """
    if not _donnees_chargees:
        return {"verdict": "inconnu", "reseaux_ok": [], "raison": "données pas encore chargées"}

    token = symbole.upper()
    if token.endswith("USDT"):
        token = token[:-4]

    cle = (token, ex_achat, ex_vente)
    en_cache = _cache_verdicts.get(cle)
    if en_cache is not None:
        return en_cache

    try:
        etat = _etat_token(_donnees_brutes, token)
        resultat = analyser_paire(etat, ex_achat, ex_vente)
    except Exception as e:
        resultat = {"verdict": "inconnu", "reseaux_ok": [], "raison": f"erreur d'analyse ({e})"}

    _cache_verdicts[cle] = resultat
    return resultat


def autorise_trade(symbole: str, ex_achat: str, ex_vente: str, mode: str = "strict") -> tuple[bool, str]:
    """
    Décision finale pour le bot.

    mode "strict" : seul un retrait VÉRIFIÉ ouvert passe. Les paires
        impliquant binance/bybit/okx (pas de données publiques) sont donc
        écartées — c'est volontaire, mais ça réduit fortement le champ.
    mode "souple" : seuls les blocages CONFIRMÉS écartent. Les cas inconnus
        passent, comme avant l'ajout de ce filtre.
    """
    resultat = paire_exploitable(symbole, ex_achat, ex_vente)
    verdict = resultat["verdict"]

    if verdict == "ouvert":
        _stats_filtre["autorise"] += 1
        return True, ""
    if verdict == "bloque":
        _stats_filtre["bloque"] += 1
        return False, resultat["raison"]

    _stats_filtre["inconnu"] += 1
    if mode == "souple":
        return True, ""
    return False, resultat["raison"] or "état des retraits inconnu"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    tokens = [t.upper() for t in sys.argv[1:]] or ["COTI", "PYBOBO", "ZIL", "BTR", "BLUAI"]

    print(f"Vérification de : {', '.join(tokens)}")
    print("(kucoin, bitget, gateio — binance/bybit/okx exigent une clé API)\n")

    rapport = asyncio.run(verifier(tokens))
    print(formater(rapport))

    print(f"\n{'=' * 62}\nLECTURE DU RÉSULTAT\n{'=' * 62}")
    print("[OK]     le token peut circuler : l'écart mérite d'être creusé")
    print("[BLOQUE] le cycle ne peut PAS se boucler — l'écart n'est pas")
    print("         exploitable, et c'est très probablement POURQUOI il persiste")
    print("[?]      plateforme sans données publiques (clé API requise)")

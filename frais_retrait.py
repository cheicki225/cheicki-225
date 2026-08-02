"""
Frais de transfert réels entre plateformes
===============================================
Une première version retenait simplement le réseau le moins cher de la
plateforme de départ. C'était FAUX : un transfert n'est possible que sur
un réseau supporté par les DEUX plateformes. Retenir un réseau de niche à
0,001$ que la destination n'accepte pas donnait un coût irréaliste.

Ce module calcule le coût réel d'un transfert d'USDT de A vers B :

  1. réseaux où le RETRAIT est ouvert sur A
  2. ∩ réseaux où le DÉPÔT est ouvert sur B
  3. filtrés par le montant minimum de retrait
  4. → le moins cher de ceux qui restent

Les plateformes nomment les mêmes réseaux différemment (TRC20 / TRX /
Tron…), d'où la normalisation avant intersection.

⚠️ COUVERTURE PARTIELLE — à connaître :
Seules KuCoin, Bitget et Gate.io exposent ces données sans authentification.
Binance, Bybit et OKX exigent une clé API signée. Dès qu'une des deux
plateformes d'un transfert n'est pas couverte, le résultat est une
ESTIMATION, signalée par `est_estime = True`. Ne la confonds pas avec une
mesure.

⚠️ Non testable dans mon environnement (pas d'accès réseau aux
plateformes). La logique a été testée avec des réponses simulées reprenant
leur format documenté. Teste ce fichier seul :
    python3 frais_retrait.py
"""

import asyncio
import logging
import platform

import aiohttp

log = logging.getLogger("frais_retrait")

# Estimation prudente quand une plateforme n'expose pas ses frais.
# Volontairement HAUTE : mieux vaut sous-estimer un profit que l'inverse.
FRAIS_ESTIME_USDT = 1.0

# Les plateformes nomment le même réseau de façons différentes. Sans cette
# normalisation, l'intersection entre deux plateformes serait presque
# toujours vide et on retomberait systématiquement sur l'estimation.
_ALIAS_RESEAUX = {
    "trc20": "TRON", "trx": "TRON", "tron": "TRON",
    "erc20": "ETH", "eth": "ETH", "ethereum": "ETH",
    "bep20": "BSC", "bsc": "BSC", "bnbsmartchain": "BSC", "bnb": "BSC",
    "sol": "SOL", "solana": "SOL",
    "matic": "POLYGON", "polygon": "POLYGON", "pol": "POLYGON",
    "arbitrum": "ARBITRUM", "arb": "ARBITRUM", "arbitrumone": "ARBITRUM", "arbevm": "ARBITRUM",
    "optimism": "OPTIMISM", "op": "OPTIMISM", "opeth": "OPTIMISM",
    "avaxc": "AVAX-C", "avaxcchain": "AVAX-C", "cchain": "AVAX-C",
    "ton": "TON", "toncoin": "TON",
    "apt": "APTOS", "aptos": "APTOS",
    "base": "BASE", "basemainnet": "BASE",
    "near": "NEAR", "algo": "ALGO", "algorand": "ALGO",
    "xtz": "TEZOS", "tezos": "TEZOS",
    "kava": "KAVA", "celo": "CELO", "ftm": "FANTOM", "fantom": "FANTOM",
}


def _normaliser(nom) -> str:
    """Ramène les noms de réseau à une forme commune ('TRC20' et 'TRX' -> 'TRON')."""
    if not nom:
        return "?"
    cle = str(nom).strip().lower().replace("-", "").replace("_", "").replace(" ", "")
    return _ALIAS_RESEAUX.get(cle, str(nom).strip().upper())


# {exchange: {RESEAU: {"frais", "min_retrait", "retrait_ouvert", "depot_ouvert"}}}
_reseaux: dict[str, dict[str, dict]] = {}


def _session() -> aiohttp.ClientSession:
    if platform.system() == "Windows":
        try:
            from aiohttp.resolver import AsyncResolver
            resolver = AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
            return aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=resolver))
        except Exception:
            pass
    return aiohttp.ClientSession()


def _flottant(valeur, defaut=0.0) -> float:
    try:
        return float(valeur)
    except (TypeError, ValueError):
        return defaut


def _parser_kucoin(data) -> dict:
    """/api/v3/currencies — public. `chains[]` avec withdrawalMinFee, withdrawalMinSize."""
    resultat = {}
    if not isinstance(data, dict):
        return resultat
    for devise in data.get("data", []) or []:
        if not isinstance(devise, dict) or devise.get("currency") != "USDT":
            continue
        for chaine in devise.get("chains", []) or []:
            if not isinstance(chaine, dict):
                continue
            reseau = _normaliser(chaine.get("chainName") or chaine.get("chainId"))
            resultat[reseau] = {
                "frais": _flottant(chaine.get("withdrawalMinFee")),
                "min_retrait": _flottant(chaine.get("withdrawalMinSize")),
                "retrait_ouvert": chaine.get("isWithdrawEnabled") is not False,
                "depot_ouvert": chaine.get("isDepositEnabled") is not False,
            }
        break
    return resultat


def _parser_bitget(data) -> dict:
    """/api/v2/spot/public/coins — public. `chains[]` avec withdrawFee, minWithdrawAmount."""
    resultat = {}
    if not isinstance(data, dict):
        return resultat
    for devise in data.get("data", []) or []:
        if not isinstance(devise, dict) or devise.get("coin") != "USDT":
            continue
        for chaine in devise.get("chains", []) or []:
            if not isinstance(chaine, dict):
                continue
            reseau = _normaliser(chaine.get("chain"))
            resultat[reseau] = {
                "frais": _flottant(chaine.get("withdrawFee")),
                "min_retrait": _flottant(chaine.get("minWithdrawAmount")),
                "retrait_ouvert": str(chaine.get("withdrawable")).lower() != "false",
                "depot_ouvert": str(chaine.get("rechargeable")).lower() != "false",
            }
        break
    return resultat


def _parser_gateio(data) -> dict:
    """/api/v4/wallet/withdraw_status — public. `withdraw_fix_on_chains`."""
    resultat = {}
    if not isinstance(data, list):
        return resultat
    for devise in data:
        if not isinstance(devise, dict) or devise.get("currency") != "USDT":
            continue
        for nom, frais in (devise.get("withdraw_fix_on_chains") or {}).items():
            resultat[_normaliser(nom)] = {
                "frais": _flottant(frais),
                "min_retrait": 0.0,   # non exposé par cet endpoint
                "retrait_ouvert": True,
                "depot_ouvert": True,
            }
        break
    return resultat


_SOURCES_PUBLIQUES = {
    "kucoin": ("https://api.kucoin.com/api/v3/currencies", _parser_kucoin),
    "bitget": ("https://api.bitget.com/api/v2/spot/public/coins", _parser_bitget),
    "gateio": ("https://api.gateio.ws/api/v4/wallet/withdraw_status", _parser_gateio),
}


async def _rafraichir_une_fois():
    """Ne vide jamais les données déjà connues : une panne réseau ne doit rien effacer."""
    async with _session() as session:
        for exchange, (url, parser) in _SOURCES_PUBLIQUES.items():
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status != 200:
                        log.warning(f"frais_retrait : {exchange} — statut HTTP {resp.status}")
                        continue
                    reseaux = parser(await resp.json(content_type=None))
            except Exception as e:
                log.warning(f"frais_retrait : échec {exchange} ({e})")
                continue

            if reseaux:
                _reseaux[exchange] = reseaux
                ouverts = [r for r, d in reseaux.items() if d["retrait_ouvert"]]
                log.info(f"frais_retrait : {exchange} — {len(reseaux)} réseaux ({len(ouverts)} ouverts au retrait)")
            else:
                log.warning(f"frais_retrait : {exchange} — USDT introuvable dans la réponse")

    log.info(f"frais_retrait : {len(_reseaux)}/6 plateformes couvertes (les autres seront estimées)")


async def boucle_rafraichissement(intervalle_sec: float = 6 * 3600):
    while True:
        try:
            await _rafraichir_une_fois()
        except Exception as e:
            log.error(f"frais_retrait : erreur de rafraîchissement ({e})")
        await asyncio.sleep(intervalle_sec)


def frais_transfert(ex_source: str, ex_dest: str, montant_usdt: float = 0.0) -> dict:
    """
    Coût réel d'un transfert d'USDT de ex_source vers ex_dest.

    Retourne {"frais", "reseau", "est_estime", "raison"}.
    `est_estime` vaut True dès qu'une des deux plateformes n'est pas
    couverte par les données publiques — la valeur reste alors indicative.
    """
    src = _reseaux.get(ex_source)
    dst = _reseaux.get(ex_dest)

    if not src or not dst:
        manquantes = [n for n, d in ((ex_source, src), (ex_dest, dst)) if not d]
        return {
            "frais": FRAIS_ESTIME_USDT, "reseau": "estimation", "est_estime": True,
            "raison": f"frais non publics pour {', '.join(manquantes)}",
        }

    candidats = []
    for reseau, infos_src in src.items():
        if not infos_src["retrait_ouvert"]:
            continue
        infos_dst = dst.get(reseau)
        if not infos_dst or not infos_dst["depot_ouvert"]:
            continue  # réseau inutilisable : la destination ne l'accepte pas
        if montant_usdt and infos_src["min_retrait"] > montant_usdt:
            continue  # montant minimum de retrait supérieur au trade
        candidats.append((infos_src["frais"], reseau))

    if not candidats:
        return {
            "frais": FRAIS_ESTIME_USDT, "reseau": "estimation", "est_estime": True,
            "raison": f"aucun réseau commun utilisable entre {ex_source} et {ex_dest}",
        }

    frais, reseau = min(candidats, key=lambda x: x[0])
    return {"frais": frais, "reseau": reseau, "est_estime": False, "raison": ""}


def frais_retrait_usdt(ex_source: str, ex_dest: str = None, montant_usdt: float = 0.0) -> float:
    """Raccourci ne renvoyant que le montant des frais."""
    if ex_dest is None:
        return FRAIS_ESTIME_USDT
    return frais_transfert(ex_source, ex_dest, montant_usdt)["frais"]


def detail(ex_source: str, ex_dest: str = None, montant_usdt: float = 0.0) -> dict:
    if ex_dest is None:
        return {"frais": FRAIS_ESTIME_USDT, "reseau": "estimation",
                "est_estime": True, "raison": "destination inconnue"}
    return frais_transfert(ex_source, ex_dest, montant_usdt)


def tous() -> dict:
    return {ex: dict(r) for ex, r in _reseaux.items()}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    async def _test():
        print("Récupération des réseaux de retrait USDT...\n")
        await _rafraichir_une_fois()
        for ex in sorted(_reseaux):
            print(f"\n{ex} :")
            for reseau, d in sorted(_reseaux[ex].items(), key=lambda x: x[1]["frais"]):
                etat = "ouvert" if d["retrait_ouvert"] else "FERMÉ"
                print(f"   {reseau:<12} {d['frais']:>8.4f}$  min {d['min_retrait']:>8.2f}  {etat}")

        print("\n\nCoût réel des transferts (50$) :")
        noms = list(_SOURCES_PUBLIQUES)
        for a in noms:
            for b in noms:
                if a == b:
                    continue
                r = frais_transfert(a, b, 50)
                marque = " (estimé)" if r["est_estime"] else ""
                print(f"  {a:<8} -> {b:<8} : {r['frais']:>7.4f}$ via {r['reseau']}{marque}")

    asyncio.run(_test())

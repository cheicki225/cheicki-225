"""
Logger d'opportunités + suivi de confirmation
==================================================
Deux problèmes à résoudre avant de pouvoir entraîner un modèle ML :

1. On n'a pas encore de vraies données (le bot ne fait que détecter,
   pas encore exécuter de trades réels)
2. Un modèle supervisé (XGBoost) a besoin d'un "résultat" pour apprendre
   (l'opportunité était-elle réelle et exploitable, ou un mirage qui a
   disparu avant qu'on ait pu agir ?)

Ce module :
- Enregistre CHAQUE opportunité détectée avec toutes ses features dans un CSV
- Revérifie automatiquement 2s ET 5s plus tard, en parallèle et indépendamment
  l'une de l'autre, si le spread était encore là (paper trading léger, sans
  exécution réelle) -> ça donne un label "confirmee" / "disparue" exploitable
  pour entraîner un modèle plus tard
- Calcule en plus un spread NET RÉEL (frais de retrait inclus, comme
  paper_trading.py) à 5s, et un label "rentable_apres_5s" — "confirmée" au
  sens ci-dessus ne veut dire que "toujours au-dessus du seuil de collecte
  bas (0.05%, frais de trading seulement)", ce qui n'implique PAS que le
  trade serait resté rentable une fois le coût du retrait payé

Une fois quelques jours de données accumulées, ce CSV peut être branché
directement sur train_arbitrage_model.py (déjà construit) pour entraîner
le filtre XGBoost.

CORRECTIF DU 02/08 — bug de timing corrigé :
Avant, les deux vérifications utilisaient `await verifier_apres(...)` l'une
après l'autre (0.5s puis 2s), donc "confirmee_5s" était en réalité mesurée
à 0.5+2 = 2.5s après détection, pas 5s comme son nom le laissait croire (et
comme filtre_ml.py / le texte des alertes Telegram le supposaient). Les deux
vérifications tournent maintenant en parallèle via asyncio.gather(), chacune
avec son propre délai isolé (2s et 5s réels depuis la détection).
⚠️ Les données déjà collectées AVANT ce correctif ont ce biais de timing —
voir _init_csv() qui archive automatiquement l'ancien fichier au lieu de
mélanger les deux schémas dans un même CSV.
"""

import asyncio
import csv
import logging
import os
import time
from dataclasses import asdict

import stockage

log = logging.getLogger("opportunity_logger")

CSV_PATH = stockage.chemin_donnees("opportunites_log.csv")

# Montant de référence pour le calcul des frais de retrait — identique à
# paper_trading.MONTANT_PAR_TRADE_USDT (50.0), défini localement ici pour ne
# pas créer de dépendance croisée entre les deux modules.
MONTANT_REFERENCE_USDT = 50.0

COLONNES = [
    "timestamp", "type_arbitrage", "exchanges", "symboles",
    "spread_brut_pct", "frais_total_pct", "spread_net_pct",
    "confirmee_2s", "confirmee_5s", "spread_net_pct_apres_5s",
    "spread_net_reel_pct_apres_5s", "rentable_apres_5s",
]


def _init_csv():
    """
    Crée le fichier avec les en-têtes s'il n'existe pas encore. Si un fichier
    existe déjà mais avec un ancien schéma de colonnes (ex: avant l'ajout des
    colonnes "frais de retrait inclus"), il est archivé plutôt que réutilisé
    tel quel — sinon les nouvelles lignes, écrites dans l'ordre de COLONNES,
    se retrouveraient décalées sous les en-têtes de l'ancien fichier.
    """
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, encoding="utf-8") as f:
            entete_existant = f.readline().strip().split(",")
        if entete_existant != COLONNES:
            horodatage = time.strftime("%Y%m%d_%H%M%S")
            chemin_archive = CSV_PATH.replace(".csv", f"_ancien_schema_{horodatage}.csv")
            os.rename(CSV_PATH, chemin_archive)
            log.warning(
                f"Schéma de colonnes changé (correctif timing ML) — "
                f"ancien fichier archivé sous {chemin_archive}"
            )

    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(COLONNES)


def _ecrire_ligne(ligne: dict):
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES)
        writer.writerow(ligne)


async def logger_avec_suivi(opp, detecter_inter_exchange_func, detecter_triangulaire_func, triangle=None):
    """
    Enregistre une opportunité immédiatement, puis revérifie 2s ET 5s plus
    tard — en parallèle, chacune avec son propre délai isolé depuis la
    détection — si elle tient toujours (paper trading léger) pour générer
    un label.

    detecter_inter_exchange_func / detecter_triangulaire_func : les fonctions
    de détection du Bloc 3, réutilisées pour revérifier plus tard sans dupliquer
    la logique de calcul.
    """
    _init_csv()

    ligne = {
        "timestamp": opp.timestamp,
        "type_arbitrage": opp.type_arbitrage,
        "exchanges": "|".join(opp.exchanges),
        "symboles": "|".join(opp.symboles),
        "spread_brut_pct": opp.spread_brut_pct,
        "frais_total_pct": opp.frais_total_pct,
        "spread_net_pct": opp.spread_net_pct,
        "confirmee_2s": "", "confirmee_5s": "", "spread_net_pct_apres_5s": "",
        "spread_net_reel_pct_apres_5s": "", "rentable_apres_5s": "",
    }

    async def verifier_apres(
        delai: float, cle_confirmee: str, cle_spread: str | None = None,
        cle_reel: str | None = None, cle_rentable: str | None = None,
    ):
        await asyncio.sleep(delai)
        try:
            if opp.type_arbitrage == "inter_exchange":
                nouvelles = detecter_inter_exchange_func(opp.symboles[0])
                match = next(
                    (n for n in nouvelles if set(n.exchanges) == set(opp.exchanges)), None
                )
                encore_la = match is not None
                if encore_la and cle_spread:
                    ligne[cle_spread] = match.spread_net_pct

                # Spread RÉELLEMENT rentable = frais de retrait inclus, pas
                # juste "toujours au-dessus du seuil de collecte (0.05%,
                # frais de trading seuls)". Sans ça, "confirmée" ne veut dire
                # que "encore un peu positif avant même de compter le coût
                # du transfert retour" — un bar beaucoup trop bas pour que
                # le modèle apprenne quelque chose d'utile sur la rentabilité.
                if encore_la and cle_reel:
                    import frais_retrait
                    ex_achat, ex_vente = match.exchanges
                    info_retrait = frais_retrait.frais_transfert(ex_vente, ex_achat, MONTANT_REFERENCE_USDT)
                    frais_retrait_pct = (info_retrait["frais"] / MONTANT_REFERENCE_USDT) * 100
                    spread_reel_pct = match.spread_net_pct - frais_retrait_pct
                    ligne[cle_reel] = round(spread_reel_pct, 4)
                    if cle_rentable:
                        ligne[cle_rentable] = "1" if spread_reel_pct > 0 else "0"
                elif cle_rentable:
                    ligne[cle_rentable] = "0"  # disparue = non rentable, sans ambiguïté

            else:  # triangulaire
                nouvelle = detecter_triangulaire_func(opp.exchanges[0], triangle)
                encore_la = nouvelle is not None
                if encore_la and cle_spread:
                    ligne[cle_spread] = nouvelle.spread_net_pct

            ligne[cle_confirmee] = "1" if encore_la else "0"
        except Exception as e:
            log.error(f"Erreur vérification suivi : {e}")
            ligne[cle_confirmee] = ""

    # Les deux vérifications tournent en parallèle, chacune avec son délai
    # isolé depuis la détection — avant, le second `await` ne démarrait
    # qu'APRÈS la fin du premier (deux `await` séquentiels), donc
    # "confirmee_5s" était en réalité mesurée à 0.5+2 = 2.5s, pas 5s.
    await asyncio.gather(
        verifier_apres(2, "confirmee_2s"),
        verifier_apres(5, "confirmee_5s", "spread_net_pct_apres_5s", "spread_net_reel_pct_apres_5s", "rentable_apres_5s"),
    )

    _ecrire_ligne(ligne)
    log.debug(
        f"Opportunité loggée avec suivi : {ligne['symboles']} "
        f"confirmee_5s={ligne['confirmee_5s']} rentable_apres_5s={ligne['rentable_apres_5s']}"
    )


def stats_rapides() -> str:
    """Résumé rapide du CSV accumulé — utile pour savoir si on a assez de données."""
    if not os.path.exists(CSV_PATH):
        return "Aucune donnée enregistrée pour l'instant."

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))

    if not lignes:
        return "Fichier vide."

    total = len(lignes)
    confirmees_5s = sum(1 for l in lignes if l.get("confirmee_5s") == "1")
    taux = (confirmees_5s / total * 100) if total else 0

    rentables_5s = sum(1 for l in lignes if l.get("rentable_apres_5s") == "1")
    taux_rentable = (rentables_5s / total * 100) if total else 0

    return (
        f"📁 {total} opportunités enregistrées\n"
        f"✅ {confirmees_5s} confirmées après 5s ({taux:.1f}%) — spread encore positif hors frais de retrait\n"
        f"💰 {rentables_5s} RÉELLEMENT rentables après 5s ({taux_rentable:.1f}%) — frais de retrait inclus\n"
        f"{'✅ Assez de données pour un premier entraînement (500+)' if total >= 500 else f'⏳ Encore {500 - total} avant un entraînement fiable (objectif : 500+)'}"
    )


def nettoyer_csv(symboles_a_retirer: set[str]) -> tuple[int, int]:
    """
    Retire du CSV toutes les lignes correspondant à des symboles blacklistés
    a posteriori (faux positifs identifiés par health_manager APRÈS avoir déjà
    été loggés). À lancer avant tout entraînement pour ne pas apprendre sur
    des données polluées par des bugs de flux ou des collisions de ticker.

    Retourne (nb_lignes_avant, nb_lignes_apres).
    """
    if not os.path.exists(CSV_PATH):
        return (0, 0)

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))

    avant = len(lignes)
    lignes_propres = [l for l in lignes if l.get("symboles", "").split("|")[0] not in symboles_a_retirer]
    apres = len(lignes_propres)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES)
        writer.writeheader()
        writer.writerows(lignes_propres)

    log.info(f"Nettoyage CSV : {avant} -> {apres} lignes ({avant - apres} retirées)")
    return (avant, apres)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "clean":
        import health_manager
        symboles_blacklistes = health_manager.symboles_blacklistes()
        avant, apres = nettoyer_csv(symboles_blacklistes)
        print(f"Nettoyage effectué : {avant} -> {apres} lignes ({avant - apres} lignes polluées retirées)")
    print(stats_rapides())

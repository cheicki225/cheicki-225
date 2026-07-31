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
- Revérifie automatiquement 2s et 5s plus tard si le spread était encore là
  (paper trading léger, sans exécution réelle) -> ça donne un label
  "confirmee" / "disparue" exploitable pour entraîner un modèle plus tard

Une fois quelques jours de données accumulées, ce CSV peut être branché
directement sur train_arbitrage_model.py (déjà construit) pour entraîner
le filtre XGBoost.
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

COLONNES = [
    "timestamp", "type_arbitrage", "exchanges", "symboles",
    "spread_brut_pct", "frais_total_pct", "spread_net_pct",
    "confirmee_2s", "confirmee_5s", "spread_net_pct_apres_5s",
]


def _init_csv():
    """Crée le fichier avec les en-têtes s'il n'existe pas encore."""
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
    Enregistre une opportunité immédiatement, puis revérifie 2s et 5s plus
    tard si elle tient toujours (paper trading léger) pour générer un label.

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
    }

    async def verifier_apres(delai: float, cle_confirmee: str, cle_spread: str | None = None):
        await asyncio.sleep(delai)
        try:
            if opp.type_arbitrage == "inter_exchange":
                nouvelles = detecter_inter_exchange_func(opp.symboles[0])
                # toujours là si une opportunité avec les mêmes exchanges existe encore
                encore_la = any(
                    set(n.exchanges) == set(opp.exchanges) for n in nouvelles
                )
            else:  # triangulaire
                nouvelle = detecter_triangulaire_func(opp.exchanges[0], triangle)
                encore_la = nouvelle is not None
                if encore_la and cle_spread:
                    ligne[cle_spread] = nouvelle.spread_net_pct

            ligne[cle_confirmee] = "1" if encore_la else "0"
        except Exception as e:
            log.error(f"Erreur vérification suivi : {e}")
            ligne[cle_confirmee] = ""

    await verifier_apres(0.5, "confirmee_2s")
    await verifier_apres(2, "confirmee_5s", "spread_net_pct_apres_5s")

    _ecrire_ligne(ligne)
    log.debug(f"Opportunité loggée avec suivi : {ligne['symboles']} confirmee_5s={ligne['confirmee_5s']}")


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

    return (
        f"📁 {total} opportunités enregistrées\n"
        f"✅ {confirmees_5s} confirmées après 5s ({taux:.1f}%)\n"
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

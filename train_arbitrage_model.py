"""
Entraînement du modèle ML de filtrage d'opportunités d'arbitrage
======================================================================
Objectif : apprendre à distinguer une opportunité EXPLOITABLE (spread encore
là 5s plus tard, donc le temps d'agir) d'un MIRAGE (spread qui disparaît
avant qu'on ait pu trader), à partir des données collectées par
opportunity_logger.py (opportunites_log.csv).

⚠️ FAIT vs ESTIMATION :
- [FAIT] Ce script entraîne un classifieur XGBoost binaire sur tes vraies
  données, avec split train/test et métriques calculées honnêtement.
- [ESTIMATION] La qualité du modèle dépend ENTIÈREMENT de la quantité et
  qualité des données. En dessous de quelques centaines de lignes propres,
  les métriques annoncées ont une marge d'erreur importante (peu de
  données de test = intervalle de confiance large). Ne considère PAS un
  bon score ici comme une garantie de rentabilité — c'est un filtre pour
  réduire les faux positifs du radar, pas un signal de trading autonome.
- Ceci n'est pas un conseil financier.

Utilisation :
    python3 train_arbitrage_model.py
    python3 train_arbitrage_model.py --csv opportunites_log.csv --sortie modele_arbitrage.json

Installation (déjà dans requirements.txt) :
    pip install pandas scikit-learn xgboost --break-system-packages
"""

import argparse
import json
import logging
import os
import sys
import time

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score, classification_report, confusion_matrix, roc_auc_score,
)
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("train_arbitrage_model")

# Doit rester cohérent avec opportunity_logger.COLONNES
COLONNE_CIBLE = "confirmee_5s"
SEUIL_LIGNES_RECOMMANDE = 500  # même seuil que opportunity_logger.stats_rapides()

# Colonnes numériques utilisées telles quelles comme features
FEATURES_NUMERIQUES_BRUTES = ["spread_brut_pct", "frais_total_pct", "spread_net_pct"]


# ============================================================
# CHARGEMENT & NETTOYAGE
# ============================================================
def charger_et_nettoyer(chemin_csv: str) -> pd.DataFrame:
    if not os.path.exists(chemin_csv):
        log.error(f"Fichier introuvable : {chemin_csv}")
        sys.exit(1)

    df = pd.read_csv(chemin_csv)
    n_brut = len(df)
    log.info(f"{n_brut} lignes chargées depuis {chemin_csv}")

    colonnes_requises = FEATURES_NUMERIQUES_BRUTES + [
        "type_arbitrage", "exchanges", "timestamp", COLONNE_CIBLE,
    ]
    manquantes = [c for c in colonnes_requises if c not in df.columns]
    if manquantes:
        log.error(f"Colonnes manquantes dans le CSV : {manquantes}")
        log.error("Vérifie que ce CSV vient bien de opportunity_logger.py (colonnes attendues différentes).")
        sys.exit(1)

    # La cible peut être vide (chaîne "") si la revérification a échoué (erreur
    # réseau au moment du suivi) — ces lignes n'ont pas de label utilisable
    df[COLONNE_CIBLE] = pd.to_numeric(df[COLONNE_CIBLE], errors="coerce")
    avant_dropna = len(df)
    df = df.dropna(subset=[COLONNE_CIBLE] + FEATURES_NUMERIQUES_BRUTES)
    n_sans_label = avant_dropna - len(df)
    if n_sans_label:
        log.info(f"{n_sans_label} ligne(s) sans label exploitable retirée(s) (suivi 5s manquant/échoué)")

    if len(df) == 0:
        log.error("Aucune ligne exploitable après nettoyage — impossible d'entraîner.")
        sys.exit(1)

    return df.reset_index(drop=True)


# ============================================================
# FEATURE ENGINEERING
# ============================================================
def construire_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    features = pd.DataFrame(index=df.index)

    for col in FEATURES_NUMERIQUES_BRUTES:
        features[col] = pd.to_numeric(df[col], errors="coerce")

    # Nombre d'exchanges impliqués dans l'opportunité (2 pour inter-exchange,
    # 3 pour triangulaire en général, mais on le calcule plutôt que de le supposer)
    features["nb_exchanges"] = df["exchanges"].fillna("").apply(
        lambda s: len([x for x in s.split("|") if x])
    )

    features["est_triangulaire"] = (df["type_arbitrage"] == "triangulaire").astype(int)

    # Heure de la journée (UTC) — hypothèse : la liquidité/volatilité varie
    # selon les sessions Asie/Europe/US, donc ça peut aider le modèle à
    # distinguer les créneaux où les spreads tiennent plus longtemps.
    # [HYPOTHÈSE] pas garanti utile avec peu de données, mais coûte rien à inclure.
    features["heure_utc"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce").dt.hour.fillna(-1)

    cible = df[COLONNE_CIBLE].astype(int)

    # Retire les lignes où le feature engineering a introduit des NaN
    # (ex: spread_brut_pct corrompu dans le CSV)
    valides = features.notna().all(axis=1)
    if (~valides).sum():
        log.info(f"{(~valides).sum()} ligne(s) retirée(s) (valeur de feature invalide après conversion)")

    return features[valides].reset_index(drop=True), cible[valides].reset_index(drop=True)


# ============================================================
# ENTRAÎNEMENT
# ============================================================
def entrainer(features: pd.DataFrame, cible: pd.Series, test_size: float = 0.2, graine: int = 42):
    stratifier = cible if cible.nunique() > 1 else None
    if stratifier is None:
        log.warning(
            "⚠️ Toutes les lignes ont le même label (que des 0 ou que des 1) — "
            "impossible d'entraîner un classifieur utile. Attends d'avoir des exemples des deux classes."
        )
        sys.exit(1)

    X_train, X_test, y_train, y_test = train_test_split(
        features, cible, test_size=test_size, random_state=graine, stratify=stratifier,
    )

    # scale_pos_weight compense un déséquilibre entre opportunités confirmées
    # vs disparues (souvent déséquilibré : beaucoup plus de mirages que de
    # vraies opportunités qui tiennent 5s)
    n_neg = (y_train == 0).sum()
    n_pos = (y_train == 1).sum()
    scale_pos_weight = (n_neg / n_pos) if n_pos > 0 else 1.0

    modele = XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        scale_pos_weight=scale_pos_weight,
        eval_metric="logloss",
        random_state=graine,
    )
    modele.fit(X_train, y_train)

    return modele, X_train, X_test, y_train, y_test


def evaluer(modele, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    y_pred = modele.predict(X_test)
    y_proba = modele.predict_proba(X_test)[:, 1]

    accuracy = accuracy_score(y_test, y_pred)
    matrice = confusion_matrix(y_test, y_pred).tolist()
    rapport = classification_report(y_test, y_pred, output_dict=True, zero_division=0)

    try:
        auc = roc_auc_score(y_test, y_proba) if y_test.nunique() > 1 else None
    except ValueError:
        auc = None

    importances = dict(zip(X_test.columns, modele.feature_importances_.round(4).tolist()))
    importances = dict(sorted(importances.items(), key=lambda x: -x[1]))

    return {
        "accuracy": round(float(accuracy), 4),
        "roc_auc": round(float(auc), 4) if auc is not None else None,
        "matrice_confusion": matrice,
        "rapport_classification": rapport,
        "importance_features": importances,
        "n_test": len(y_test),
    }


def afficher_resultats(resultats: dict, n_total: int, n_train: int, n_test: int):
    print("\n" + "=" * 60)
    print("📊 RÉSULTATS DE L'ENTRAÎNEMENT")
    print("=" * 60)
    print(f"Lignes totales utilisées : {n_total} (train: {n_train} / test: {n_test})")

    if n_total < SEUIL_LIGNES_RECOMMANDE:
        print(
            f"⚠️  [RISQUE] {n_total} < {SEUIL_LIGNES_RECOMMANDE} lignes recommandées — "
            f"les métriques ci-dessous ont une marge d'erreur ÉLEVÉE. "
            f"Traite-les comme indicatives, pas définitives."
        )

    print(f"\n[FAIT] Accuracy (test) : {resultats['accuracy']:.1%}")
    if resultats["roc_auc"] is not None:
        print(f"[FAIT] ROC-AUC (test)  : {resultats['roc_auc']:.3f}  (0.5 = aléatoire, 1.0 = parfait)")

    print(f"\nMatrice de confusion (test, {resultats['n_test']} exemples) :")
    print("                 Prédit: disparu   Prédit: confirmé")
    tn, fp = resultats["matrice_confusion"][0]
    fn, tp = resultats["matrice_confusion"][1]
    print(f"  Réel: disparu       {tn:>6}            {fp:>6}")
    print(f"  Réel: confirmé      {fn:>6}            {tp:>6}")

    rc = resultats["rapport_classification"]
    if "1" in rc:
        print(
            f"\n[FAIT] Sur les opportunités RÉELLEMENT confirmées après 5s : "
            f"le modèle en détecte {rc['1']['recall']:.0%} (rappel), "
            f"et parmi ce qu'il annonce 'confirmé', {rc['1']['precision']:.0%} le sont vraiment (précision)."
        )

    print("\nImportance des features (ce que le modèle utilise le plus) :")
    for nom, valeur in resultats["importance_features"].items():
        barre = "█" * int(valeur * 40)
        print(f"  {nom:<18} {valeur:.3f}  {barre}")
    print("=" * 60 + "\n")


# ============================================================
# SAUVEGARDE
# ============================================================
def sauvegarder(modele, resultats: dict, features: pd.DataFrame, chemin_sortie: str, chemin_csv: str):
    modele.save_model(chemin_sortie)

    chemin_meta = chemin_sortie.rsplit(".", 1)[0] + "_meta.json"
    meta = {
        "entraine_le": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "source_csv": chemin_csv,
        "colonnes_features_ordre": list(features.columns),
        "accuracy_test": resultats["accuracy"],
        "roc_auc_test": resultats["roc_auc"],
        "n_lignes_entrainement": resultats["n_test"],  # nombre exact dispo directement pour test ; total loggé séparément dans les logs console
    }
    with open(chemin_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    log.info(f"✅ Modèle sauvegardé : {chemin_sortie}")
    log.info(f"✅ Métadonnées sauvegardées : {chemin_meta}")
    log.info(
        "ℹ️  Ce modèle n'est PAS encore branché sur le bot en live — c'est une étape séparée "
        "(charger modele_arbitrage.json dans bot_fusionne_v1.py pour filtrer les alertes)."
    )


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Entraîne le modèle de filtrage d'opportunités d'arbitrage.")
    parser.add_argument("--csv", default="opportunites_log.csv", help="Chemin du CSV source")
    parser.add_argument("--sortie", default="modele_arbitrage.json", help="Chemin du modèle entraîné")
    parser.add_argument("--test-size", type=float, default=0.2, help="Fraction réservée au test (0.2 = 20%%)")
    args = parser.parse_args()

    df = charger_et_nettoyer(args.csv)

    if len(df) < SEUIL_LIGNES_RECOMMANDE:
        log.warning(
            f"⚠️ {len(df)} lignes propres seulement (recommandé : {SEUIL_LIGNES_RECOMMANDE}+). "
            f"On entraîne quand même, mais considère les résultats comme préliminaires."
        )

    features, cible = construire_features(df)
    log.info(f"Répartition des labels : {(cible == 1).sum()} confirmées / {(cible == 0).sum()} disparues")

    modele, X_train, X_test, y_train, y_test = entrainer(features, cible, test_size=args.test_size)
    resultats = evaluer(modele, X_test, y_test)
    afficher_resultats(resultats, n_total=len(features), n_train=len(X_train), n_test=len(X_test))
    sauvegarder(modele, resultats, features, args.sortie, args.csv)


if __name__ == "__main__":
    main()

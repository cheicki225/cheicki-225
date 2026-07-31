"""
Filtre ML — scoring des opportunités par le modèle entraîné
=================================================================
Charge le modèle produit par train_arbitrage_model.py et calcule, pour
chaque opportunité détectée, la probabilité qu'elle soit encore valide
5 secondes après détection (même signal que la colonne confirmee_5s du
CSV d'entraînement).

⚠️ Conçu pour ÉCHOUER EN DOUCEUR : si le modèle n'est pas encore entraîné,
si le fichier est absent (ex: pas encore poussé sur Railway), ou si
xgboost n'est pas installé, le scoring est simplement désactivé — le bot
continue de fonctionner normalement, aucune alerte n'est jamais bloquée
par erreur à cause de ce module.

Le score n'est qu'INFORMATIF tant que FILTRAGE_ML_ACTIF=False dans
config.py (comportement par défaut). Voir config.py pour l'activer.
"""

import json
import logging
import os

from config import CHEMIN_MODELE_ML

log = logging.getLogger("filtre_ml")

_modele = None
_colonnes_features = None
_pandas = None

# Ordre de repli si le fichier _meta.json est absent — DOIT rester identique
# à construire_features() dans train_arbitrage_model.py
_COLONNES_PAR_DEFAUT = [
    "spread_brut_pct", "frais_total_pct", "spread_net_pct",
    "nb_exchanges", "est_triangulaire", "heure_utc",
]


def _charger_modele():
    global _modele, _colonnes_features, _pandas

    if not os.path.exists(CHEMIN_MODELE_ML):
        log.info(f"ℹ️ Modèle ML absent ({CHEMIN_MODELE_ML}) — scoring désactivé, alertes normales (aucun impact)")
        return

    try:
        import pandas as pd
        from xgboost import XGBClassifier
    except ImportError as e:
        log.warning(f"⚠️ Dépendance ML manquante ({e}) — scoring désactivé. Vérifie requirements.txt (xgboost, pandas).")
        return

    try:
        modele = XGBClassifier()
        modele.load_model(CHEMIN_MODELE_ML)

        chemin_meta = CHEMIN_MODELE_ML.rsplit(".", 1)[0] + "_meta.json"
        if os.path.exists(chemin_meta):
            with open(chemin_meta, encoding="utf-8") as f:
                colonnes = json.load(f).get("colonnes_features_ordre") or _COLONNES_PAR_DEFAUT
        else:
            log.warning(f"⚠️ {chemin_meta} introuvable — utilisation de l'ordre de colonnes par défaut")
            colonnes = _COLONNES_PAR_DEFAUT

        _modele = modele
        _colonnes_features = colonnes
        _pandas = pd
        log.info(f"✅ Modèle ML chargé ({CHEMIN_MODELE_ML}) — scoring des opportunités activé")
    except Exception as e:
        log.warning(f"⚠️ Échec chargement du modèle ML ({e}) — scoring désactivé, alertes normales (aucun impact)")
        _modele = None


def modele_disponible() -> bool:
    return _modele is not None


def score_opportunite(opp) -> float | None:
    """
    Retourne la probabilité (0-1) que l'opportunité soit encore valide 5s
    après détection, selon le modèle ML. Retourne None si le modèle n'est
    pas chargé, ou en cas d'erreur de scoring (échec silencieux — ne doit
    JAMAIS faire planter le traitement d'une opportunité).
    """
    if _modele is None:
        return None

    try:
        import time as _time
        heure_utc = _time.gmtime(opp.timestamp).tm_hour
        valeurs = {
            "spread_brut_pct": opp.spread_brut_pct,
            "frais_total_pct": opp.frais_total_pct,
            "spread_net_pct": opp.spread_net_pct,
            "nb_exchanges": len(opp.exchanges),
            "est_triangulaire": 1 if opp.type_arbitrage == "triangulaire" else 0,
            "heure_utc": heure_utc,
        }
        ligne = _pandas.DataFrame([[valeurs[c] for c in _colonnes_features]], columns=_colonnes_features)
        proba = _modele.predict_proba(ligne)[0][1]
        return round(float(proba), 3)
    except Exception as e:
        log.warning(f"⚠️ Erreur scoring ML pour {getattr(opp, 'symboles', '?')} (ignorée) : {e}")
        return None


_charger_modele()

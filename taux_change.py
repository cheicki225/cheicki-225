"""
Taux de change EUR/USDT en temps réel
========================================
Bitvavo cote tout en EUR (BTC-EUR, ETH-EUR...), alors que tout le reste du
bot compare des prix en USDT. Pour rendre les prix Bitvavo comparables à
ceux de Binance, Bybit, OKX, KuCoin, Bitget, Gate.io et CoinEx, il faut
d'abord les convertir.

D'où vient le taux : le prix du marché EURUSDT lui-même, déjà suivi par
6 des 7 autres plateformes (c'est une paire courante). On réutilise
directement le cache WebSocket du bot — aucun appel réseau supplémentaire,
aucune dépendance externe.

⚠️ RISQUE STRUCTUREL À CONNAÎTRE
Le taux EUR/USDT bouge, lui aussi, en permanence — pas autant qu'un altcoin,
mais pas nul. Comparer un prix Bitvavo (converti à l'instant T) à un prix
Binance (observé à l'instant T) ajoute donc une source d'erreur qui
n'existe pas quand toutes les plateformes cotent déjà dans la même devise.
Un écart de change de 0.05% peut suffire à transformer un vrai arbitrage en
faux positif, ou l'inverse. C'est le compromis assumé en ajoutant Bitvavo.
"""

import logging
import time

log = logging.getLogger("taux_change")

# Symbole EURUSDT lui-même : (pas d'import circulaire, prix_live est
# injecté par le module appelant à chaque lecture)
SYMBOLE_EURUSDT = "EURUSDT"

# Un prix EUR/USDT plus vieux que ça n'est plus fiable pour convertir —
# mieux vaut ignorer l'opportunité que convertir avec un taux périmé.
AGE_MAX_SEC = 15.0

# Repli si aucune plateforme ne fournit EURUSDT à cet instant (rare, mais
# ne doit jamais faire planter la détection). Actualisé manuellement de
# temps en temps — ce n'est qu'un filet de sécurité, pas une source fiable.
TAUX_REPLI = 1.08


def taux_eur_vers_usdt(prix_live: dict) -> tuple[float, bool]:
    """
    Retourne (taux, est_fiable). 1 EUR = `taux` USDT.

    est_fiable=False si on retombe sur le repli — le code appelant doit
    alors traiter l'opportunité avec plus de prudence, ou l'ignorer plutôt
    que de convertir avec un chiffre qui n'est plus une vraie mesure.
    """
    maintenant = time.time()
    meilleur = None

    for exchange, symboles in prix_live.items():
        if exchange == "bitvavo":
            continue  # Bitvavo ne fournit pas ce taux, ce serait circulaire
        donnees = symboles.get(SYMBOLE_EURUSDT)
        if not donnees:
            continue
        age = maintenant - donnees.get("timestamp", 0)
        if age > AGE_MAX_SEC:
            continue
        bid, ask = donnees.get("bid", 0), donnees.get("ask", 0)
        if bid <= 0 or ask <= 0:
            continue
        milieu = (bid + ask) / 2
        # Le plus récent gagne, entre les plateformes qui le fournissent
        if meilleur is None or age < meilleur[1]:
            meilleur = (milieu, age)

    if meilleur is None:
        return TAUX_REPLI, False
    return meilleur[0], True


def eur_vers_usdt(montant_eur: float, prix_live: dict) -> tuple[float, bool]:
    """Convertit un montant EUR en USDT. Retourne (montant, est_fiable)."""
    taux, fiable = taux_eur_vers_usdt(prix_live)
    return montant_eur * taux, fiable

"""
Configuration centralisée du bot d'arbitrage
=================================================
Tous les seuils critiques ici, en un seul endroit — plus besoin de chercher
dans 5 fichiers différents pour ajuster le comportement du bot.

⚠️ Si tu modifies un seuil ici, redémarre le bot pour que ça prenne effet
(certains sont lus une seule fois au démarrage).
"""

# ============================================================
# SEUILS D'ARBITRAGE (spread net, après frais)
# ============================================================
SEUIL_MIN_INTER_EXCHANGE_PCT = 2.3   # % net minimum pour déclencher une alerte Telegram (monté de 1.5% à 2.3% le 02/08 — 1.5% était sous le point mort réel une fois frais de retrait inclus)
SEUIL_MIN_TRIANGULAIRE_PCT = 1.5     # idem pour le triangulaire (était 0.4%)

# Seuil bas séparé pour la COLLECTE DE DONNÉES ML (pas d'alerte, juste logging)
SEUIL_MIN_COLLECTE_ML_PCT = 0.05


# ============================================================
# DÉTECTION DES FAUX POSITIFS / DONNÉES SUSPECTES
# ============================================================
# Écart brut au-delà duquel c'est presque toujours une collision de ticker
# ou un bug, jamais un vrai écart d'arbitrage — blacklisté immédiatement
SEUIL_ECART_ABSURDE_PCT = 20.0

# Combien de temps un même écart doit persister SANS jamais se refermer
# avant d'être considéré suspect (un vrai écart se referme en quelques secondes)
SEUIL_PERSISTANCE_SUSPECTE_SEC = 20

# Tolérance de courte absence avant de considérer qu'une opportunité s'est
# VRAIMENT refermée (évite qu'un prix pas encore rafraîchi ne réinitialise
# le compteur de persistance à tort)
GRACE_PERIODE_SEC = 5

# Pas de prix reçu depuis ce délai -> paire considérée en panne
# 60s était trop strict avec 300+ paires : certains altcoins peu actifs
# ne reçoivent naturellement aucune mise à jour pendant 1-2 minutes, sans
# que ce soit une vraie panne de connexion. 150s laisse plus de marge.
SEUIL_PANNE_SEC = 150

# Une paire blacklistée est réexaminée après ce délai — évite qu'un faux
# positif n'exclue une paire définitivement (et de toute façon, la
# blacklist entière repart à zéro à chaque redémarrage du bot)
TTL_BLACKLIST_SEC = 1 * 3600  # 1 heure

# Tickers courts/génériques à très haut risque de désigner une crypto
# DIFFÉRENTE selon l'exchange — exclus par précaution dès la découverte.
# DÉSACTIVÉ le 31/07 sur demande (on laisse tout passer). Liste d'origine
# gardée en commentaire pour pouvoir réactiver facilement :
# TICKERS_A_RISQUE = {
#     "U", "A", "S", "T", "C", "H", "G", "W", "F",
#     "AI", "ONE", "IO", "OG", "ID",
# }
TICKERS_A_RISQUE = set()


# ============================================================
# ALERTE DE SANTÉ GLOBALE
# ============================================================
# Si le nombre total de paires actives chute de plus de ce pourcentage par
# rapport au maximum observé dans la session, alerte Telegram immédiate —
# signe probable d'un bug (ex: blacklist qui explose, exchange down, etc.)
SEUIL_CHUTE_PAIRES_ALERTE_PCT = 40  # ex: 40 = alerte si on perd 40%+ des paires
COOLDOWN_ALERTE_CHUTE_SEC = 600     # ne renvoie pas l'alerte plus d'1x/10min


# ============================================================
# INFRASTRUCTURE / CONNEXIONS
# ============================================================
NB_CONNEXIONS_PAR_EXCHANGE = 4   # découpage des paires en plusieurs connexions WS
MIN_EXCHANGES = 3                # une paire doit exister sur au moins N exchanges pour être utilisée

# Volume minimum (24h, en $) sur un exchange pour qu'une paire y soit
# considérée comme "liquide" — élimine à la source les tokens listés
# partout mais tradés nulle part (carnet trop fin, faux signaux garantis).
# DÉSACTIVÉ le 31/07 sur demande (0 = aucun filtre, tout passe). Était 350_000.
# ⚠️ Remettre une valeur >0 réactive la protection contre les carnets trop fins.
VOLUME_MIN_USDT = 0

FRAIS_TRADING_PCT = {
    "binance": 0.10, "bybit": 0.10, "okx": 0.10, "kucoin": 0.10, "bitget": 0.10,
    "gateio": 0.20,  # confirmé : 2x plus cher que les autres au tarif de base (sans token GT/VIP)
}


# ============================================================
# RÉSEAUX POUR REEQUILIBRAGE (transferts entre exchanges)
# ============================================================
RESEAU_PREFERE = "SOL"
RESEAU_FALLBACK = "TRC20"


# ============================================================
# LIMITE GLOBALE D'ALERTES
# ============================================================
# Maximum de cryptos DIFFÉRENTES alertées par minute glissante — évite
# qu'une seule paire instable (ex: un altcoin peu liquide qui oscille)
# ne monopolise le flux d'alertes Telegram.
#
# ⚠️ CALCUL DE CHARGE TELEGRAM — à recalculer à CHAQUE nouveau type de message.
# Telegram bloque au-delà d'environ 20 messages/minute vers un même chat
# (blocage de 2h45 constaté le 01/08 avec l'ancienne valeur de 30).
#
# Messages envoyés par opportunité alertée :
#   1. l'alerte d'opportunité elle-même   (bot_fusionne_v1 -> envoyer_alerte)
#   2. le résultat du trade papier         (paper_trading.simuler_trade)
#   3. le résumé du suivi 10s              (suivi_opportunite) [ajouté le 02/08]
#
# 6 alertes/minute x 3 messages = 18 messages/minute, sous la limite de 20.
# (Avec l'ancienne valeur de 8 : 8 x 3 = 24/minute, AU-DESSUS de la limite —
#  d'où la baisse à 6 en même temps que l'ajout du suivi 10s.)
# En pratique c'est encore moins, car le résumé de suivi n'est envoyé que
# s'il est informatif (voir SUIVI_ENVOYER_SEULEMENT_SI_POSITIF ci-dessous).
MAX_ALERTES_PAR_MINUTE = 6

# Cooldown minimum entre deux alertes/trades sur la MÊME crypto, peu importe
# la combinaison d'exchanges — évite qu'une crypto volatile ne déclenche
# des dizaines d'alertes/trades papier en quelques secondes, et qu'elle ne
# re-déclenche en boucle à chaque petit mouvement de prix.
# (Était défini DEUX FOIS dans ce fichier avec deux commentaires différents —
#  sans effet tant que les valeurs étaient identiques, mais piège garanti le
#  jour où on en modifie une seule. Fusionné le 02/08.)
COOLDOWN_PAR_CRYPTO_SEC = 15


# ============================================================
# SUIVI DE PERSISTANCE 10s (suivi_opportunite.py)
# ============================================================
# Après chaque alerte, le bot relit le prix chaque seconde pendant 10s dans
# le cache WebSocket (aucun appel réseau) et recalcule le spread NET RÉEL,
# frais de retrait inclus — ce que le seuil d'alerte ne fait PAS.
# C'est la mesure directe de "est-ce que ce spread tient assez longtemps",
# là où le score ML n'est qu'une probabilité apprise.
SUIVI_ACTIF = True

# Durée du suivi et intervalle entre deux lectures (secondes)
SUIVI_DUREE_SEC = 10
SUIVI_INTERVALLE_SEC = 1.0

# Un prix plus vieux que ça n'est plus "observé maintenant" — c'est un flux
# WebSocket muet ou coupé. Affiché « ? » plutôt que réutilisé comme s'il
# était frais (un prix figé donnerait un faux spread stable).
SUIVI_AGE_MAX_PRIX_SEC = 5.0

# True  = n'envoie le résumé Telegram QUE si le spread réel a été positif au
#         moins une seconde pendant la fenêtre. Les cas « négatif du début à
#         la fin » sont toujours écrits dans le CSV, mais pas notifiés —
#         ils sont majoritaires et tous identiques, donc ils saturent
#         Telegram sans rien t'apprendre de nouveau.
# False = envoie tout (utile quelques heures pour observer, puis à remettre
#         à True — attention au quota Telegram calculé plus haut).
SUIVI_ENVOYER_SEULEMENT_SI_POSITIF = True


# ============================================================
# GESTION DU RISQUE (mode papier)
# ============================================================
# Circuit breaker global : pause automatique du bot si trop de pertes
# consécutives sur TOUTES les cryptos confondues (signal qu'un problème
# plus large existe — bug, marché anormal — pas juste une crypto isolée)
# Interrupteur général — mets à False pour désactiver complètement le
# circuit breaker (utile en phase de test/observation, à réactiver une
# fois que tu veux une vraie protection automatique)
CIRCUIT_BREAKER_ACTIVE = False

CIRCUIT_BREAKER_PERTES_CONSECUTIVES = 10

# Stop-loss journalier : si le profit papier cumulé de la journée descend
# sous ce seuil (négatif), les alertes/trades papier sont coupés jusqu'au
# lendemain (reset automatique à minuit)
STOP_LOSS_JOURNALIER_USDT = -20.0

# ⚠️ OBSOLÈTE (02/08) — la "double vérification" n'existe plus dans le code.
# simuler_trade() ne fait plus qu'UN seul contrôle de profondeur ("Contrôle
# instantané unique"), et la variable `double_verif_ok` vaut désormais
# simplement `profit_net_usdt > 0`, c'est-à-dire "le trade est rentable" —
# pas "deux contrôles concordent".
# Cette constante n'était plus lue par aucun fichier (elle était seulement
# importée dans paper_trading.py sans jamais servir). Gardée ici, à zéro,
# uniquement pour ne pas casser un éventuel import oublié ailleurs.
# Les noms `double_verification_ok` (colonne CSV) et "Rejetés (double vérif)"
# (dashboard) sont conservés VOLONTAIREMENT : les renommer casserait
# index.html (ligne ~1140) et rendrait illisible tout l'historique CSV déjà
# accumulé. Retiens simplement que ça veut dire "trade rentable".
DOUBLE_VERIFICATION_DELAI_SEC = 0.0

# ============================================================
# FILTRE ML (modèle entraîné sur opportunites_log.csv)
# ============================================================
# Chemin du modèle entraîné par train_arbitrage_model.py. Si absent (pas
# encore entraîné) ou si xgboost n'est pas installé, le scoring est
# simplement désactivé — le bot continue de fonctionner normalement,
# aucune alerte n'est bloquée par erreur.
CHEMIN_MODELE_ML = "modele_arbitrage.json"

# False = le score ML est calculé et affiché (Telegram + dashboard) mais
# NE BLOQUE AUCUNE alerte — comportement par défaut, le plus prudent tant
# que tu n'as pas toi-même jugé le modèle assez fiable sur la durée.
# True = les opportunités sous SEUIL_ML_CONFIANCE_MIN ne sont PLUS envoyées
# en alerte Telegram ni tradées en mode papier (mais restent visibles dans
# le dashboard/log — rien n'est caché, juste pas alerté).
FILTRAGE_ML_ACTIF = False

# Score minimum (probabilité 0-1 que l'opportunité soit encore valide 5s
# après détection) pour passer le filtre si FILTRAGE_ML_ACTIF=True.
SEUIL_ML_CONFIANCE_MIN = 0.5


# ============================================================
# TRANSFERT DE FONDS ENTRE EXCHANGES (mode papier uniquement pour l'instant)
# ============================================================
# Capital fictif de départ par exchange (mode papier)
CAPITAL_PAR_EXCHANGE_PAPIER = 200.0

# Si le solde d'un exchange descend sous ce % du solde moyen des autres,
# un rééquilibrage simulé se déclenche automatiquement
SEUIL_REEQUILIBRAGE_PCT = 50.0

# Frais de transfert simulé (réseau SOL ~0.01$, TRC20 ~1$ — on prend une
# estimation prudente pour ne pas sous-estimer le coût réel)
FRAIS_TRANSFERT_SIMULE_USDT = 0.5


# ============================================================
# STOCKS DE TOKENS (contrainte de capital réelle)
# ============================================================
# Pour VENDRE un token sur une plateforme, il faut l'y détenir à l'avance.
# Un transfert d'USDT ne crée pas ce stock. Avec un capital limité, on ne
# peut donc pré-positionner qu'un nombre restreint de tokens — c'est la
# contrainte qui rend la majorité des opportunités détectées inexploitables
# en pratique, et que la simulation ignorait jusqu'ici.
#
# Mets SUIVI_STOCKS_ACTIF à False pour revenir à l'ancien comportement
# (aucune contrainte de stock, chiffres plus flatteurs mais irréalistes).
SUIVI_STOCKS_ACTIF = True

# Nombre de tokens DIFFÉRENTS que tu peux tenir en stock simultanément.
# Avec 1200$ répartis sur 6 plateformes, en immobilisant 50$ par token,
# une dizaine de positions est déjà un maximum réaliste.
MAX_TOKENS_EN_STOCK = 10

# Valeur immobilisée par token, sur la plateforme où tu comptes vendre
VALEUR_STOCK_PAR_TOKEN_USDT = 50.0

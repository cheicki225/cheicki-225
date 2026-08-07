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
SEUIL_MIN_INTER_EXCHANGE_PCT = 3.5
# ⚠️ Ce seuil ne décide PLUS des alertes tant que SEUIL_BENEFICE_REEL_ACTIF
# vaut True (voir plus bas) — c'est SEUIL_BENEFICE_REEL_PCT qui gouverne.
# Il ne sert plus que dans trois cas :
#   1. REPLI si le calcul du bénéfice réel échoue (exception inattendue)
#   2. seuil d'affichage du panneau « Cryptos suivies » de la webapp
#   3. valeur initiale du seuil modifiable depuis le menu Telegram
#
# Réglé à 3.5% (et non 2.3%) car c'est le point mort réel sur un trade de
# 50$ avec des frais de retrait estimés à 1$ : 1.5% d'objectif + 2% de
# frais de retrait. En cas de repli, mieux vaut rater des opportunités que
# d'alerter sur des trades qu'on sait perdants.
# Historique : 0.5% -> 1.5% (31/07) -> 2.3% (02/08) -> 3.5% (02/08, repli)
SEUIL_MIN_TRIANGULAIRE_PCT = 1.5     # idem pour le triangulaire (était 0.4%)

# Seuil bas séparé pour la COLLECTE DE DONNÉES ML (pas d'alerte, juste logging)
SEUIL_MIN_COLLECTE_ML_PCT = 0.05


# ============================================================
# SEUIL SUR LE BÉNÉFICE RÉEL (frais de retrait inclus)
# ============================================================
# PROBLÈME que ça résout :
# SEUIL_MIN_INTER_EXCHANGE_PCT porte sur le spread NET au sens "après frais
# de TRADING seulement". Les frais de RETRAIT (nécessaires pour rapatrier
# les fonds et boucler l'arbitrage) n'y sont pas comptés. Résultat : une
# alerte à 1.55% pouvait correspondre à une perte réelle de -0.45%.
#
# Ces frais sont FIXES en dollars, donc leur poids en % dépend entièrement du
# montant tradé — 1$ de retrait vaut 2% sur un trade de 50$, mais 0.2% sur
# 500$. Un seuil unique en % ne peut donc PAS être correct pour toutes les
# paires d'exchanges à la fois : trop haut pour celles à frais faibles, trop
# bas pour celles à frais élevés.
#
# SOLUTION : quand SEUIL_BENEFICE_REEL_ACTIF = True, le bot calcule à la
# détection le bénéfice RÉELLEMENT attendu (frais de trading + frais de
# retrait réels de cette paire précise) et le compare directement à
# SEUIL_BENEFICE_REEL_PCT. Tu règles ton objectif de bénéfice, le bot fait
# l'arithmétique exacte pour chaque paire.
SEUIL_BENEFICE_REEL_ACTIF = True

# Bénéfice minimum RÉELLEMENT attendu, en %, pour déclencher une alerte.
# ⚠️ Ce n'est PAS un bénéfice garanti : le slippage, le second transfert
# (retour du token) et le délai réel d'exécution ne sont toujours PAS
# modélisés. C'est le meilleur cas, pas le cas probable.
SEUIL_BENEFICE_REEL_PCT = 1.5

# Montant de référence par trade, en USDT. Sert au mode papier ET au calcul
# du bénéfice réel ci-dessus (les frais de retrait étant fixes en dollars,
# ce montant change complètement le résultat).
#
# ⚠️ LEVIER LE PLUS EFFICACE du bot : augmenter ce montant dilue les frais
# fixes bien plus efficacement que monter le seuil. Pour viser +1.5% réel :
#     50$/trade  -> il faut un spread brut-frais_trading d'environ 3.5%
#    200$/trade  -> environ 2.0% suffit
#    500$/trade  -> environ 1.7% suffit
# (avec des frais de retrait estimés à 1$ ; c'est moins avec les vrais frais
#  KuCoin/Bitget/Gate.io, qui sont les seuls réellement connus.)
MONTANT_PAR_TRADE_USDT = 50.0


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
    "coinex": 0.20,  # ajouté le 04/08 — tarif de base sans token CET ni palier VIP
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
#   4. la clôture d'une position en attente (positions_attente) [ajouté le 02/08]
#   5. les alertes d'arbitrage perpétuel      (arbitrage_perpetuel) [03/08]
#      — débit très faible : cooldown de 30 min par couple, donc quelques
#        messages par heure au plus, mais il faut leur laisser de la place.
#
# 4 alertes/minute x 4 messages = 16 messages/minute, ce qui laisse ~4
# messages/minute de marge pour les alertes perpétuelles et les messages
# système (démarrage, résumés, circuit breaker).
# (8 x 3 = 24 et 6 x 4 = 24 étaient tous deux AU-DESSUS de la limite — d'où
#  les baisses successives 8 -> 6 -> 5 à chaque nouveau type de message.)
# En pratique les clôtures de positions sont bornées par MAX_POSITIONS_EN_ATTENTE
# (3), donc la charge réelle reste bien en dessous de ce pire cas théorique.
# En pratique c'est encore moins, car le résumé de suivi n'est envoyé que
# s'il est informatif (voir SUIVI_ENVOYER_SEULEMENT_SI_POSITIF ci-dessous).
MAX_ALERTES_PAR_MINUTE = 4

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
SUIVI_STOCKS_ACTIF = False
# Désactivé le 02/08 à la demande : le bot s'arrêtait de trader au bout de
# quelques minutes, une fois les 10 emplacements pris (ou, plus tôt encore,
# le capital d'une plateforme épuisé — 200$ ne financent que 4 positions
# de 50$, et les alertes se concentrent sur kucoin/gateio).
#
# ⚠️ CE QUE ÇA CHANGE DANS L'INTERPRÉTATION DES CHIFFRES
# Le bot simule désormais des ventes de tokens qu'il ne détient pas. Dans la
# réalité c'est impossible : vendre sur kucoin suppose d'y avoir déjà le
# token. Les résultats deviennent donc PLUS FLATTEURS et MOINS RÉALISTES —
# ils décrivent un monde sans contrainte de capital ni de pré-positionnement.
# Le Profit Factor, déjà à ~88 (contre 2 à 3 pour une vraie bonne stratégie),
# va encore monter. Ce n'est pas une amélioration de performance : c'est la
# disparition d'une contrainte qui existe bel et bien.
#
# En contrepartie — et c'est le but ici — le bot ne s'arrête plus, donc la
# collecte de données pour le ML et le suivi des spreads tourne en continu.
#
# Remets à True pour retrouver une simulation contrainte et réaliste.

# Nombre de tokens DIFFÉRENTS que tu peux tenir en stock simultanément.
# Avec 1200$ répartis sur 6 plateformes, en immobilisant 50$ par token,
# une dizaine de positions est déjà un maximum réaliste.
MAX_TOKENS_EN_STOCK = 10

# Valeur immobilisée par token, sur la plateforme où tu comptes vendre
VALEUR_STOCK_PAR_TOKEN_USDT = 50.0


# ============================================================
# NOUVELLES COTATIONS (nouveaux_listings.py)
# ============================================================
# Détecte les paires qui apparaissent sur une plateforme où elles n'étaient
# pas au relevé précédent. Les premières heures d'une cotation sont un des
# rares moments où un écart large est RÉEL plutôt que le symptôme d'un
# blocage : le carnet est mince, le prix n'a pas encore convergé.
# ⚠️ C'est aussi le moment le plus risqué (volatilité extrême, liquidité
# quasi nulle, retraits souvent fermés au début). Croise avec verif_retraits.
LISTINGS_ACTIF = True

# 30 min : les cotations ne sont pas si fréquentes, inutile de marteler les
# API. Chaque relevé interroge les 6 plateformes.
LISTINGS_INTERVALLE_SEC = 1800


# ============================================================
# ARBITRAGE SPOT-FUTURES (PERPÉTUEL) ET FUNDING
# ============================================================
# Voir arbitrage_perpetuel.py. Intérêt principal : les deux jambes sont sur
# LA MÊME plateforme, donc AUCUN transfert entre exchanges — ni retrait
# fermé, ni délai blockchain, ni frais de retrait fixes. C'est précisément
# le mur contre lequel bute l'arbitrage inter-plateformes classique.
PERP_ACTIF = False
# Désactivé le 04/08 à la demande : stratégie jugée trop complexe pour
# l'instant. Le module reste présent dans le dépôt mais totalement inerte —
# aucune boucle lancée, aucun appel réseau, aucun message Telegram.
# ⚠️ Le fichier arbitrage_perpetuel.py doit malgré tout exister sur le dépôt :
# bot_fusionne_v1.py l'importe en haut du fichier, et un import manquant
# fait planter le bot au démarrage (ModuleNotFoundError).
# Repasse à True quand tu voudras le réactiver.

# Base minimale (perp au-dessus du spot, en %) pour signaler un arbitrage
# de convergence. Le coût d'un cycle complet est d'environ 0.40% à 0.80%
# selon la plateforme (4 ordres), donc en dessous de ~0.8% il ne reste rien.
PERP_SEUIL_BASE_PCT = 1.0

# Funding minimal, en taux ANNUALISÉ (%), pour signaler une position de
# récolte. 50% annualisé = environ 0.046% par période de 8h.
# ⚠️ Un taux annualisé n'est PAS une promesse de rendement : il suppose que
# le taux actuel se maintienne un an, ce qui n'arrive jamais. C'est une
# unité de comparaison, pas une prévision.
PERP_SEUIL_FUNDING_APR_PCT = 50.0

# Intervalle de sondage REST. Le funding ne change qu'aux périodes (8h en
# général) : inutile d'interroger plus souvent. Les WebSockets sont réservés
# à l'arbitrage inter-plateformes, où la seconde compte.
PERP_INTERVALLE_SONDAGE_SEC = 60

# Montant de référence pour les calculs (mêmes conventions que le reste)
PERP_MONTANT_USDT = 50.0

# ⚠️ Chaque alerte perpétuelle est un message Telegram SUPPLÉMENTAIRE.
# Le cooldown interne (30 min par couple plateforme/symbole/type) limite
# fortement le débit, mais mets à False si le fil devient trop chargé.
PERP_NOTIFIER = True


# ============================================================
# FILTRE : NE TRADER QUE LES TOKENS RÉELLEMENT RETIRABLES
# ============================================================
# Un arbitrage ne peut se boucler que si le token peut PHYSIQUEMENT circuler
# entre les deux plateformes : retrait ouvert côté achat, dépôt ouvert côté
# vente, et un réseau commun aux deux.
#
# C'est la réponse au constat de fond : un écart de 13% qui PERSISTE sur un
# token peu liquide n'est presque jamais une opportunité que tout le monde
# aurait ratée — c'est un écart que personne ne PEUT refermer, parce que le
# token est prisonnier de sa plateforme. Le filtre écarte ces cas.
#
# Voir verif_retraits.py. Données publiques, aucune clé API requise.
FILTRE_RETRAITS_ACTIF = True

# "strict" : le bot ne trade QUE les paires dont le retrait est VÉRIFIÉ
#            ouvert. Seules kucoin, bitget et gateio exposent cette
#            information publiquement — donc toutes les paires impliquant
#            binance, bybit ou okx sont écartées, faute de preuve.
#            ⚠️ Cela réduit fortement le nombre d'alertes. C'est le
#            comportement demandé : ne trader que ce qui est sélectionné.
#
# "souple" : seuls les blocages CONFIRMÉS sont écartés. Les paires dont
#            l'état est inconnu (binance/bybit/okx) continuent de passer.
#            Utile si le mode strict assèche trop le flux d'alertes.
FILTRE_RETRAITS_MODE = "strict"


# ============================================================
# POSITIONS EN ATTENTE (idée à tester — mode papier uniquement)
# ============================================================
# PRINCIPE : quand un trade se retrouve négatif au moment de la vente
# (l'écart s'est refermé), au lieu d'encaisser la perte tout de suite, on
# garde la position et on vend automatiquement dès que ça repasse positif.
#
# ⚠️ POURQUOI LES TROIS LIMITES CI-DESSOUS SONT INDISPENSABLES
# Sans elles, cette stratégie plafonne les gains (on vend dès +0.01$) mais
# laisse courir les pertes (on ne vend jamais tant que c'est négatif). Pire,
# elle fausse complètement les statistiques : une position jamais clôturée
# n'apparaît JAMAIS comme une perte, donc le taux de réussite grimpe vers
# 100% pendant que du capital dort dans des positions perdantes invisibles.
# Les limites garantissent que CHAQUE position finit par être enregistrée,
# gagnante ou perdante.
POSITIONS_ATTENTE_ACTIF = True

# Durée maximale d'attente. Au-delà : vente au prix du marché, quel qu'il
# soit, et l'emplacement est libéré. 30 min = compromis entre laisser une
# chance au prix de revenir et ne pas bloquer le capital trop longtemps.
DUREE_MAX_ATTENTE_SEC = 1800

# Stop-loss par position, en % du montant engagé. En dessous, on coupe sans
# attendre la fin du délai — c'est la protection contre un token qui
# s'effondre au lieu de remonter.
STOP_LOSS_POSITION_PCT = -3.0

# Plafond de positions simultanément en attente. Chaque position occupe un
# emplacement de stock (MAX_TOKENS_EN_STOCK = 10) : en réserver trop
# empêcherait le bot de continuer à trader normalement.
MAX_POSITIONS_EN_ATTENTE = 3

# Fréquence de vérification des positions (secondes). Lecture dans le cache
# WebSocket, aucun appel réseau — 5s est largement suffisant.
INTERVALLE_VERIF_ATTENTE_SEC = 5.0

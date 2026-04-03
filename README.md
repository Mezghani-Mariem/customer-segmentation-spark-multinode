# 🎯 Customer Segmentation with Apache Spark (Multinode)

## 📊 Description
Application web de segmentation client utilisant l'analyse **RFM** et l'algorithme **K-Means** sur un **cluster Spark distribué** (3 machines).

## 🏗️ Architecture
┌─────────────────────────────────────────────────┐
│ Spark Cluster │
├─────────────────────────────────────────────────┤
│ Master (192.168.56.101) │
│ ├── Flask Web Interface (Port 5000) │
│ └── Spark Master (Port 7077) │
├─────────────────────────────────────────────────┤
│ Worker1 (192.168.56.102) - 10 cores │
│ Worker2 (192.168.56.103) - 10 cores │
└─────────────────────────────────────────────────┘

## 📈 Fonctionnalités
- ✅ Traitement distribué de **541,909 transactions**
- ✅ Analyse RFM (Récence, Fréquence, Montant)
- ✅ Clustering K-Means (4 segments clients)
- ✅ Interface web Flask
- ✅ Visualisation interactive
- ✅ Export CSV

## 🚀 Installation

```bash
git clone https://github.com/Mezghani-Mariem/customer-segmentation-spark-multinode.git
cd customer-segmentation-spark-multinode
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
🎯 Utilisation
Démarrer Spark Master :

bash
cd /home/mariem/spark-3.5.0
./sbin/start-master.sh
Démarrer les workers :

bash
./sbin/start-worker.sh spark://192.168.56.101:7077
Lancer l'application :

bash
python app_multinode.py
Accéder à : http://192.168.56.101:5000

📊 Résultats
Cluster 0 : Champions (très actifs, gros dépensiers)

Cluster 1 : Fidèles (bons clients)

Cluster 2 : Occasionnels (clients moyens)

Cluster 3 : À risque (à réactiver)

🛠️ Technologies
Apache Spark 3.5.0

Python 3.12

Flask

K-Means

Pandas / Matplotlib

👤 Auteur
Mezghani Mariem



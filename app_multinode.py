from flask import Flask, request, jsonify
from pyspark.sql.functions import datediff, lit

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, max as spark_max
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
import base64
import os

app = Flask(__name__)

print("="*50)
print("🔥 Démarrage Spark MULTINODE")
print("="*50)

spark = SparkSession.builder \
    .appName("CustomerSegmentationMultinode") \
    .master("spark://192.168.56.101:7077") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.host", "192.168.56.101") \
    .getOrCreate()

print(f"✅ Spark démarré!")
print(f"   Master: {spark.sparkContext.master}")
print(f"   Version: {spark.version}")

@app.route('/')
def index():
    return '''
    <html>
    <head><title>Segmentation Client</title>
    <style>
        body{font-family:Arial;margin:40px;background:linear-gradient(135deg,#667eea,#764ba2);}
        .container{max-width:900px;margin:auto;background:white;padding:30px;border-radius:10px;}
        button{background:#667eea;color:white;padding:12px 30px;border:none;border-radius:5px;cursor:pointer;}
        img{max-width:100%;}
        table{width:100%;border-collapse:collapse;}
        th,td{border:1px solid #ddd;padding:8px;}
        th{background:#667eea;color:white;}
    </style>
    </head>
    <body>
    <div class="container">
        <h1>🎯 Segmentation Client - MODE MULTINODE</h1>
        <p>Cluster: 2 workers | 20 cœurs</p>
        <button onclick="start()">🚀 Lancer</button>
        <div id="status"></div>
        <div id="results"></div>
    </div>
    <script>
        async function start(){
            document.getElementById('status').innerHTML='⏳ Traitement...';
            const r=await fetch('/segment',{method:'POST'});
            const d=await r.json();
            if(d.success){
                document.getElementById('status').innerHTML='✅ Terminé!';
                document.getElementById('results').innerHTML=`
                    <p>👥 Clients: ${d.total_customers}</p>
                    <img src="data:image/png;base64,${d.plot_url}">
                    ${d.stats_html}
                `;
            }else{
                document.getElementById('status').innerHTML='❌ '+d.error;
            }
        }
    </script>
    </body>
    </html>
    '''

@app.route('/segment', methods=['POST'])
def segment():
    try:
        df = spark.read.csv("../data.csv", header=True, inferSchema=True)
        df_clean = df.filter(col("CustomerID").isNotNull()).filter(col("Quantity")>0).filter(col("UnitPrice")>0)
        df_with_total = df_clean.withColumn("TotalAmount", col("Quantity")*col("UnitPrice"))
        
        customer_df = df_with_total.groupBy("CustomerID").agg(
            sum("TotalAmount").alias("Monetary"),
            count("InvoiceNo").alias("Frequency")
        )
        max_date = df_with_total.select(spark_max("InvoiceDate")).collect()[0][0]
        recency_df = df_with_total.groupBy("CustomerID") \
    .agg(datediff(lit(max_date), spark_max("InvoiceDate")).alias("Recency"))
        final_df = recency_df.join(customer_df, "CustomerID").filter(col("Recency")>0)
        
        assembler = VectorAssembler(inputCols=["Recency","Frequency","Monetary"], outputCol="features")
        df_features = assembler.transform(final_df)
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        df_scaled = scaler.fit(df_features).transform(df_features)
        
        kmeans = KMeans(featuresCol="scaled_features", k=4, seed=42)
        predictions = kmeans.fit(df_scaled).transform(df_scaled)
        
        pdf = predictions.select("prediction","Frequency","Monetary").limit(500).toPandas()
        plt.figure(figsize=(10,6))
        colors=['red','blue','green','purple']
        for c in pdf['prediction'].unique():
            d=pdf[pdf['prediction']==c]
            plt.scatter(d['Frequency'],d['Monetary'],c=colors[int(c)],label=f'Cluster {int(c)}',alpha=0.6)
        plt.xlabel('Fréquence')
        plt.ylabel('Montant')
        plt.legend()
        img=io.BytesIO()
        plt.savefig(img,format='png')
        img.seek(0)
        plot_url=base64.b64encode(img.getvalue()).decode()
        plt.close()
        
        stats = predictions.groupBy("prediction").agg(
            avg("Recency").alias("Récence"),
            avg("Frequency").alias("Fréquence"),
            avg("Monetary").alias("Montant"),
            count("*").alias("Nb clients")
        ).toPandas().round(2)
        
        return jsonify({'success':True,'plot_url':plot_url,'stats_html':stats.to_html(index=False),'total_customers':predictions.count()})
    except Exception as e:
        return jsonify({'success':False,'error':str(e)})

if __name__ == '__main__':
    print("🌐 http://192.168.56.101:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)

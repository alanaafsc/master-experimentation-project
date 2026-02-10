import numpy as np
from prometheus_api_client import PrometheusConnect
from datetime import datetime, timedelta
## processamento estatístico (Análise) para detectar anomalias
#Regra $3\sigma$ e o Random Walk


#===================================================================
# Conexão com o Prometheus
prom = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)

def get_p95_latency(service_name):
    # Query para pegar o P95 da latência nos últimos 30s (Istio)
    query = f'histogram_quantile(0.95, sum(rate(istio_request_duration_milliseconds_bucket{{destination_service="{service_name}"}}[1m])) by (le))'
    result = prom.custom_query(query=query)
    return float(result[0]['value'][1]) if result else 0.0

def check_3_sigma_anomaly(history_data, current_value):
    if len(history_data) < 10: return False # Espera ter dados suficientes
    
    mu = np.mean(history_data) # Média histórica 
    sigma = np.std(history_data) # Desvio padrão 
    
    # Regra 3-sigma do artigo: P95_now >= mu + 3 * sigma 
    upper_bound = mu + 3 * sigma
    
    if current_value >= upper_bound:
        print(f"ANOMALIA DETECTADA! Valor: {current_value:.2f}, Limite: {upper_bound:.2f}")
        return True
    return False

# Exemplo de loop de monitoramento (executa a cada 30s conforme o artigo) 
history = []
service = "product-service" # Exemplo de serviço do Hipster-shop 

# Simulando coleta
# current_p95 = get_p95_latency(service)
# if check_3_sigma_anomaly(history, current_p95):
#     # Próximo passo: Chamar o Random Walk
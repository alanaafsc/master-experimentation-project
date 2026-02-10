import networkx as nx
from prometheus_api_client import PrometheusConnect

class MonitoringModule:
    def __init__(self):
        # Conexão com o Prometheus - via Helm
        self.prom = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)

    def get_topology(self):
        """
        Extrai as dependências (edges) entre microsserviços do Istio.
        Isso mapeia quem chama quem (p_k,m) conforme o artigo.
        """
        query = 'sum(rate(istio_requests_total[5m])) by (source_workload, destination_workload)'
        results = self.prom.custom_query(query=query)
        
        G = nx.DiGraph()
        for res in results:
            source = res['metric'].get('source_workload', 'unknown')
            dest = res['metric'].get('destination_workload', 'unknown')
            weight = float(res['value'][1])
            if source != 'unknown' and dest != 'unknown':
                G.add_edge(source, dest, weight=weight)
        return G

    def get_metrics(self, service_name):
        """Coleta P95 e Throughput conforme Seção III.A """
        # Latência P95 (Regra 3-sigma) 
        p95_query = f'histogram_quantile(0.95, sum(rate(istio_request_duration_milliseconds_bucket{{destination_workload="{service_name}"}}[1m])) by (le))'
        
        # Throughput (Overcommit Detection)
        throughput_query = f'sum(rate(istio_requests_total{{destination_workload="{service_name}"}}[1m]))'
        
        # ... lógica de requisição ao Prometheus ...
        return p95_val, throughput_val
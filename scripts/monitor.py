"""
monitor.py — Módulo de Monitoramento (MAPE-K: Monitor)

Responsável pela coleta de métricas de telemetria via Prometheus e pela
extração da topologia de dependências entre microsserviços via Istio.

Métricas coletadas por serviço (a cada ciclo de SCRAPE_INTERVAL_S):
  - Latência P95  : percentil 95 das latências de requisição (ms)
  - Throughput    : taxa de requisições processadas (req/s)
  - Uso de CPU    : núcleos virtuais consumidos (cores)

A topologia é representada por um grafo dirigido G = (V, E), onde:
  - V = conjunto de microsserviços identificados pelo Istio
  - E = arestas ponderadas pelo volume de tráfego entre pares (source, destination)
"""

import logging

import networkx as nx
from prometheus_api_client import PrometheusConnect

from config import PROMETHEUS_URL, SERVICES

logger = logging.getLogger("MonitoringModule")


class MonitoringModule:
    """
    Interface com Prometheus e Istio para coleta de métricas e topologia.

    Attributes:
        prom : PrometheusConnect — cliente HTTP do Prometheus
    """

    def __init__(self):
        self.prom = PrometheusConnect(url=PROMETHEUS_URL, disable_ssl=True)
        logger.info("MonitoringModule iniciado. Prometheus: %s", PROMETHEUS_URL)

    # ─────────────────────────────────────────────────────────────────────────
    # Topologia de dependências
    # ─────────────────────────────────────────────────────────────────────────

    def get_topology(self) -> nx.DiGraph:
        """
        Extrai o grafo dirigido de dependências entre microsserviços.

        A query PromQL agrega o volume de tráfego de 5 minutos por par
        (source_workload, destination_workload), produzindo as arestas E do
        grafo G com peso proporcional ao volume de chamadas.

        Returns:
            nx.DiGraph com serviços como nós e tráfego como peso das arestas.
        """
        query = (
            "sum(rate(istio_requests_total[5m])) "
            "by (source_workload, destination_workload)"
        )
        results = self.prom.custom_query(query=query)

        G = nx.DiGraph()
        for res in results:
            source = res["metric"].get("source_workload", "")
            dest = res["metric"].get("destination_workload", "")
            weight = float(res["value"][1])
            if source and dest and source != "unknown" and dest != "unknown":
                G.add_edge(source, dest, weight=weight)

        logger.debug("Topologia extraída: %d nós, %d arestas.", G.number_of_nodes(), G.number_of_edges())
        return G

    # ─────────────────────────────────────────────────────────────────────────
    # Métricas por serviço
    # ─────────────────────────────────────────────────────────────────────────

    def get_metrics(self, service_name: str) -> tuple[float, float, float]:
        """
        Coleta latência P95, throughput e CPU para um serviço específico.

        Queries PromQL utilizadas:
          P95     → histogram_quantile(0.95, ...)  sobre istio_request_duration_milliseconds_bucket
          Tput    → sum(rate(istio_requests_total[1m]))
          CPU     → sum(rate(container_cpu_usage_seconds_total[1m]))

        Args:
            service_name : nome do workload Kubernetes (ex: "productcatalogservice")

        Returns:
            Tupla (p95_ms, throughput_rps, cpu_cores). Retorna 0.0 em caso de
            ausência de dados (serviço ainda sem tráfego ou Prometheus indisponível).
        """
        # 1. Latência P95 (ms)
        p95_query = (
            f'histogram_quantile(0.95, sum(rate('
            f'istio_request_duration_milliseconds_bucket{{'
            f'destination_workload="{service_name}"}}[1m])) by (le))'
        )
        p95_res = self.prom.custom_query(query=p95_query)
        p95_val = float(p95_res[0]["value"][1]) if p95_res else 0.0

        # 2. Throughput (req/s)
        tput_query = (
            f'sum(rate(istio_requests_total{{'
            f'destination_workload="{service_name}"}}[1m]))'
        )
        tput_res = self.prom.custom_query(query=tput_query)
        tput_val = float(tput_res[0]["value"][1]) if tput_res else 0.0

        # 3. Uso de CPU (cores)
        cpu_query = (
            f'sum(rate(container_cpu_usage_seconds_total{{'
            f'container="{service_name}"}}[1m]))'
        )
        cpu_res = self.prom.custom_query(query=cpu_query)
        cpu_val = float(cpu_res[0]["value"][1]) if cpu_res else 0.0

        logger.debug(
            "[%s] P95=%.2fms | Tput=%.4f req/s | CPU=%.4f cores",
            service_name, p95_val, tput_val, cpu_val,
        )
        return p95_val, tput_val, cpu_val

    def get_all_metrics(self) -> dict[str, dict[str, float]]:
        """
        Coleta métricas de todos os serviços listados em SERVICES.

        Returns:
            Dicionário {service_name: {p95_ms, throughput_rps, cpu_cores}}.
        """
        all_metrics: dict[str, dict[str, float]] = {}
        for svc in SERVICES:
            p95, tput, cpu = self.get_metrics(svc)
            all_metrics[svc] = {
                "p95_ms": round(p95, 2),
                "throughput_rps": round(tput, 4),
                "cpu_cores": round(cpu, 4),
            }
        return all_metrics


# ── Bloco de teste manual ───────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.DEBUG)
    monitor = MonitoringModule()

    svc = sys.argv[1] if len(sys.argv) > 1 else "frontend"
    p95, tput, cpu = monitor.get_metrics(svc)
    print(f"\n{svc}:")
    print(f"  P95 Latência : {p95:.2f} ms")
    print(f"  Throughput   : {tput:.4f} req/s")
    print(f"  CPU          : {cpu:.4f} cores")

    topo = monitor.get_topology()
    print(f"\nTopologia: {list(topo.edges(data='weight'))}")

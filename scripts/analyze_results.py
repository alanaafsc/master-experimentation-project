"""
analyze_results.py — Análise pós-experimento: XAI-FinOps vs HPA Baseline.

Processa os artefatos gerados por run_experiment.py e hpa_baseline.py
para calcular as métricas de avaliação descritas no Capítulo 6 da dissertação
e gerar as figuras e tabelas correspondentes.

Métricas calculadas:
  Detecção   : Precision, Recall, F1-Score, Tempo de Detecção
  Causa Raiz : Accuracy na identificação do serviço causador da falha
  MTTR       : Mean Time To Repair — XAI-FinOps vs HPA
  FinOps     : Escalonamentos desnecessários, custo computacional evitado

Saídas em analysis/:
  figures/fig1_latency_timeline.png   — linha do tempo de latência P95
  figures/fig2_mttr_comparison.png    — comparação de MTTR (barras)
  figures/fig3_scale_events.png       — escalonamentos por serviço
  figures/fig4_detection_metrics.png  — Precision / Recall / F1
  tables/tab1_detection_metrics.csv   — tabela de detecção
  tables/tab2_root_cause_accuracy.csv — tabela de causa raiz
  tables/tab3_mttr_comparison.csv     — tabela de MTTR
  tables/tab4_finops_metrics.csv      — tabela FinOps
  summary.txt                         — resumo textual completo

Uso:
    cd master-experimentation-project/
    python scripts/analyze_results.py
"""

import glob as glob_module
import json
import os
from datetime import datetime, timezone

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.use("Agg")

# ── Diretórios ────────────────────────────────────────────────────────────────
XAI_DIR = "results"
HPA_DIR = "results_hpa"
OUT_DIR = "analysis"
TABLES_DIR = os.path.join(OUT_DIR, "tables")
FIGURES_DIR = os.path.join(OUT_DIR, "figures")

for _d in [OUT_DIR, TABLES_DIR, FIGURES_DIR]:
    os.makedirs(_d, exist_ok=True)

# ── Parâmetros de análise ─────────────────────────────────────────────────────
FAULT_SERVICE = "productcatalogservice"   # serviço com falha injetada (Cenário A)
RECOVERY_K = 2.0                          # k-σ para considerar sistema recuperado
VCPU_COST_PER_HOUR = 0.048               # USD/vCPU/hora (referência GKE n1-standard)
VCPU_PER_REPLICA = 0.5                   # vCPUs estimadas por réplica adicional


# ─────────────────────────────────────────────────────────────────────────────
# Carregamento de dados
# ─────────────────────────────────────────────────────────────────────────────

def load_metrics(results_dir: str) -> pd.DataFrame:
    """Carrega metrics_raw.csv como DataFrame com timestamp em UTC."""
    path = os.path.join(results_dir, "metrics_raw.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def load_meta(results_dir: str) -> dict:
    """Carrega experiment_meta.json."""
    path = os.path.join(results_dir, "experiment_meta.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_xai_events(results_dir: str) -> list[dict]:
    """Carrega todos os event_*.json do subdiretório reports/."""
    pattern = os.path.join(results_dir, "reports", "event_*.json")
    events = []
    for p in sorted(glob_module.glob(pattern)):
        with open(p, encoding="utf-8") as f:
            events.append(json.load(f))
    return events


def load_hpa_events(results_dir: str) -> pd.DataFrame:
    """Carrega hpa_events.csv com os escalonamentos detectados pelo HPA."""
    path = os.path.join(results_dir, "hpa_events.csv")
    if not os.path.exists(path):
        return pd.DataFrame(
            columns=["cycle", "timestamp", "service",
                     "from_replicas", "to_replicas", "direction"]
        )
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def _parse_utc(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def get_phase(meta: dict, phase_name: str) -> tuple[datetime, datetime]:
    """Retorna (start, end) de uma fase a partir dos metadados."""
    for phase in meta.get("phases", []):
        if phase.get("name") == phase_name and "start" in phase:
            return _parse_utc(phase["start"]), _parse_utc(phase["end"])
    raise ValueError(f"Fase '{phase_name}' não encontrada nos metadados.")


# ─────────────────────────────────────────────────────────────────────────────
# Métricas de detecção de anomalias
# ─────────────────────────────────────────────────────────────────────────────

def compute_detection_metrics(
    metrics_df: pd.DataFrame,
    meta: dict,
    xai_events: list[dict],
) -> dict:
    """
    Calcula Precision, Recall, F1 e tempo de detecção.

    Ground truth:
      - Positivo (deveria detectar): ciclos na fase FAULT_INJECTION
      - Negativo (não deveria detectar): ciclos na fase BASELINE

    TP = eventos de anomalia detectados dentro da fase de falha
    FP = eventos de anomalia detectados dentro da fase de baseline (falsos alarmes)
    FN = ciclos da fase de falha sem detecção de anomalia
    """
    baseline_start, baseline_end = get_phase(meta, "BASELINE")
    fault_start, fault_end = get_phase(meta, "FAULT_INJECTION")

    # Ciclos totais de cada fase para o serviço alvo
    fault_cycles = metrics_df[
        (metrics_df["service"] == FAULT_SERVICE) &
        (metrics_df["timestamp"] >= fault_start) &
        (metrics_df["timestamp"] <= fault_end)
    ]
    total_fault_cycles = len(fault_cycles)

    # Eventos detectados por fase
    tp = 0
    fp = 0
    first_detection_ts = None

    for ev in xai_events:
        ev_ts = _parse_utc(ev["timestamp"])
        if fault_start <= ev_ts <= fault_end:
            tp += 1
            if first_detection_ts is None or ev_ts < first_detection_ts:
                first_detection_ts = ev_ts
        elif baseline_start <= ev_ts <= baseline_end:
            fp += 1

    fn = max(0, total_fault_cycles - tp)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1        = (2 * precision * recall / (precision + recall)
                 if (precision + recall) > 0 else 0.0)

    detection_time_s = None
    if first_detection_ts is not None:
        detection_time_s = round((first_detection_ts - fault_start).total_seconds(), 1)

    return {
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "total_fault_cycles": total_fault_cycles,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "detection_time_s": detection_time_s,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Acurácia de identificação de causa raiz
# ─────────────────────────────────────────────────────────────────────────────

def compute_root_cause_accuracy(meta: dict, xai_events: list[dict]) -> dict:
    """
    Calcula a taxa de acerto na identificação da causa raiz.

    Correto: root_cause_service == FAULT_SERVICE durante a fase de falha.
    """
    fault_start, fault_end = get_phase(meta, "FAULT_INJECTION")

    fault_events = [
        ev for ev in xai_events
        if fault_start <= _parse_utc(ev["timestamp"]) <= fault_end
    ]

    if not fault_events:
        return {
            "fault_service": FAULT_SERVICE,
            "total_events": 0,
            "correct_root_cause": 0,
            "incorrect_root_cause": 0,
            "accuracy": None,
        }

    correct = sum(
        1 for ev in fault_events
        if ev.get("root_cause_service") == FAULT_SERVICE
    )

    return {
        "fault_service": FAULT_SERVICE,
        "total_events": len(fault_events),
        "correct_root_cause": correct,
        "incorrect_root_cause": len(fault_events) - correct,
        "accuracy": round(correct / len(fault_events), 4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MTTR
# ─────────────────────────────────────────────────────────────────────────────

def compute_mttr(metrics_df: pd.DataFrame, meta: dict, label: str) -> dict:
    """
    Calcula o MTTR como o tempo entre remoção da falha e retorno da latência
    P95 à banda de controle: μ_baseline + RECOVERY_K × σ_baseline.

    Convenção: se o sistema não recuperar dentro do período de observação,
    retorna mttr_s=None com mensagem de erro.
    """
    baseline_start, baseline_end = get_phase(meta, "BASELINE")
    fault_removed_at = _parse_utc(meta["fault_removed_at"])

    baseline_p95 = metrics_df[
        (metrics_df["service"] == FAULT_SERVICE) &
        (metrics_df["timestamp"] >= baseline_start) &
        (metrics_df["timestamp"] <= baseline_end)
    ]["p95_ms"]

    if baseline_p95.empty:
        return {"label": label, "mttr_s": None, "error": "Sem dados de baseline"}

    mu = baseline_p95.mean()
    sigma = baseline_p95.std(ddof=1)
    recovery_threshold = mu + RECOVERY_K * sigma

    post_fault = metrics_df[
        (metrics_df["service"] == FAULT_SERVICE) &
        (metrics_df["timestamp"] > fault_removed_at)
    ].sort_values("timestamp")

    recovered = post_fault[post_fault["p95_ms"] <= recovery_threshold].head(1)

    if recovered.empty:
        return {
            "label": label,
            "fault_removed_at": fault_removed_at.isoformat(),
            "baseline_mean_ms": round(mu, 2),
            "baseline_std_ms": round(sigma, 2),
            "recovery_threshold_ms": round(recovery_threshold, 2),
            "mttr_s": None,
            "error": "Sistema não recuperou dentro do período de observação",
        }

    recovered_at = recovered["timestamp"].iloc[0]
    mttr_s = (recovered_at - fault_removed_at).total_seconds()

    return {
        "label": label,
        "fault_removed_at": fault_removed_at.isoformat(),
        "recovered_at": recovered_at.isoformat(),
        "baseline_mean_ms": round(mu, 2),
        "baseline_std_ms": round(sigma, 2),
        "recovery_threshold_ms": round(recovery_threshold, 2),
        "mttr_s": round(mttr_s, 1),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Métricas FinOps
# ─────────────────────────────────────────────────────────────────────────────

def compute_finops(
    xai_events: list[dict],
    hpa_events_df: pd.DataFrame,
    meta_xai: dict,
    meta_hpa: dict,
) -> dict:
    """
    Compara o custo computacional entre XAI-FinOps e HPA.

    Escalonamento "desnecessário" = escalonamento aplicado a um serviço que
    NÃO é o FAULT_SERVICE durante a fase de falha (serviço sintomático escalado).

    Custo estimado = réplicas_desnecessárias × vCPUs_por_réplica
                     × duração_fase_falha_h × USD_por_vCPU_h
    """
    fault_start, fault_end = get_phase(meta_xai, "FAULT_INJECTION")
    fault_duration_h = (fault_end - fault_start).total_seconds() / 3600

    # ── XAI-FinOps ───────────────────────────────────────────────────────────
    xai_scale_up = [ev for ev in xai_events if "SCALE_UP" in ev.get("decision", "")]
    xai_correct  = [ev for ev in xai_scale_up if ev.get("root_cause_service") == FAULT_SERVICE]
    xai_wrong    = [ev for ev in xai_scale_up if ev.get("root_cause_service") != FAULT_SERVICE]
    xai_wrong_replicas = sum(
        ev.get("execute_result", {}).get("to_replicas", 3)
        - ev.get("execute_result", {}).get("from_replicas", 1)
        for ev in xai_wrong
    )
    xai_wrong_cost = xai_wrong_replicas * VCPU_PER_REPLICA * fault_duration_h * VCPU_COST_PER_HOUR

    # ── HPA Baseline ─────────────────────────────────────────────────────────
    if not hpa_events_df.empty:
        try:
            hpa_fault_start, hpa_fault_end = get_phase(meta_hpa, "FAULT_INJECTION")
        except ValueError:
            hpa_fault_start, hpa_fault_end = fault_start, fault_end

        hpa_up = hpa_events_df[
            (hpa_events_df["timestamp"] >= hpa_fault_start) &
            (hpa_events_df["timestamp"] <= hpa_fault_end) &
            (hpa_events_df["direction"] == "SCALE_UP")
        ]
        hpa_correct = hpa_up[hpa_up["service"] == FAULT_SERVICE]
        hpa_wrong   = hpa_up[hpa_up["service"] != FAULT_SERVICE]
        hpa_wrong_replicas = int(
            (hpa_wrong["to_replicas"] - hpa_wrong["from_replicas"]).clip(lower=0).sum()
        )
    else:
        hpa_up = pd.DataFrame()
        hpa_correct = pd.DataFrame()
        hpa_wrong = pd.DataFrame()
        hpa_wrong_replicas = 0

    hpa_wrong_cost = hpa_wrong_replicas * VCPU_PER_REPLICA * fault_duration_h * VCPU_COST_PER_HOUR
    cost_saved = hpa_wrong_cost - xai_wrong_cost

    return {
        "fault_duration_h": round(fault_duration_h, 3),
        "vcpu_cost_per_hour_usd": VCPU_COST_PER_HOUR,
        "xai_finops": {
            "total_scale_up_events": len(xai_scale_up),
            "correct_scale_events": len(xai_correct),
            "unnecessary_scale_events": len(xai_wrong),
            "unnecessary_replicas_added": xai_wrong_replicas,
            "estimated_unnecessary_cost_usd": round(xai_wrong_cost, 6),
        },
        "hpa_baseline": {
            "total_scale_up_events": len(hpa_up),
            "correct_scale_events": len(hpa_correct),
            "unnecessary_scale_events": len(hpa_wrong),
            "unnecessary_replicas_added": hpa_wrong_replicas,
            "estimated_unnecessary_cost_usd": round(hpa_wrong_cost, 6),
        },
        "comparison": {
            "estimated_cost_saved_usd": round(cost_saved, 6),
            "unnecessary_replicas_avoided": hpa_wrong_replicas - xai_wrong_replicas,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Figuras
# ─────────────────────────────────────────────────────────────────────────────

def _shade_phase(ax, meta: dict, phase_name: str, color: str, label: str):
    try:
        s, e = get_phase(meta, phase_name)
        ax.axvspan(s, e, alpha=0.12, color=color, label=label)
        ax.axvline(s, color=color, linestyle="--", linewidth=0.8, alpha=0.6)
    except ValueError:
        pass


def plot_latency_timeline(
    df_xai: pd.DataFrame,
    df_hpa: pd.DataFrame,
    meta_xai: dict,
    meta_hpa: dict,
):
    """Fig 1 — Latência P95 do serviço causador ao longo do experimento."""
    fig, axes = plt.subplots(2, 1, figsize=(14, 8))

    configs = [
        (axes[0], df_xai, meta_xai, "XAI-FinOps", "#2c7bb6"),
        (axes[1], df_hpa, meta_hpa, "HPA Baseline (Grupo de Controle)", "#d7191c"),
    ]

    for ax, df, meta, label, color in configs:
        svc_df = df[df["service"] == FAULT_SERVICE].sort_values("timestamp")
        ax.plot(svc_df["timestamp"], svc_df["p95_ms"],
                color=color, linewidth=1.5, label=f"P95 — {FAULT_SERVICE}", zorder=3)

        _shade_phase(ax, meta, "BASELINE", "steelblue", "Fase Baseline")
        _shade_phase(ax, meta, "FAULT_INJECTION", "orange", "Fase Falha")
        _shade_phase(ax, meta, "RECOVERY", "green", "Fase Recuperação")

        try:
            fr = _parse_utc(meta["fault_removed_at"])
            ax.axvline(fr, color="darkgreen", linestyle=":", linewidth=2,
                       alpha=0.9, label="Falha removida (t₀ MTTR)", zorder=4)
        except (KeyError, ValueError):
            pass

        ax.set_title(f"{label} — P95: {FAULT_SERVICE}", fontsize=11, pad=6)
        ax.set_ylabel("Latência P95 (ms)", fontsize=9)
        ax.legend(fontsize=8, loc="upper left", ncol=2)
        ax.grid(True, alpha=0.2, linestyle="--")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        plt.setp(ax.get_xticklabels(), rotation=20, ha="right", fontsize=8)

    axes[1].set_xlabel("Tempo", fontsize=9)
    fig.suptitle(
        f"Latência P95 — {FAULT_SERVICE}\nXAI-FinOps vs HPA Baseline",
        fontsize=12, y=1.01,
    )
    fig.tight_layout()
    path = os.path.join(FIGURES_DIR, "fig1_latency_timeline.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  [Fig 1] {path}")


def plot_mttr_comparison(mttr_xai: dict, mttr_hpa: dict):
    """Fig 2 — Comparação de MTTR entre XAI-FinOps e HPA."""
    labels = ["XAI-FinOps", "HPA Baseline"]
    values = [mttr_xai.get("mttr_s") or 0, mttr_hpa.get("mttr_s") or 0]
    colors = ["#2c7bb6", "#d7191c"]

    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(labels, values, color=colors, width=0.45,
                  edgecolor="white", linewidth=1.5)

    for bar, val in zip(bars, values):
        if val > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(values) * 0.02,
                f"{val:.0f}s",
                ha="center", va="bottom", fontsize=13, fontweight="bold",
            )

    ymax = max(values) * 1.3 if max(values) > 0 else 120
    ax.set_ylim(0, ymax)
    ax.set_ylabel("MTTR (segundos)", fontsize=11)
    ax.set_title("Mean Time To Repair (MTTR)\nXAI-FinOps vs HPA Baseline", fontsize=12)
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines[["top", "right"]].set_visible(False)

    if mttr_xai.get("mttr_s") and mttr_hpa.get("mttr_s"):
        reduction = (mttr_hpa["mttr_s"] - mttr_xai["mttr_s"]) / mttr_hpa["mttr_s"] * 100
        ax.text(
            0.5, 0.92,
            f"Redução: {reduction:.1f}%",
            transform=ax.transAxes, ha="center", va="top",
            fontsize=10, color="darkgreen", fontweight="bold",
        )

    fig.tight_layout()
    path = os.path.join(FIGURES_DIR, "fig2_mttr_comparison.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  [Fig 2] {path}")


def plot_scale_events(
    xai_events: list[dict],
    hpa_events_df: pd.DataFrame,
    meta_hpa: dict,
):
    """Fig 3 — Escalonamentos por serviço (XAI-FinOps vs HPA)."""
    xai_by_svc: dict[str, int] = {}
    for ev in xai_events:
        if "SCALE_UP" in ev.get("decision", ""):
            svc = ev.get("root_cause_service", "unknown")
            xai_by_svc[svc] = xai_by_svc.get(svc, 0) + 1

    hpa_by_svc: dict[str, int] = {}
    if not hpa_events_df.empty:
        try:
            hs, he = get_phase(meta_hpa, "FAULT_INJECTION")
        except ValueError:
            hs, he = None, None
        if hs and he:
            hpa_fault = hpa_events_df[
                (hpa_events_df["timestamp"] >= hs) &
                (hpa_events_df["timestamp"] <= he) &
                (hpa_events_df["direction"] == "SCALE_UP")
            ]
            for _, row in hpa_fault.iterrows():
                svc = row["service"]
                hpa_by_svc[svc] = hpa_by_svc.get(svc, 0) + 1

    all_svc = sorted(set(list(xai_by_svc) + list(hpa_by_svc)))
    if not all_svc:
        print("  [Fig 3] Sem escalonamentos para plotar.")
        return

    x = np.arange(len(all_svc))
    w = 0.35

    fig, ax = plt.subplots(figsize=(max(8, len(all_svc) * 2 + 2), 5))
    b1 = ax.bar(x - w/2, [xai_by_svc.get(s, 0) for s in all_svc],
                w, label="XAI-FinOps", color="#2c7bb6")
    b2 = ax.bar(x + w/2, [hpa_by_svc.get(s, 0) for s in all_svc],
                w, label="HPA Baseline", color="#d7191c")

    for bars in [b1, b2]:
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, h + 0.05,
                        str(int(h)), ha="center", va="bottom", fontsize=9)

    # Destaca a causa raiz esperada
    if FAULT_SERVICE in all_svc:
        idx = all_svc.index(FAULT_SERVICE)
        ax.axvspan(idx - 0.5, idx + 0.5, alpha=0.08, color="green",
                   label=f"Causa raiz esperada: {FAULT_SERVICE}")

    short = [s.replace("service", "\nsvc") for s in all_svc]
    ax.set_xticks(x)
    ax.set_xticklabels(short, fontsize=9)
    ax.set_ylabel("Nº de Escalonamentos (SCALE_UP)", fontsize=10)
    ax.set_title(
        "Escalonamentos por Serviço — XAI-FinOps vs HPA\n"
        "(ideal: escalar apenas o serviço causa raiz)",
        fontsize=11,
    )
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines[["top", "right"]].set_visible(False)

    fig.tight_layout()
    path = os.path.join(FIGURES_DIR, "fig3_scale_events.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  [Fig 3] {path}")


def plot_detection_metrics(detection: dict):
    """Fig 4 — Precision, Recall e F1-Score do XAI-FinOps."""
    metrics = {
        "Precision": detection["precision"],
        "Recall": detection["recall"],
        "F1-Score": detection["f1_score"],
    }
    colors = ["#2c7bb6", "#1a9641", "#fdae61"]

    fig, ax = plt.subplots(figsize=(6, 5))
    bars = ax.bar(metrics.keys(), metrics.values(), color=colors, width=0.45,
                  edgecolor="white", linewidth=1.5)

    for bar, val in zip(bars, metrics.values()):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.015,
            f"{val:.4f}",
            ha="center", va="bottom", fontsize=12, fontweight="bold",
        )

    ax.set_ylim(0, 1.18)
    ax.set_ylabel("Score", fontsize=11)
    ax.set_title(
        "Métricas de Detecção de Anomalias — XAI-FinOps\n"
        "Regra 3σ + Correlação de Pearson + ARIMA(2,1,0)",
        fontsize=11,
    )
    ax.axhline(1.0, color="gray", linestyle=":", alpha=0.4)
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines[["top", "right"]].set_visible(False)

    if detection.get("detection_time_s") is not None:
        ax.text(
            0.98, 0.04,
            f"Tempo de detecção: {detection['detection_time_s']}s\n"
            f"TP={detection['true_positives']}  FP={detection['false_positives']}  "
            f"FN={detection['false_negatives']}",
            transform=ax.transAxes, ha="right", va="bottom", fontsize=8,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", alpha=0.85),
        )

    fig.tight_layout()
    path = os.path.join(FIGURES_DIR, "fig4_detection_metrics.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  [Fig 4] {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Exportação de tabelas CSV
# ─────────────────────────────────────────────────────────────────────────────

def export_tables(
    detection: dict,
    root_cause: dict,
    mttr_xai: dict,
    mttr_hpa: dict,
    finops: dict,
):
    """Salva as 4 tabelas de resultados em CSV."""

    pd.DataFrame({
        "Métrica": [
            "Verdadeiros Positivos (TP)",
            "Falsos Positivos (FP)",
            "Falsos Negativos (FN)",
            "Ciclos totais na fase de falha",
            "Precision",
            "Recall",
            "F1-Score",
            "Tempo de Detecção (s)",
        ],
        "Valor": [
            detection["true_positives"],
            detection["false_positives"],
            detection["false_negatives"],
            detection["total_fault_cycles"],
            detection["precision"],
            detection["recall"],
            detection["f1_score"],
            detection.get("detection_time_s", "N/A"),
        ],
    }).to_csv(os.path.join(TABLES_DIR, "tab1_detection_metrics.csv"),
              index=False, encoding="utf-8")

    pd.DataFrame({
        "Métrica": [
            "Serviço com falha injetada",
            "Total de eventos na fase de falha",
            "Causa raiz identificada corretamente",
            "Causa raiz identificada incorretamente",
            "Accuracy",
        ],
        "Valor": [
            root_cause["fault_service"],
            root_cause["total_events"],
            root_cause["correct_root_cause"],
            root_cause["incorrect_root_cause"],
            root_cause.get("accuracy", "N/A"),
        ],
    }).to_csv(os.path.join(TABLES_DIR, "tab2_root_cause_accuracy.csv"),
              index=False, encoding="utf-8")

    pd.DataFrame({
        "Sistema": ["XAI-FinOps", "HPA Baseline"],
        "MTTR (s)": [
            mttr_xai.get("mttr_s", "N/A"),
            mttr_hpa.get("mttr_s", "N/A"),
        ],
        "Threshold de recuperação (ms)": [
            mttr_xai.get("recovery_threshold_ms", "N/A"),
            mttr_hpa.get("recovery_threshold_ms", "N/A"),
        ],
        "Baseline μ P95 (ms)": [
            mttr_xai.get("baseline_mean_ms", "N/A"),
            mttr_hpa.get("baseline_mean_ms", "N/A"),
        ],
        "Baseline σ P95 (ms)": [
            mttr_xai.get("baseline_std_ms", "N/A"),
            mttr_hpa.get("baseline_std_ms", "N/A"),
        ],
    }).to_csv(os.path.join(TABLES_DIR, "tab3_mttr_comparison.csv"),
              index=False, encoding="utf-8")

    xf = finops["xai_finops"]
    hf = finops["hpa_baseline"]
    pd.DataFrame({
        "Métrica": [
            "Total de escalonamentos (SCALE_UP)",
            "Escalonamentos corretos (causa raiz)",
            "Escalonamentos desnecessários",
            "Réplicas desnecessárias adicionadas",
            "Custo estimado desnecessário (USD)",
        ],
        "XAI-FinOps": [
            xf["total_scale_up_events"],
            xf["correct_scale_events"],
            xf["unnecessary_scale_events"],
            xf["unnecessary_replicas_added"],
            xf["estimated_unnecessary_cost_usd"],
        ],
        "HPA Baseline": [
            hf["total_scale_up_events"],
            hf["correct_scale_events"],
            hf["unnecessary_scale_events"],
            hf["unnecessary_replicas_added"],
            hf["estimated_unnecessary_cost_usd"],
        ],
    }).to_csv(os.path.join(TABLES_DIR, "tab4_finops_metrics.csv"),
              index=False, encoding="utf-8")

    print(f"  [Tabelas] Salvas em {TABLES_DIR}/")


# ─────────────────────────────────────────────────────────────────────────────
# Resumo textual
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(
    detection: dict,
    root_cause: dict,
    mttr_xai: dict,
    mttr_hpa: dict,
    finops: dict,
):
    comp = finops["comparison"]
    xf   = finops["xai_finops"]
    hf   = finops["hpa_baseline"]

    mttr_reduction = None
    if mttr_xai.get("mttr_s") and mttr_hpa.get("mttr_s") and mttr_hpa["mttr_s"] > 0:
        mttr_reduction = (mttr_hpa["mttr_s"] - mttr_xai["mttr_s"]) / mttr_hpa["mttr_s"] * 100

    lines = [
        "",
        "=" * 65,
        "  XAI-FinOps — Resultados do Experimento (Cap 6)",
        "=" * 65,
        "",
        "── DETECÇÃO DE ANOMALIAS (XAI-FinOps, Regra 3σ) ────────────",
        f"  TP={detection['true_positives']}  FP={detection['false_positives']}  FN={detection['false_negatives']}",
        f"  Precision      : {detection['precision']:.4f}",
        f"  Recall         : {detection['recall']:.4f}",
        f"  F1-Score       : {detection['f1_score']:.4f}",
        f"  Tempo detecção : {detection.get('detection_time_s', 'N/A')}s após injeção",
        "",
        "── IDENTIFICAÇÃO DE CAUSA RAIZ (Pearson) ────────────────────",
        f"  Falha injetada em : {root_cause['fault_service']}",
        f"  Accuracy          : {root_cause.get('accuracy', 'N/A')}  "
        f"({root_cause['correct_root_cause']}/{root_cause['total_events']} corretos)",
        "",
        "── MTTR (Mean Time To Repair) ───────────────────────────────",
        f"  XAI-FinOps   : {mttr_xai.get('mttr_s', 'N/A')}s",
        f"  HPA Baseline : {mttr_hpa.get('mttr_s', 'N/A')}s",
        f"  Redução      : {f'{mttr_reduction:.1f}%' if mttr_reduction is not None else 'N/A'}",
        "",
        "── FINOPS ───────────────────────────────────────────────────",
        f"  Duração da fase de falha        : {finops['fault_duration_h']:.2f}h",
        f"  XAI-FinOps  — escal. desnec.   : {xf['unnecessary_scale_events']}",
        f"  HPA Baseline — escal. desnec.   : {hf['unnecessary_scale_events']}",
        f"  Réplicas desnecessárias evitadas: {comp['unnecessary_replicas_avoided']}",
        f"  Custo estimado evitado          : USD {comp['estimated_cost_saved_usd']:.6f}",
        "",
        "=" * 65,
    ]

    text = "\n".join(lines)
    print(text)

    path = os.path.join(OUT_DIR, "summary.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"  [Resumo] {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  XAI-FinOps — analyze_results.py")
    print("=" * 65)
    print()
    print("Carregando dados...")

    try:
        df_xai    = load_metrics(XAI_DIR)
        meta_xai  = load_meta(XAI_DIR)
        xai_events = load_xai_events(XAI_DIR)
        print(f"  XAI-FinOps : {len(df_xai)} registros, {len(xai_events)} eventos XAI")
    except FileNotFoundError as e:
        print(f"  [ERRO] {e}")
        print("  Execute run_experiment.py primeiro.")
        return

    try:
        df_hpa   = load_metrics(HPA_DIR)
        meta_hpa = load_meta(HPA_DIR)
        hpa_evs  = load_hpa_events(HPA_DIR)
        print(f"  HPA Baseline: {len(df_hpa)} registros, {len(hpa_evs)} eventos HPA")
    except FileNotFoundError as e:
        print(f"  [AVISO] Dados HPA não encontrados ({e}).")
        print("  Execute hpa_baseline.py para habilitar comparação XAI vs HPA.")
        df_hpa   = pd.DataFrame()
        meta_hpa = {}
        hpa_evs  = pd.DataFrame()

    print("\nCalculando métricas...")
    detection  = compute_detection_metrics(df_xai, meta_xai, xai_events)
    root_cause = compute_root_cause_accuracy(meta_xai, xai_events)
    mttr_xai   = compute_mttr(df_xai, meta_xai, "XAI-FinOps")
    mttr_hpa   = (compute_mttr(df_hpa, meta_hpa, "HPA Baseline")
                  if not df_hpa.empty else {"label": "HPA Baseline", "mttr_s": None})
    finops     = compute_finops(xai_events, hpa_evs, meta_xai,
                                meta_hpa if meta_hpa else meta_xai)

    print("\nGerando figuras...")
    plot_latency_timeline(df_xai, df_hpa if not df_hpa.empty else df_xai,
                          meta_xai, meta_hpa if meta_hpa else meta_xai)
    plot_mttr_comparison(mttr_xai, mttr_hpa)
    plot_scale_events(xai_events, hpa_evs, meta_hpa if meta_hpa else meta_xai)
    plot_detection_metrics(detection)

    print("\nExportando tabelas...")
    export_tables(detection, root_cause, mttr_xai, mttr_hpa, finops)

    print_summary(detection, root_cause, mttr_xai, mttr_hpa, finops)

    # Salva resultado completo em JSON para rastreabilidade
    full_results = {
        "generated_at": datetime.utcnow().isoformat(),
        "fault_service": FAULT_SERVICE,
        "detection": detection,
        "root_cause": root_cause,
        "mttr_xai": mttr_xai,
        "mttr_hpa": mttr_hpa,
        "finops": finops,
    }
    with open(os.path.join(OUT_DIR, "full_results.json"), "w", encoding="utf-8") as f:
        json.dump(full_results, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()

import json
import time
import random
import os
import logging
from datetime import datetime, timezone

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("StreamingProducer")


UFS = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
       "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
       "RO", "RR", "RS", "SC", "SE", "SP", "TO"]

STATUS_META = ["atingida", "em_progresso", "critico"]

MUNICIPIOS_EXEMPLO = {
    "SP": ("3550308", "Sao Paulo"),
    "RJ": ("3304557", "Rio de Janeiro"),
    "CE": ("2304400", "Fortaleza"),
    "BA": ("2927408", "Salvador"),
    "MG": ("3106200", "Belo Horizonte"),
    "PE": ("2611606", "Recife"),
    "AM": ("1302603", "Manaus"),
    "RS": ("4314902", "Porto Alegre"),
    "PR": ("4106902", "Curitiba"),
    "DF": ("5300108", "Brasilia"),
}


def generate_mock_event():
    uf = random.choice(UFS)
    mun_id, mun_nome = MUNICIPIOS_EXEMPLO.get(uf, ("0000000", "Desconhecido"))
    nova_taxa = round(random.uniform(700, 800), 2)
    meta_atual = random.choice(STATUS_META)

    event = {
        "event_id": f"evt_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sigla_uf": uf,
        "id_municipio": mun_id,
        "nome_municipio": mun_nome,
        "nova_medicao_saeb": nova_taxa,
        "status_meta": meta_atual,
        "meta_atingida": meta_atual == "atingida",
    }
    return event


def start_producer(landing_zone_path: str, interval_sec: int = 3, max_events: int = None):
    os.makedirs(landing_zone_path, exist_ok=True)
    logger.info("=" * 60)
    logger.info(f"Producer iniciado — landing zone: {landing_zone_path}")
    logger.info(f"Intervalo: {interval_sec}s | Max eventos: {max_events or 'infinito'}")
    logger.info("=" * 60)

    count = 0
    try:
        while max_events is None or count < max_events:
            event = generate_mock_event()
            file_name = f"event_{int(time.time() * 1000)}_{count:04d}.json"
            file_path = os.path.join(landing_zone_path, file_name)

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(event, f, ensure_ascii=False)

            count += 1
            logger.info(f"[{count:04d}] {file_name} -> {event['sigla_uf']} | "
                        f"SAEB={event['nova_medicao_saeb']} | meta={event['status_meta']}")

            time.sleep(interval_sec)

    except KeyboardInterrupt:
        logger.info(f"Producer interrompido apos {count} eventos.")
    finally:
        logger.info(f"Total de eventos gerados: {count}")


if __name__ == "__main__":
    env = os.environ.get("ENV", "dev")

    if env == "prod":
        landing_dir = os.path.join(project_root, "datalake", "raw", "streaming_landing")
    else:
        landing_dir = os.path.join(project_root, "datalake_sample", "raw", "streaming_landing")

    os.makedirs(landing_dir, exist_ok=True)
    start_producer(landing_dir, interval_sec=3)
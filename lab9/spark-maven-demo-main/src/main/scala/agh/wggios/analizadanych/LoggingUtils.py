import logging


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )

    logging.getLogger("py4j").setLevel(logging.WARN)
    logging.getLogger("pyspark").setLevel(logging.INFO)
    logging.getLogger("org.apache.hadoop").setLevel(logging.ERROR)
    logging.getLogger("agh.wggios.analizadanych").setLevel(logging.DEBUG)
    logging.getLogger("com.databricks").setLevel(logging.ERROR)
    logging.getLogger("akka").setLevel(logging.WARN)

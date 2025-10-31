import yaml
import logging
from datetime import datetime
import pandas as pd
from src.ingestion import DataIngestion
from src.transformation import DataTransformation
from src.quality_checks import DataQuality
import os


class EcommerceDataPipeline:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.setup_logging()

    def load_config(self, config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def setup_logging(self):
        os.makedirs('logs', exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/pipeline_execution.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def run_pipeline(self):
        self.logger.info("Iniciando pipeline de e-commerce...")

        try:

            self.logger.info("Descargando y cargando datos...")
            ingestion = DataIngestion(self.config['ingestion'])
            products_df, sales_df, inventory_df = ingestion.run()

            self.logger.info("Ejecutando transformaciones...")
            transformer = DataTransformation(self.config['transformation'])
            metrics = transformer.run(products_df, sales_df, inventory_df)

            self.logger.info("Ejecutando tests de calidad...")
            quality = DataQuality()
            quality_results = quality.run(products_df, sales_df, inventory_df)

            self.logger.info("Generando reporte...")
            self.generate_report(metrics, quality_results)

            self.logger.info("Pipeline completado")

        except Exception as e:
            self.logger.error(f"Error en el pipeline: {str(e)}")

    def generate_report(self, metrics, quality_results):
        os.makedirs('data/reports', exist_ok=True)
        report = {
            'timestamp': datetime.now().isoformat(),
            'metrics': metrics,
            'quality_results': quality_results
        }
        pd.Series(report).to_json(f"data/reports/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")


if __name__ == "__main__":
    pipeline = EcommerceDataPipeline('config/pipeline_config.yaml')
    pipeline.run_pipeline()

import pytest
import os
from src.orchestrator import EcommerceDataPipeline
from src.ingestion import DataIngestion
from src.transformation import DataTransformation
from src.quality_checks import DataQuality
import pandas as pd
import yaml


@pytest.fixture
def sample_config(tmp_path):
    """Priemero creamos una configuracion temporal para pruebas"""
    config_content = {
        "ingestion": {
            "api_url": "https://fakestoreapi.com/products",
            "sales_csv": "data/raw/sales.csv",
            "inventory_csv": "data/raw/inventory.csv"
        },
        "transformation": {"critical_stock_threshold": 10}
    }

    config_path = tmp_path / "pipeline_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config_content, f)
    return str(config_path)


def test_pipeline_initialization(sample_config):
    """Verificamos que el pipeline se inicializa bien"""
    pipeline = EcommerceDataPipeline(sample_config)
    assert pipeline.config is not None
    assert "ingestion" in pipeline.config


def test_ingestion_creates_parquets(sample_config):
    """Verificamos que la etapa de ingesta descargue y guarde archivos"""
    ingestion = DataIngestion({
        "api_url": "https://fakestoreapi.com/products",
        "sales_csv": "data/raw/sales.csv",
        "inventory_csv": "data/raw/inventory.csv"
    })

    os.makedirs("data/raw", exist_ok=True)
    pd.DataFrame({"id": [1], "quantity": [5], "date": ["2025-10-31"]}).to_csv("data/raw/sales.csv", index=False)
    pd.DataFrame({"id": [1], "stock": [10], "min_stock": [5], "cost": [20]}).to_csv("data/raw/inventory.csv", index=False)

    products_df, sales_df, inventory_df = ingestion.run()
    assert not products_df.empty
    assert "price" in products_df.columns
    assert os.path.exists("data/raw/products.parquet")


def test_transformation_metrics(sample_config):
    """Verificamos que se generen metricas de negocio correctamente"""
    products_df = pd.DataFrame({
        "id": [1, 2],
        "title": ["Prod A", "Prod B"],
        "price": [10, 20],
        "category": ["cat1", "cat2"]
    })
    sales_df = pd.DataFrame({
        "id": [1, 2],
        "quantity": [5, 10],
        "date": ["2025-10-31", "2025-10-30"]
    })
    inventory_df = pd.DataFrame({
        "id": [1, 2],
        "stock": [8, 2],
        "min_stock": [5, 3],
        "cost": [6, 12]
    })

    transformer = DataTransformation({"critical_stock_threshold": 5})
    metrics = transformer.run(products_df, sales_df, inventory_df)

    assert "critical_products" in metrics
    assert "sales_by_category" in metrics
    assert "top_products" in metrics
    assert "profit_by_product" in metrics


def test_quality_checks(sample_config):
    """Validamos las pruebas de calidad que aparecen en el parcial"""
    products_df = pd.DataFrame({
        "id": [1],
        "price": [20],
        "category": ["electronics"]
    })
    sales_df = pd.DataFrame({
        "id": [1],
        "quantity": [5],
        "date": ["2025-10-31"]
    })
    inventory_df = pd.DataFrame({
        "id": [1],
        "stock": [10],
        "min_stock": [5]
    })

    quality = DataQuality()
    results = quality.run(products_df, sales_df, inventory_df)

    assert results["negative_prices"]
    assert results["valid_stock"]
    assert results["valid_categories"]
    assert results["valid_dates"]

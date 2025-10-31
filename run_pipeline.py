from src.orchestrator import EcommerceDataPipeline

if __name__ == "__main__":
    print("=== Ejecutando Pipeline de E-commerce ===")
    pipeline = EcommerceDataPipeline('config/pipeline_config.yaml')
    pipeline.run_pipeline()
    print("=== Ejecucion completada ===")

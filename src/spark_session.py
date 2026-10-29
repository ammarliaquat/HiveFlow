# src/spark_session.py
import os
from pyspark.sql import SparkSession

class SparkSessionFactory:

    @staticmethod
    def get_session(config: dict) -> SparkSession:
        spark_cfg = config.get("spark", {})
        app_env = os.getenv("APP_ENV", "production").lower()

        if app_env == "local":
            # Must inject driver classpath BEFORE the JVM boots!
            submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
            if "ojdbc" not in submit_args:
                os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path C:/opt/ETL/jars/ojdbc8.jar --jars file:///C:/opt/ETL/jars/ojdbc8.jar {submit_args} pyspark-shell"

        builder = SparkSession.builder.appName(spark_cfg.get("app_name", "GenericETLPipeline"))

        is_remote = False
        if app_env == "local":
            if os.name == 'nt':
                # Force local IP to bypass invalid Windows hostnames containing underscores
                if "SPARK_LOCAL_IP" not in os.environ:
                    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
                # Fix python warnings / command resolve issues
                if "PYSPARK_PYTHON" not in os.environ:
                    os.environ["PYSPARK_PYTHON"] = "python"
                if "PYSPARK_DRIVER_PYTHON" not in os.environ:
                    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
                if "HADOOP_HOME" not in os.environ:
                    os.environ["HADOOP_HOME"] = r"C:\hadoop"
                
                # Check and fix JAVA_HOME if it points to non-existent path
                java_home = os.environ.get("JAVA_HOME", "")
                if not java_home or not os.path.exists(java_home):
                    fallback_java = r"C:\Program Files\Java\jdk-17"
                    if os.path.exists(fallback_java):
                        os.environ["JAVA_HOME"] = fallback_java

            connect_url = os.getenv("SPARK_CONNECT_URL")
            if connect_url:
                print(f"[SparkSession] Connecting via Spark Connect to: {connect_url}")
                builder = builder.remote(connect_url)
                is_remote = True
            else:
                builder = (
                    builder.master("local[*]")
                    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,mysql:mysql-connector-java:8.0.30")
                )
        else:
            builder = builder.master(spark_cfg.get("master", "local[*]"))
            builder = (
                builder
                .config("spark.executor.memory", spark_cfg.get("executor_memory", "4g"))
                .config("spark.driver.memory", spark_cfg.get("driver_memory", "2g"))
                .config("spark.executor.cores", str(spark_cfg.get("executor_cores", 2)))
            )
            extra_configs = spark_cfg.get("extra_configs", {})
            for k, v in extra_configs.items():
                builder = builder.config(k, str(v))

        # Apply extra_configs for all session types (excluding driver classpath configs under Spark Connect)
        if app_env == "local":
            extra_configs = spark_cfg.get("extra_configs", {})
            for k, v in extra_configs.items():
                if is_remote and "extraClassPath" in k:
                    continue
                builder = builder.config(k, str(v))

        # Spark Event Logging support
        event_log_dir = spark_cfg.get("event_log_dir") or os.getenv("SPARK_EVENT_LOG_DIR")
        if event_log_dir:
            os.makedirs(event_log_dir, exist_ok=True)
            builder = builder.config("spark.eventLog.enabled", "true")
            abs_event_log_dir = os.path.abspath(event_log_dir).replace('\\', '/')
            builder = builder.config("spark.eventLog.dir", f"file:///{abs_event_log_dir}")

        spark = builder.getOrCreate()
        
        if not is_remote:
            try:
                spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))
            except Exception:
                pass
                
        return spark

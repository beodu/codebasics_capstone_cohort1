# Databricks notebook source
# MAGIC %md
# MAGIC # Run dbt build (Gold)
# MAGIC Called by the ADF master pipeline as stage 3. `dbt build` runs models and
# MAGIC tests together, so a failing test fails the pipeline. That is the point:
# MAGIC a dashboard that is silently wrong is worse than one that is visibly down.

# COMMAND ----------
# MAGIC %pip install dbt-databricks==1.8.* dbt-core==1.8.*
# MAGIC %restart_python

# COMMAND ----------
import os
import subprocess

# Credentials come from a Databricks secret scope, never from notebook literals.
os.environ["DATABRICKS_HOST"] = dbutils.secrets.get("atliq", "databricks_host")
os.environ["DATABRICKS_HTTP_PATH"] = dbutils.secrets.get("atliq", "databricks_http_path")
os.environ["DATABRICKS_TOKEN"] = dbutils.secrets.get("atliq", "databricks_token")

PROJECT_DIR = "/Workspace/Repos/atliq/phase1/dbt/atliq_gold"


def run(cmd):
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True)
    print(result.stdout[-8000:])
    if result.returncode != 0:
        print(result.stderr[-4000:])
        raise Exception(f"dbt failed: {' '.join(cmd)}")
    return result.stdout


# COMMAND ----------
run(["dbt", "deps"])
run(["dbt", "build", "--target", "dev"])

# Source freshness is checked after the build, so a stale-source warning is
# visible in the log without blocking a build that is otherwise correct.
subprocess.run(["dbt", "source", "freshness"], cwd=PROJECT_DIR)

# COMMAND ----------
dbutils.notebook.exit("dbt build SUCCESS")

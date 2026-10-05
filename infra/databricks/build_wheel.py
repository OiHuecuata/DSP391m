"""Build one portable wheel from repository jobs and authoritative contracts."""

import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def build_wheel(output: Path | None = None):
    output = (output or ROOT / ".local/databricks/dist").resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dsp-databricks-wheel-") as directory:
        stage = Path(directory)
        source = ROOT / "pipelines/spark/src/dsp_spark"
        for path in source.rglob("*.py"):
            if "__pycache__" not in path.parts:
                target = stage / "dsp_spark" / path.relative_to(source)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target)
        platform = stage / "dsp_databricks"
        platform.mkdir()
        shutil.copyfile(
            Path(__file__).with_name("run_social_activity_daily.py"),
            platform / "run_social_activity_daily.py",
        )
        collector = stage / "dsp_collector/runtime"
        collector.mkdir(parents=True)
        shutil.copyfile(
            ROOT / "apps/collector/src/dsp_collector/runtime/config.py",
            collector / "config.py",
        )
        contracts = stage / "dsp_contracts"
        for path in (ROOT / "contracts").rglob("*.schema.json"):
            target = contracts / path.relative_to(ROOT / "contracts")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
        shutil.copyfile(
            ROOT / "configs/reference/vietnam_locations.v1.json",
            contracts / "reference/vietnam_locations.v1.json",
        )
        for package in (
            stage / "dsp_spark",
            platform,
            stage / "dsp_collector",
            contracts,
        ):
            (package / "__init__.py").write_text(
                '"""Packaged repository resources."""\n'
            )
        (stage / "pyproject.toml").write_text("""[project]
name = "dsp391-spark-jobs"
version = "0.2.0"
description = "Portable DSP391 Spark jobs with canonical contract resources"
requires-python = ">=3.12"
dependencies = ["jsonschema (>=4.25.0,<5.0.0)"]

[build-system]
requires = ["poetry-core>=2.0.0,<3.0.0"]
build-backend = "poetry.core.masonry.api"

[project.scripts]
social_activity_daily = "dsp_databricks.run_social_activity_daily:main"
court_supply_by_location = "dsp_databricks.run_social_activity_daily:court_supply_main"

[tool.poetry]
packages = [{include = "dsp_spark"}, {include = "dsp_databricks"}, {include = "dsp_collector"}, {include = "dsp_contracts"}]
include = [{path = "dsp_contracts/**/*.json", format = "wheel"}]
""")
        # Poetry supplies its own build backend. Spark and Delta are deliberately
        # omitted from wheel dependencies: managed compute supplies that runtime.
        subprocess.run(
            [
                "poetry",
                "build",
                "--format",
                "wheel",
                "--directory",
                str(stage),
                "--output",
                str(output),
            ],
            check=True,
        )
    return output / "dsp391_spark_jobs-0.2.0-py3-none-any.whl"


if __name__ == "__main__":
    print(build_wheel())

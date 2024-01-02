# Databricks notebook source

from brickflow import Project, PypiTaskLibrary, MavenTaskLibrary  # make sure brickflow imports are at the top

import workflows

def main() -> None:
    """Project entrypoint"""
    with Project(
        "bf_demo_dataaisummit",
        git_repo="https://github.com/asingamaneni/DataAISummit2024",
        provider="github",
        libraries=[
            MavenTaskLibrary(coordinates="com.cronutils:cron-utils:9.2.0"),
            # PypiTaskLibrary(package="spark-expectations==1.0.1"), # Uncomment if spark-expectations is needed
        ],
    ) as f:
        f.add_pkg(workflows)


if __name__ == "__main__":
    main()

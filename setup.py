from setuptools import find_packages, setup

setup(
    name="digital-trade-foundation",
    version="0.1.0",
    description="技能赛训协作基础服务",
    package_dir={"": "src"},
    packages=find_packages("src"),
    python_requires=">=3.11",
)

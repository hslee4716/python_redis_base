from setuptools import setup, find_packages

setup(
    name="redis_load_balancer",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "redis",
        "opencv-python",
        "numpy",
    ],
)
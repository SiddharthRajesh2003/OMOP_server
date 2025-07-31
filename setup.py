from setuptools import setup, find_packages
import os

# Read README
def read_readme():
    if os.path.exists('README.md'):
        with open('README.md', 'r', encoding='utf-8') as f:
            return f.read()
    return ""

setup(
    name="OMOP-server",  # Note: using uppercase here too
    version="1.0.0",
    author="Siddharth Rajesh",
    author_email="your.email@example.com",
    description="ETL framework for transforming healthcare data into OMOP Common Data Model",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/SiddharthRajesh2003/OMOP_server",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Healthcare Industry",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        'certifi==2025.7.14',
        'charset-normalizer==3.4.2',
        'colorama==0.4.6',
        'filelock==3.18.0',
        'fsspec==2025.7.0',
        'fuzzywuzzy==0.18.0',
        'greenlet==3.2.3',
        'huggingface-hub==0.34.3',
        'idna==3.10',
        'Jinja2==3.1.4',
        'joblib==1.5.1',
        'logging==0.4.9.6',
        'MarkupSafe==2.1.5',
        'mpmath==1.3.0',
        'networkx==3.3',
        'numpy==2.3.2',
        'packaging==25.0',
        'pandas==2.3.1',
        'pillow==11.0.0',
        'pyodbc==5.2.0',
        'python-dateutil==2.9.0.post0',
        'python-dotenv==1.1.1',
        'pytz==2025.2',
        'PyYAML==6.0.2',
        'regex==2025.7.34',
        'requests==2.32.4',
        'safetensors==0.5.3',
        'scikit-learn==1.7.1',
        'scipy==1.16.1',
        'six==1.17.0',
        'SQLAlchemy==2.0.42',
        'sympy==1.13.3',
        'threadpoolctl==3.6.0',
        'tokenizers==0.21.4',
        'torch==2.7.1',
        'torchaudio==2.7.1',
        'torchvision==0.22.1',
        'tqdm==4.67.1',
        'transformers==4.54.1',
        'typing_extensions==4.14.1',
        'tzdata==2025.2',
        'urllib3==2.5.0',
    ],
    extras_require={
        'dev': [
            'pytest',
            'pytest-cov',
            'black',
            'flake8',
            'mypy',
        ]
    },
    include_package_data=True,
    package_data={
        'OMOP_server': ['config.json'],
    },
    zip_safe=False,
)
from setuptools import setup

setup(
    name="aws",
    version="0.1",
    packages=["s3"],
    install_requires=[
        "boto3==1.34.109",
        "pandas==2.2.2",
        "ipykernel==6.29.4",
        "python-dotenv==1.0.1",
        "pyarrow==16.1.0",
        "tqdm==4.66.4",
    ],
    author="Luis Henrique",
)

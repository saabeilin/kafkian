from setuptools import find_packages, setup


def readme():
    with open("README.md") as f:
        return f.read()


setup(
    name="kafkian",
    version="0.15.0",
    description="Opinionated Kafka Python client on top of Confluent python library",
    long_description=readme(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    keywords="kafka",
    url="https://github.com/saabeilin/kafkian",
    author="Sergei Beilin",
    author_email="saabeilin@gmail.com",
    license="Apache 2.0",
    packages=find_packages(),
    install_requires=[
        "confluent-kafka>=1.0.0",
        "fastavro>=0.18.0",
        "avro-python3>=1.8.2",
    ],
    zip_safe=False,
)

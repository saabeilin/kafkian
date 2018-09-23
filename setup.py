from setuptools import find_packages, setup

setup(name="kafkian",
      version="0.7.0",
      description="Opinionated Kafka Python client on top of Confluent python library",
      url="https://github.com/saabeilin/kafkian",
      author="Sergei Beilin",
      author_email="saabeilin@gmail.com",
      license="Apache 2.0",
      packages=find_packages(),
      install_requires=[
          'structlog>=17.2.0',
          'confluent-kafka>=0.11.5',
          'fastavro>=0.18.0',
          'avro-python3>=1.8.2'
      ],
      zip_safe=False)

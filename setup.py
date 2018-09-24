from setuptools import find_packages, setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(name="kafkian",
      version="0.7.2",
      description="Opinionated Kafka Python client on top of Confluent python library",
      long_description=readme(),
      long_description_content_type="text/markdown",
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
      ],
      keywords='kafka',
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

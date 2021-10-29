from setuptools import setup

setup(
    name="shapleqclient",
    version="0.0.3",
    description="Python shapleqclient for ShapleQ",
    url="https://github.com/paust-team/shapleq-python.git",
    author="Elon Choi",
    license="MIT",
    packages=['shapleqclient', 'shapleqclient.common', 'shapleqclient.message', 'shapleqclient.proto'],
    zip_safe=False,
    install_requires=[
        "protobuf==3.12.4",
        "kazoo==2.8.0"
    ]
)
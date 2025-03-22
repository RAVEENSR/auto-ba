from setuptools import setup, find_packages

setup(
    name="auto-ba",
    version="0.1.0",
    description="A tool for automated bug assignment",
    author="Raveen Savinda Rathnayake",
    url="https://github.com/RAVEENSR/auto-ba",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.2.0",  # For data manipulation
        "pyspark>=3.1.0",  # For Spark session and DataFrame operations
        "nltk>=3.6.0",  # For natural language processing
        "scikit-learn>=0.24.0",  # For TF-IDF vectorization and normalization
        "Flask>=1.1.2",  # For building the web application
        "flask-cors>=3.0.10",  # For enabling cross-origin resource sharing in Flask
        "gevent>=21.1.2"  # For the WSGI server via gevent.pywsgi
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

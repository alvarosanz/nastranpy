from setuptools import setup, find_packages


setup(
    name='nastranpy',
    version='0.1.2',
    description='A library to interact with nastran models',
    url='https://github.com/alvarosanz/nastranpy',
    author='Alvaro Sanz Oriz',
    author_email='alvaro.sanz.oriz@gmail.com',
    packages=find_packages(),
    license='MIT',
    keywords='NASTRAN FEM engineering',
    long_description=open('README.rst').read(),
    install_requires=['numpy'],
    python_requires='>=3.3',
)

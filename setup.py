from setuptools import setup


setup(
    name='nastranpy',
    version='0.1.1',
    description='A library to interact with nastran models',
    url='https://github.com/alvarosanz/nastranpy',
    author='Álvaro Sanz Oriz',
    author_email='alvaro.sanz.oriz@gmail.com',
    packages=['nastranpy',],
    license='MIT',
    keywords='NASTRAN FEM engineering',
    long_description=open('README.rst').read(),
    install_requires=['numpy'],
    python_requires='>=3.3',
)

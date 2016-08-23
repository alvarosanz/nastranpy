from distutils.core import setup


setup(
    name='nastranpy',
    version='0.1',
    description='A library to interact with nastran models',
    url='https://bitbucket.org/alvarosanz/nastranpy',
    author='Alvaro Sanz Oriz',
    author_email='alvaro.sanz@aernnova.com',
    packages=['nastranpy',],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.md').read(),
    install_requires=['numpy'],
)

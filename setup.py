from setuptools import setup

setup(
    name='nxtGenJS',
    version='1.0',
    packages=['helper', 'log_script', 'mongo_connection'],
    url='',
    license='',
    author='Hardeep Singh',
    author_email='hardeepsinghbamrah@gmail.com',
    description='This project will combine tow mongodb collections ',
    data_files=[('config', ['cfg/config.cfg'])]
)

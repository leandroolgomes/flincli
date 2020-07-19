from setuptools import setup, find_packages

setup(
    name='flincli',
    version='1.0.0',
    py_modules=['cli'],
    package_dir={'': 'src'},
    packages=find_packages('src'),
    include_package_data=True,
    install_requires=[
        'click',
        'boto3',
        'tabulate',
        'PyYAML==5.3.1' ,
        'requests==2.23.0'
    ],
    entry_points='''
        [console_scripts]
        flincli=cli:cli
    '''
)

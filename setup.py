from distutils.core import setup

from spooky_rpc import VERSION

with open('README.rst', 'r') as readme_file:
    readme_content = readme_file.read()

setup(
    name='spooky-rpc',
    version=VERSION,
    description='A client/server framework for supporting RPC over a filesystem transport.',
    long_description=readme_content,
    author='Paul Pelzl',
    author_email='pelzlpj@gmail.com',
    url='http://github.com/pelzlpj/spooky-rpc',
    license='http://www.opensource.org/licenses/bsd-license.php',
    py_modules=['spooky_rpc'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: System :: Networking'],
    platforms=['any']
)



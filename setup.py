'''
Created on 30.08.2011

@author: joe
'''
import setuptools
setuptools.setup(
    name = "CloudFusion",
    packages = setuptools.find_packages(),
    include_package_data = True,
    install_requires = ['mechanize', 'requests==1.2.3', 'nose', 'oauth', 'poster', 'simplejson', 'httplib2', 'beautifulsoup4'],
#  requests bug with requests 2.0.1:
#    File "/usr/local/lib/python2.7/dist-packages/requests/cookies.py", line 311, in _find_no_duplicates
#    raise KeyError('name=%r, domain=%r, path=%r' % (name, domain, path))
#    KeyError: "name=Cookie(version=0, name='bang', value='QUFCQmh5c3FET1RnMUZkcXlrMXNBNXV4eFhaU080NWtzYndmUDlDa0p1SEFHZw%3D%3D', port=None, port_specified=False, domain='.dropbox.com', domain_specified=True, domain_initial_dot=False, path='/', path_specified=True, secure=False, expires=1383395405, discard=False, comment=None, comment_url=None, rest={'httponly': None}, rfc2109=False), domain=None, path=None"
    version = "3.7.6",
    description = "Filesystem interface to cloud storage services",
    author = "Johannes Mueller",
    author_email = "johannes.mueller1@mail.inf.tu-dresden.de",
    url = "https://github.com/joe42/CloudFusion",
    download_url = "https://github.com/joe42/CloudFusion/archive/master.zip",
    entry_points={
        'console_scripts': [
            'cloudfusion = cloudfusion.main:main',
        ],
    },
    keywords = ["encoding", "i18n", "xml"],
    classifiers = [
        "Programming Language :: Python :: 2.6",
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "License :: Other/Proprietary License",
        "Operating System :: Linux",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: System :: Filesystems",
        ],
    long_description = """\
CloudFusion lets you access your Dropbox or Sugarsync files from Linux like any file on your desktop.
"""
)

import setuptools


setuptools.setup(
	name = 'qwarc',
	description = 'A framework for quick web archival',
	author = 'JustAnotherArchivist',
	url = 'https://github.com/JustAnotherArchivist/qwarc',
	classifiers = [
		'Development Status :: 3 - Alpha',
		'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
		'Programming Language :: Python :: 3.6',
		'Programming Language :: Python :: 3.7',
	],
	packages = ['qwarc'],
	setup_requires = ['setuptools_scm'],
	use_scm_version = True,
	install_requires = ['aiohttp==3.7.4', 'warcio', 'yarl'],
	entry_points = {
		'console_scripts': [
			'qwarc = qwarc.cli:main',
		],
	},
)

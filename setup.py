from setuptools import setup, find_packages

setup(name='cypy',
      version='2021.09.27.a',
      description="wizyoung's personal python utilities",
      classifiers=[
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
      ],
      url='https://github.com/wizyoung/cypy',
      author='wizyoung',
      author_email='happyyanghehe@gmail.com',
      license='MIT',
      packages=["cypy"],
      package_dir={"": "./"},
      install_requires=[
        'lmdb',
        'omegaconf',
        'easydict',
        'scipy',
        'scikit-learn'
      ],
      extras_require={
        'full': ['torch', 'torchvision'],
      },
    )
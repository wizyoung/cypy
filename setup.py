from setuptools import setup, find_packages

setup(name='cypy',
      version='2021.12.22.a',
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
      packages=["cypy", "cypy.taiji"],
      package_dir={"": "./"},
      install_requires=[
        'lmdb',
        'omegaconf',
        'easydict',
        'scipy',
        'scikit-learn',
        'requests',
        'psutil',
        'decord',
        'tqdm',
      ],
      extras_require={
        'full': ['torch', 'torchvision'],
      },
    )